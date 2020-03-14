# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import json
import logging
import os
import re
from datetime import datetime
from datetime import timedelta

from googleapiclient import discovery
from googleapiclient.http import build_http
from googleapiclient.errors import HttpError
from oauth2client import client, file, tools

GMB_DISCOVERY_FILE = "gmb_discovery.json"
CLIENT_SECRETS_FILE = "client_secrets.json"
CREDENTIALS_STORAGE = "credentials.dat"
SCHEMAS_FILE = "schemas.json"
SENTIMENTS_LASTRUN_FILE = "sentiments_lastrun"
SCOPES = [
    "https://www.googleapis.com/auth/business.manage",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-language"
]
DATASET_ID = "alligator"
MAX_RETRIES = 10
MIN_TOKENS = 20
INSIGHTS_DAYS_BACK = 540
CALLS_DAYS_BACK = 7
DIRECTIONS_NUM_DAYS = "SEVEN"
BQ_JOBS_QUERY_MAXRESULTS_PER_PAGE = 1000
BQ_TABLEDATA_INSERTALL_BATCHSIZE = 5000

logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.CRITICAL)


class API(object):

  def __init__(self, project_id):
    client_secrets = os.path.join(
        os.path.dirname(__file__), CLIENT_SECRETS_FILE)

    flow = client.flow_from_clientsecrets(
        client_secrets,
        SCOPES,
        message=tools.message_if_missing(client_secrets))

    storage = file.Storage(CREDENTIALS_STORAGE)
    credentials = storage.get()

    if credentials is None or credentials.invalid:
      flags = argparse.Namespace(
          noauth_local_webserver=True,
          logging_level=logging.getLevelName(
              logging.getLogger().getEffectiveLevel()))
      credentials = tools.run_flow(flow, storage, flags=flags)

    http = credentials.authorize(http=build_http())

    with open(GMB_DISCOVERY_FILE) as gmb_discovery_file:
      self.gmb_service = discovery.build_from_document(
          gmb_discovery_file.read(),
          base="https://www.googleapis.com/",
          http=http)

    self.PROJECT_ID = project_id
    self.dataset_exists = False
    self.existing_tables = {}

    with open(SCHEMAS_FILE) as schemas_file:
      self.schemas = json.load(schemas_file)

    self.bq_service = discovery.build("bigquery", "v2", http=http)
    self.nlp_service = discovery.build("language", "v1", http=http)

  def accounts(self):
    data = []
    page_token = None

    while True:
      response_json = self.gmb_service.accounts().list(
          pageToken=page_token).execute(num_retries=MAX_RETRIES)

      data = data + (response_json.get("accounts") or [])

      page_token = response_json.get("nextPageToken")
      if not page_token:
        break

    logging.info(json.dumps(data, indent=2))

    self.to_bigquery(table_name="accounts", data=data)

    return data

  def locations(self, account_id, location_id=None):
    data = []
    page_token = None

    if not location_id:
      while True:
        response_json = self.gmb_service.accounts().locations().list(
            parent=account_id,
            pageToken=page_token).execute(num_retries=MAX_RETRIES)

        data = data + (response_json.get("locations") or [])

        page_token = response_json.get("nextPageToken")
        if not page_token:
          break

    else:
      response_json = self.gmb_service.accounts().locations().get(
          name=location_id).execute(num_retries=MAX_RETRIES)
      data = data + (response_json.get("locations") or [])

    logging.info(json.dumps(data, indent=2))
    self.to_bigquery(table_name="locations", data=data)

    return data

  def reviews(self, location_id):
    page_token = None

    while True:
      response_json = self.gmb_service.accounts().locations().reviews().list(
          parent=location_id,
          pageToken=page_token).execute(num_retries=MAX_RETRIES)

      data = response_json.get("reviews") or []
      logging.info(json.dumps(data, indent=2))
      self.to_bigquery(table_name="reviews", data=data)

      page_token = response_json.get("nextPageToken")
      if not page_token:
        break

  def sentiments(self):
    page_token = None
    lastrun = self.get_sentiments_lastrun()

    self.ensure_dataset_exists()
    self.ensure_table_exists(table_name="reviews")

    query = {
      "query": """
        SELECT 
          comment,
          name,
          reviewId
        FROM 
          [{projectId}:{datasetId}.reviews]
        WHERE 
          comment IS NOT NULL
          AND (
            DATE(_PARTITIONTIME) > "{lastrun}"
            OR
              _PARTITIONTIME IS NULL)
      """.format(
          projectId=self.PROJECT_ID,
          datasetId=DATASET_ID,
          lastrun=lastrun),
      "maxResults": BQ_JOBS_QUERY_MAXRESULTS_PER_PAGE
    }

    response_json = self.bq_service.jobs().query(
        projectId=self.PROJECT_ID,
        body=query).execute(num_retries=MAX_RETRIES)

    rows = response_json.get("rows") or []
    self.process_sentiments(rows)
    
    page_token = response_json.get("pageToken")
    if page_token:
      job_id = response_json.get("jobReference").get("jobId")

      while True:
        response_json_job = self.bq_service.jobs().getQueryResults(
            projectId=self.PROJECT_ID,
            jobId=job_id,
            maxResults=BQ_JOBS_QUERY_MAXRESULTS_PER_PAGE,
            pageToken=page_token).execute(num_retries=MAX_RETRIES)

        rows_job = response_json_job.get("rows") or []
        self.process_sentiments(rows_job)

        page_token = response_json_job.get("pageToken")
        if not page_token:
          break

    self.set_sentiments_lastrun()

  def get_sentiments_lastrun(self):
    lastrun_file_path = os.path.join(
        os.path.dirname(__file__), SENTIMENTS_LASTRUN_FILE)
    lastrun = datetime(year=1970, month=1, day=1).date()

    try:
      lastrun = datetime.fromtimestamp(os.path.getmtime(lastrun_file_path)).date()
    except OSError:
      logging.info("No previous run for sentiment analysis found. " + 
          "Performing sentiment analysis on all available reviews.")
  
    return lastrun        

  def process_sentiments(self, rows):
    sentiments = []

    for row in rows:
      sentiment = {}
      comment = row.get("f")[0].get("v")
      sentiment["comment"] = comment
      sentiment["name"] = row.get("f")[1].get("v")
      sentiment["reviewId"] = row.get("f")[2].get("v")
      sentiment["annotation"] = self.annotate_text(comment)

      sentiments.append(sentiment)

    logging.info(json.dumps(sentiments, indent=2))
    self.to_bigquery(table_name="sentiments", data=sentiments)

  def set_sentiments_lastrun(self):
    lastrun_file_path = os.path.join(
        os.path.dirname(__file__), SENTIMENTS_LASTRUN_FILE)
    current_time = datetime.now().timestamp()

    if os.path.isfile(lastrun_file_path):
      os.utime(lastrun_file_path, (current_time, current_time))
    else:
      os.open(lastrun_file_path, os.O_CREAT)

  def annotate_text(self, content):
    if not content:
      return

    body = {
        "document": {
            "type": "PLAIN_TEXT",
            "content": content
        },
        "features": {
            "extractSyntax": True,
            "extractEntities": True,
            "extractDocumentSentiment": True,
            "extractEntitySentiment": True,
            "classifyText": len(content.split()) > MIN_TOKENS
        },
        "encodingType": "UTF8"
    }

    try:
      return self.nlp_service.documents().annotateText(body=body).execute(
          num_retries=MAX_RETRIES)
    except HttpError as err:
      if err.resp.status != 400:
        raise

  def insights(self, location_id):
    end_time = (datetime.now() - timedelta(days=5)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=INSIGHTS_DAYS_BACK)

    query = {
        "locationNames": [location_id],
        "basicRequest": {
            "metricRequests": {
                "metric": "ALL",
                "options": ["AGGREGATED_DAILY"]
            },
            "timeRange": {
                "startTime": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "endTime": end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            }
        },
    }

    data = []
    account_id = re.search("(accounts/[0-9]+)/locations/[0-9]+", location_id,
                           re.IGNORECASE).group(1)

    response_json = self.gmb_service.accounts().locations().reportInsights(
        name=account_id, body=query).execute(num_retries=MAX_RETRIES)

    if "locationMetrics" in response_json:
      for line in response_json.get("locationMetrics"):
        line["name"] = line.get("locationName")
        data.append(line)

      logging.info(json.dumps(data, indent=2))
      self.to_bigquery(table_name="insights", data=data)

    else:
      logging.warn("No insights reported for %s", location_id)

    return data

  def directions(self, location_id):
    query = {
        "locationNames": [location_id],
        "drivingDirectionsRequest": {
            "numDays": DIRECTIONS_NUM_DAYS,
            "language_code": "es_ES"
        }
    }

    data = []
    account_id = re.search("(accounts/[0-9]+)/locations/[0-9]+", location_id,
                           re.IGNORECASE).group(1)

    response_json = self.gmb_service.accounts().locations().reportInsights(
        name=account_id, body=query).execute(num_retries=MAX_RETRIES)

    if "locationDrivingDirectionMetrics" in response_json:
      for line in response_json.get("locationDrivingDirectionMetrics"):
        line["name"] = line.get("locationName")
        data.append(line)

      logging.info(json.dumps(data, indent=2))
      self.to_bigquery(table_name="directions", data=data)

    return data

  def hourly_calls(self, location_id):
    query = {
        "locationNames": [location_id],
        "basicRequest": {
            "metricRequests": [{
                "metric": "ACTIONS_PHONE",
                "options": ["BREAKDOWN_HOUR_OF_DAY"]
            }],
            "timeRange": {}
        },
    }

    account_id = re.search("(accounts/[0-9]+)/locations/[0-9]+", location_id,
                           re.IGNORECASE).group(1)

    limit_end_time = (datetime.now() - timedelta(days=5)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    start_time = limit_end_time - timedelta(days=CALLS_DAYS_BACK)

    data = []

    while start_time < limit_end_time:
      end_time = start_time + timedelta(days=1)

      start_time_string = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
      end_time_string = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

      query["basicRequest"]["timeRange"] = {
          "startTime": start_time_string,
          "endTime": end_time_string
      }

      response_json = self.gmb_service.accounts().locations().reportInsights(
          name=account_id, body=query).execute(num_retries=MAX_RETRIES)

      if "locationMetrics" in response_json:
        for line in response_json.get("locationMetrics"):
          line["name"] = "{}/{}".format(
              line.get("locationName"), start_time_string)
          if "metricValues" in line:
            for metric_values in line.get("metricValues"):
              if "dimensionalValues" in metric_values:
                for values in metric_values.get("dimensionalValues"):
                  values["timeDimension"]["timeRange"] = {
                      "startTime": start_time_string
                  }

          data.append(line)

      start_time = start_time + timedelta(days=1)

    if data:
      logging.info(json.dumps(data, indent=2))
      self.to_bigquery(table_name="hourly_calls", data=data)

    return data

  def ensure_dataset_exists(self):
    if self.dataset_exists:
      return

    try:
      self.bq_service.datasets().get(
          projectId=self.PROJECT_ID,
          datasetId=DATASET_ID).execute(num_retries=MAX_RETRIES)

      logging.info(u"Dataset {}:{} already exists.".format(
          self.PROJECT_ID, DATASET_ID))

      self.dataset_exists = True

      return
    except HttpError as err:
      if err.resp.status != 404:
        raise

    dataset = {
        "datasetReference": {
            "projectId": self.PROJECT_ID,
            "datasetId": DATASET_ID
        }
    }

    self.bq_service.datasets().insert(
        projectId=self.PROJECT_ID,
        body=dataset).execute(num_retries=MAX_RETRIES)

    self.dataset_exists = True

  def ensure_table_exists(self, table_name):
    if self.existing_tables.get(table_name):
      return

    try:
      self.bq_service.tables().get(
          projectId=self.PROJECT_ID, datasetId=DATASET_ID,
          tableId=table_name).execute(num_retries=MAX_RETRIES)

      logging.info(u"Table {}:{}.{} already exists.".format(
          self.PROJECT_ID, DATASET_ID, table_name))

      self.existing_tables[table_name] = True

      return
    except HttpError as err:
      if err.resp.status != 404:
        raise

    table = {
        "schema": {
            "fields": self.schemas.get(table_name)
        },
        "tableReference": {
            "projectId": self.PROJECT_ID,
            "datasetId": DATASET_ID,
            "tableId": table_name
        },
        "timePartitioning": {
            "type": 'DAY'
        }
    }

    self.bq_service.tables().insert(
        projectId=self.PROJECT_ID, datasetId=DATASET_ID,
        body=table).execute(num_retries=MAX_RETRIES)

    self.existing_tables[table_name] = True

  def to_bigquery(self, table_name, data=[]):
    if not data:
      return

    self.ensure_dataset_exists()
    self.ensure_table_exists(table_name)

    rows = [{"json": line, "insertId": line.get("name")} for line in data]

    chunk_size = BQ_TABLEDATA_INSERTALL_BATCHSIZE
    chunked_rows = [
      rows[i * chunk_size:(i + 1) * chunk_size]
      for i in range((len(rows) + chunk_size - 1) // chunk_size)
    ]

    for chunk in chunked_rows:
      logging.info(u"Inserting {} rows into table {}:{}.{}.".format(
        len(chunk), self.PROJECT_ID, DATASET_ID, table_name))

      data_chunk = {"rows": chunk, "ignoreUnknownValues": True}

      self.bq_service.tabledata().insertAll(
        projectId=self.PROJECT_ID,
        datasetId=DATASET_ID,
        tableId=table_name,
        body=data_chunk).execute(num_retries=MAX_RETRIES)
