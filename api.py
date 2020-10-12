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
from datetime import datetime
from datetime import timedelta
import json
import logging
import os
import re

from babel import Locale
from babel.core import UnknownLocaleError
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from googleapiclient.http import build_http
from oauth2client import client, file, tools
from topic_clustering import TopicClustering

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
BQ_TABLEDATA_INSERTALL_BATCHSIZE = 50

logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.CRITICAL)


class API(object):

  def __init__(self, project_id, language, flags):
    self.flags = flags
    client_secrets = os.path.join(
        os.path.dirname(__file__), CLIENT_SECRETS_FILE)

    flow = client.flow_from_clientsecrets(
        client_secrets,
        SCOPES,
        message=tools.message_if_missing(client_secrets))

    storage = file.Storage(CREDENTIALS_STORAGE)
    credentials = storage.get()

    if credentials is None or credentials.invalid:
      credential_flags = argparse.Namespace(
          noauth_local_webserver=True,
          logging_level=logging.getLevelName(
              logging.getLogger().getEffectiveLevel()))
      credentials = tools.run_flow(flow, storage, flags=credential_flags)

    http = credentials.authorize(http=build_http())

    with open(GMB_DISCOVERY_FILE) as gmb_discovery_file:
      self.gmb_service = discovery.build_from_document(
          gmb_discovery_file.read(),
          base="https://www.googleapis.com/",
          http=http)

    self.project_id = project_id
    self.dataset_exists = False
    self.existing_tables = {}
    self.language = language

    with open(SCHEMAS_FILE) as schemas_file:
      self.schemas = json.load(schemas_file)

    self.bq_service = discovery.build("bigquery", "v2", http=http)
    self.nlp_service = discovery.build("language", "v1", http=http)

    if flags["topic_clustering"]:
      self.topic_clustering = TopicClustering()

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

    logging.debug(json.dumps(data, indent=2))

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
      data = data + ([response_json] or [])

    logging.debug(json.dumps(data, indent=2))
    self.to_bigquery(table_name="locations", data=data)

    return data

  def reviews(self, location_id):
    page_token = None

    while True:
      response_json = self.gmb_service.accounts().locations().reviews().list(
          parent=location_id,
          pageToken=page_token).execute(num_retries=MAX_RETRIES)

      data = response_json.get("reviews") or []
      logging.debug(json.dumps(data, indent=2))
      self.to_bigquery(table_name="reviews", data=data)

      page_token = response_json.get("nextPageToken")
      if not page_token:
        break

  def sentiments(self):
    page_token = None
    lastrun, file_exists = self.get_sentiments_lastrun()

    self.ensure_dataset_exists()
    self.ensure_table_exists(table_name="reviews")

    if file_exists:
      logging.info(("Sentiment analysis last run: [{}]. "
                   "Performing sentiment analysis on newer reviews...")
                   .format(lastrun))
    else:
      logging.info("No previous run for sentiment analysis found. "
                  "Performing sentiment analysis on all available reviews...")

    query = {
        "query":
            """
        SELECT
          comment,
          name,
          reviewId
        FROM
          [{projectId}:{datasetId}.reviews]
        WHERE
            LENGTH(comment) > 100
          AND (
            DATE(_PARTITIONTIME) > "{lastrun}"
            OR
              _PARTITIONTIME IS NULL)
      """.format(
          projectId=self.project_id,
          datasetId=DATASET_ID,
          lastrun=lastrun),
        "maxResults":
            BQ_JOBS_QUERY_MAXRESULTS_PER_PAGE
    }

    page_ctr = 1
    message = ("Fetching reviews for sentiment analysis... "
              "[page_size={}][page={}]")
    logging.info(message.format(BQ_JOBS_QUERY_MAXRESULTS_PER_PAGE, page_ctr))

    response_json = self.bq_service.jobs().query(
        projectId=self.project_id, body=query).execute(num_retries=MAX_RETRIES)

    rows = response_json.get("rows") or []
    self.process_sentiments(rows)

    page_token = response_json.get("pageToken")
    if page_token:
      job_id = response_json.get("jobReference").get("jobId")

      while True:
        page_ctr = page_ctr + 1
        logging.info(message.format(BQ_JOBS_QUERY_MAXRESULTS_PER_PAGE, page_ctr))

        response_json_job = self.bq_service.jobs().getQueryResults(
            projectId=self.project_id,
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
    file_exists = False

    if os.path.isfile(lastrun_file_path):
      file_exists = True
      try:
        lastrun = datetime.fromtimestamp(
          os.path.getmtime(lastrun_file_path)).date()
      except OSError:
        logging.warn("Path {} is inaccessible!"
          .format(lastrun_file_path))

    return lastrun, file_exists

  def process_sentiments(self, rows):
    sentiments = []

    for row in rows:
      sentiment = {}
      comment = row.get("f")[0].get("v")
      sentiment["comment"] = comment
      sentiment["name"] = row.get("f")[1].get("v")
      sentiment["reviewId"] = row.get("f")[2].get("v")
      annotated_text = self.annotate_text(comment)
      sentiment["annotation"] = annotated_text

      sentiments.append(sentiment)

    if sentiments and self.topic_clustering:
      if sentiments:
        logging.info("Determining topics for the current batch of reviews...")
        self.topic_clustering.determine_topics(sentiments)

    logging.debug(json.dumps(sentiments, indent=2))

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

    valid_content = len(content.split()) > MIN_TOKENS
    supported_lang = self.language == "en_US"
    classify_text = valid_content and supported_lang

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
            "classifyText": classify_text
        },
        "encodingType": "UTF8"
    }

    if self.language:
      body["document"]["language"] = self.language

    try:
      return self.nlp_service.documents().annotateText(body=body).execute(
          num_retries=MAX_RETRIES)
    except HttpError as err:
      raise err

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

      logging.debug(json.dumps(data, indent=2))
      self.to_bigquery(table_name="insights", data=data)

    else:
      logging.warning("No insights reported for %s", location_id)

    return data

  def directions(self, location_id):
    query = {
        "locationNames": [location_id],
        "drivingDirectionsRequest": {
            "numDays": DIRECTIONS_NUM_DAYS
        }
    }

    if self.language:
      lang = "en_US"
      try:
        lang = Locale.parse("und_{}".format(self.language))
      except UnknownLocaleError:
        logging.warning("Error parsing language code, falling back to en_US.")

      query["drivingDirectionsRequest"]["languageCode"] = str(lang)

    data = []
    account_id = re.search("(accounts/[0-9]+)/locations/[0-9]+", location_id,
                           re.IGNORECASE).group(1)

    response_json = self.gmb_service.accounts().locations().reportInsights(
        name=account_id, body=query).execute(num_retries=MAX_RETRIES)

    if "locationDrivingDirectionMetrics" in response_json:
      for line in response_json.get("locationDrivingDirectionMetrics"):
        line["name"] = line.get("locationName")
        data.append(line)

      logging.debug(json.dumps(data, indent=2))
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
      logging.debug(json.dumps(data, indent=2))
      self.to_bigquery(table_name="hourly_calls", data=data)

    return data

  def ensure_dataset_exists(self):
    if self.dataset_exists:
      return

    try:
      self.bq_service.datasets().get(
          projectId=self.project_id,
          datasetId=DATASET_ID).execute(num_retries=MAX_RETRIES)

      logging.info(u"Dataset {}:{} already exists.".format(
          self.project_id, DATASET_ID))

      self.dataset_exists = True

      return
    except HttpError as err:
      if err.resp.status != 404:
        raise

    dataset = {
        "datasetReference": {
            "projectId": self.project_id,
            "datasetId": DATASET_ID
        }
    }

    self.bq_service.datasets().insert(
        projectId=self.project_id,
        body=dataset).execute(num_retries=MAX_RETRIES)

    self.dataset_exists = True

  def ensure_table_exists(self, table_name):
    if self.existing_tables.get(table_name):
      return

    try:
      self.bq_service.tables().get(
          projectId=self.project_id, datasetId=DATASET_ID,
          tableId=table_name).execute(num_retries=MAX_RETRIES)

      logging.info(u"Table {}:{}.{} already exists.".format(
          self.project_id, DATASET_ID, table_name))

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
            "projectId": self.project_id,
            "datasetId": DATASET_ID,
            "tableId": table_name
        },
        "timePartitioning": {
            "type": "DAY"
        }
    }

    self.bq_service.tables().insert(
        projectId=self.project_id, datasetId=DATASET_ID,
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
          len(chunk), self.project_id, DATASET_ID, table_name))

      data_chunk = {"rows": chunk, "ignoreUnknownValues": True}

      self.bq_service.tabledata().insertAll(
          projectId=self.project_id,
          datasetId=DATASET_ID,
          tableId=table_name,
          body=data_chunk).execute(num_retries=MAX_RETRIES)
