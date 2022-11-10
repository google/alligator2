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

from datetime import datetime, timedelta
from colorama import Fore, Style
import json
import logging
import os
import re
import sys

from urllib import parse
from babel import Locale
from babel.core import UnknownLocaleError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from oauthlib.oauth2.rfc6749.errors import InvalidGrantError
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from topic_clustering import TopicClustering

INVALID_REDIRECT_URI = "http://localhost:5678"
ACCOUNT_MANAGEMENT = "mybusinessaccountmanagement"
BUSINESS_INFORMATION = "mybusinessbusinessinformation"
FEDERATED_SERVICES = [ACCOUNT_MANAGEMENT, BUSINESS_INFORMATION]
GMB_DISCOVERY_FILE = "gmb_discovery.json"
CLIENT_SECRETS_FILE = "client_secrets.json"
TOKEN_FILE = "token.json"
SCHEMAS_FILE = "schemas.json"
SENTIMENTS_LASTRUN_FILE = "sentiments_lastrun"
SCOPES = [
    "https://www.googleapis.com/auth/business.manage",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-language",
]
DATASET_ID = "alligator"
MAX_RETRIES = 10
MIN_TOKENS = 20
INSIGHTS_DAYS_BACK = 540
CALLS_DAYS_BACK = 7
DIRECTIONS_NUM_DAYS = "SEVEN"
LOCATIONS_PER_PAGE = 100
BQ_JOBS_QUERY_MAXRESULTS_PER_PAGE = 1000
BQ_TABLEDATA_INSERTALL_BATCHSIZE = 50

LOCATIONS_READ_MASK = (
    "regularHours,latlng,labels,metadata,relationshipData,"
    "name,adWordsLocationExtensions,websiteUri,profile,"
    "storeCode,phoneNumbers,serviceArea,categories,"
    "storefrontAddress,languageCode,moreHours,specialHours,"
    "openInfo,title,serviceItems"
)

logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.CRITICAL)


class API(object):

  def __init__(self, project_id, language, flags):
    self.flags = flags
    client_secrets = os.path.join(
        os.path.dirname(__file__), CLIENT_SECRETS_FILE
    )

    creds = None

    if os.path.exists(TOKEN_FILE):
      creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
      if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
      else:
        flow = InstalledAppFlow.from_client_secrets_file(client_secrets, SCOPES)
        flow.redirect_uri = INVALID_REDIRECT_URI
        auth_url, _ = flow.authorization_url(prompt="consent")

        print(
            f"\n{Fore.GREEN}Please visit the following URL to"
            " authorize this application:"
        )
        print(f"\n{Style.BRIGHT}{auth_url}{Style.NORMAL}\n")
        print(
            "After allowing the application access, your browser should"
            " redirect to an invalid URL. Copy that URL from the address bar"
            " and paste it here to extract the necessary authorization"
            f" code.{Style.RESET_ALL}\n"
        )

        url = input("Please enter the URL: ").strip()
        code = parse.parse_qs(parse.urlparse(url).query)["code"][0]

        print()

        try:
          flow.fetch_token(code=code)
          creds = flow.credentials
        except InvalidGrantError as e:
          logging.error(f"Authentication has failed: {e}")
          sys.exit(1)
      with open(TOKEN_FILE, "w") as token:
        token.write(creds.to_json())
        logging.info(f"Succesfully created an authorization token.")

    self.gmb_services = {}
    for service_name in FEDERATED_SERVICES:
      self.gmb_services[service_name] = discovery.build(
          service_name, "v1", credentials=creds
      )

    with open(GMB_DISCOVERY_FILE) as gmb_discovery_file:
      self.gmb_service = discovery.build_from_document(
          gmb_discovery_file.read(),
          base="https://www.googleapis.com/",
          credentials=creds,
      )

    self.project_id = project_id
    self.dataset_exists = False
    self.existing_tables = {}
    self.language = language

    with open(SCHEMAS_FILE) as schemas_file:
      self.schemas = json.load(schemas_file)

    self.bq_service = discovery.build("bigquery", "v2", credentials=creds)
    self.nlp_service = discovery.build("language", "v1", credentials=creds)

    if flags["topic_clustering"]:
      self.topic_clustering = TopicClustering()

  def accounts(self):
    data = []
    page_token = None
    while True:
      response_json = (
          self.gmb_services[ACCOUNT_MANAGEMENT]
          .accounts()
          .list(pageToken=page_token)
          .execute(num_retries=MAX_RETRIES)
      )

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
        response_json = (
            self.gmb_services[BUSINESS_INFORMATION]
            .accounts()
            .locations()
            .list(
                parent=account_id,
                pageToken=page_token,
                pageSize=LOCATIONS_PER_PAGE,
                readMask=LOCATIONS_READ_MASK,
            )
            .execute(num_retries=MAX_RETRIES)
        )

        data = data + (response_json.get("locations") or [])
        page_token = response_json.get("nextPageToken")
        if not page_token:
          break

    else:
      response_json = (
          self.gmb_services[BUSINESS_INFORMATION]
          .locations()
          .get(name=location_id, readMask=LOCATIONS_READ_MASK)
          .execute(num_retries=MAX_RETRIES)
      )
      data = data + ([response_json] or [])

    logging.debug(json.dumps(data, indent=2))
    self.to_bigquery(table_name="locations", data=data)

    return data

  def reviews(self, location_id):
    page_token = None

    while True:
      try:
        response_json = (
            self.gmb_service.accounts()
            .locations()
            .reviews()
            .list(parent=location_id, pageToken=page_token)
            .execute(num_retries=MAX_RETRIES)
        )
      except HttpError as err:
        # Known bug on the GMB side, causing requests to return a 500
        # for locations with many thousands or reviews.
        # Workaround for now: stop listing reviews and log the error.
        logging.error(
            f"Failed to list reviews for location_id={location_id} "
            f"and pageToken={page_token} with error: {str(err)}"
        )
        break

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
      logging.info(
          f"Sentiment analysis last run: [{lastrun}]. Performing sentiment"
          " analysis on newer reviews..."
      )
    else:
      logging.info(
          "No previous run for sentiment analysis found. Performing sentiment"
          " analysis on all available reviews..."
      )

    query = {
        "query": f"""
          SELECT
            comment,
            name,
            reviewId
          FROM
            [{self.project_id}:{DATASET_ID}.reviews]
          WHERE
              LENGTH(comment) > 100
            AND (
              DATE(_PARTITIONTIME) > "{lastrun}"
              OR
                _PARTITIONTIME IS NULL)""",
        "maxResults": BQ_JOBS_QUERY_MAXRESULTS_PER_PAGE,
    }

    page_ctr = 1
    message = (
        "Fetching reviews for sentiment analysis..."
        f" [page_size={BQ_JOBS_QUERY_MAXRESULTS_PER_PAGE}][page={page_ctr}]"
    )
    logging.info(message)

    response_json = (
        self.bq_service.jobs()
        .query(projectId=self.project_id, body=query)
        .execute(num_retries=MAX_RETRIES)
    )

    rows = response_json.get("rows") or []
    self.process_sentiments(rows)

    page_token = response_json.get("pageToken")
    if page_token:
      job_id = response_json.get("jobReference").get("jobId")

      while True:
        page_ctr = page_ctr + 1
        logging.info(message)

        response_json_job = (
            self.bq_service.jobs()
            .getQueryResults(
                projectId=self.project_id,
                jobId=job_id,
                maxResults=BQ_JOBS_QUERY_MAXRESULTS_PER_PAGE,
                pageToken=page_token,
            )
            .execute(num_retries=MAX_RETRIES)
        )

        rows_job = response_json_job.get("rows") or []
        self.process_sentiments(rows_job)

        page_token = response_json_job.get("pageToken")
        if not page_token:
          break

    self.set_sentiments_lastrun()

  def get_sentiments_lastrun(self):
    lastrun_file_path = os.path.join(
        os.path.dirname(__file__), SENTIMENTS_LASTRUN_FILE
    )
    lastrun = datetime(year=1970, month=1, day=1).date()
    file_exists = False

    if os.path.isfile(lastrun_file_path):
      file_exists = True
      try:
        lastrun = datetime.fromtimestamp(
            os.path.getmtime(lastrun_file_path)
        ).date()
      except OSError:
        logging.warn(f"Path {lastrun_file_path} is inaccessible!")

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
        os.path.dirname(__file__), SENTIMENTS_LASTRUN_FILE
    )
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
        "document": {"type": "PLAIN_TEXT", "content": content},
        "features": {
            "extractSyntax": True,
            "extractEntities": True,
            "extractDocumentSentiment": True,
            "extractEntitySentiment": True,
            "classifyText": classify_text,
        },
        "encodingType": "UTF8",
    }

    if self.language:
      body["document"]["language"] = self.language

    try:
      return (
          self.nlp_service.documents()
          .annotateText(body=body)
          .execute(num_retries=MAX_RETRIES)
      )
    except HttpError as err:
      raise err

  def insights(self, location_id):
    end_time = (datetime.now() - timedelta(days=5)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    start_time = end_time - timedelta(days=INSIGHTS_DAYS_BACK)

    query = {
        "locationNames": [location_id],
        "basicRequest": {
            "metricRequests": {
                "metric": "ALL",
                "options": ["AGGREGATED_DAILY"],
            },
            "timeRange": {
                "startTime": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "endTime": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        },
    }

    data = []
    account_id = re.search(
        "(accounts/[0-9]+)/locations/[0-9]+", location_id, re.IGNORECASE
    ).group(1)

    response_json = (
        self.gmb_service.accounts()
        .locations()
        .reportInsights(name=account_id, body=query)
        .execute(num_retries=MAX_RETRIES)
    )

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
        "drivingDirectionsRequest": {"numDays": DIRECTIONS_NUM_DAYS},
    }

    if self.language:
      lang = "en_US"
      try:
        lang = Locale.parse(f"und_{self.language}")
      except UnknownLocaleError:
        logging.warning("Error parsing language code, falling back to en_US.")

      query["drivingDirectionsRequest"]["languageCode"] = str(lang)

    data = []
    account_id = re.search(
        "(accounts/[0-9]+)/locations/[0-9]+", location_id, re.IGNORECASE
    ).group(1)

    response_json = (
        self.gmb_service.accounts()
        .locations()
        .reportInsights(name=account_id, body=query)
        .execute(num_retries=MAX_RETRIES)
    )

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
                "options": ["BREAKDOWN_HOUR_OF_DAY"],
            }],
            "timeRange": {},
        },
    }

    account_id = re.search(
        "(accounts/[0-9]+)/locations/[0-9]+", location_id, re.IGNORECASE
    ).group(1)

    limit_end_time = (datetime.now() - timedelta(days=5)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    start_time = limit_end_time - timedelta(days=CALLS_DAYS_BACK)

    data = []

    while start_time < limit_end_time:
      end_time = start_time + timedelta(days=1)

      start_time_string = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
      end_time_string = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

      query["basicRequest"]["timeRange"] = {
          "startTime": start_time_string,
          "endTime": end_time_string,
      }

      response_json = (
          self.gmb_service.accounts()
          .locations()
          .reportInsights(name=account_id, body=query)
          .execute(num_retries=MAX_RETRIES)
      )

      if "locationMetrics" in response_json:
        for line in response_json.get("locationMetrics"):
          line["name"] = f"{line.get('locationName')}/{start_time_string}"
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
          projectId=self.project_id, datasetId=DATASET_ID
      ).execute(num_retries=MAX_RETRIES)

      logging.info(f"Dataset {self.project_id}:{DATASET_ID} already exists.")

      self.dataset_exists = True

      return
    except HttpError as err:
      if err.resp.status != 404:
        raise

    dataset = {
        "datasetReference": {
            "projectId": self.project_id,
            "datasetId": DATASET_ID,
        }
    }

    self.bq_service.datasets().insert(
        projectId=self.project_id, body=dataset
    ).execute(num_retries=MAX_RETRIES)

    self.dataset_exists = True

  def ensure_table_exists(self, table_name):
    if self.existing_tables.get(table_name):
      return

    try:
      self.bq_service.tables().get(
          projectId=self.project_id, datasetId=DATASET_ID, tableId=table_name
      ).execute(num_retries=MAX_RETRIES)

      logging.info(
          f"Table {self.project_id}:{DATASET_ID}.{table_name} already exists."
      )

      self.existing_tables[table_name] = True

      return
    except HttpError as err:
      if err.resp.status != 404:
        raise

    table = {
        "schema": {"fields": self.schemas.get(table_name)},
        "tableReference": {
            "projectId": self.project_id,
            "datasetId": DATASET_ID,
            "tableId": table_name,
        },
        "timePartitioning": {"type": "DAY"},
    }

    self.bq_service.tables().insert(
        projectId=self.project_id, datasetId=DATASET_ID, body=table
    ).execute(num_retries=MAX_RETRIES)

    self.existing_tables[table_name] = True

  def to_bigquery(self, table_name, data=[]):
    if not data:
      return

    self.ensure_dataset_exists()
    self.ensure_table_exists(table_name)

    rows = [{"json": line, "insertId": line.get("name")} for line in data]

    chunk_size = BQ_TABLEDATA_INSERTALL_BATCHSIZE
    chunked_rows = [
        rows[i * chunk_size : (i + 1) * chunk_size]
        for i in range((len(rows) + chunk_size - 1) // chunk_size)
    ]

    for chunk in chunked_rows:
      logging.info(
          f"Inserting {len(chunk)} rows into table"
          f" {self.project_id}:{DATASET_ID}.{table_name}."
      )

      data_chunk = {"rows": chunk, "ignoreUnknownValues": True}

      result = (
          self.bq_service.tabledata()
          .insertAll(
              projectId=self.project_id,
              datasetId=DATASET_ID,
              tableId=table_name,
              body=data_chunk,
          )
          .execute(num_retries=MAX_RETRIES)
      )
      if "insertErrors" in result:
        logging.error(
            "Errors found in the BigQuery insert operation. Details below."
        )
        logging.error(result["insertErrors"])
