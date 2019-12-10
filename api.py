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

import json
import logging
import os
import sys

from googleapiclient import discovery
from googleapiclient.http import build_http
from googleapiclient.errors import HttpError
from oauth2client import client, file, tools

GMB_DISCOVERY_FILE = "gmb_discovery.json"
CLIENT_SECRETS_FILE = "client_secrets.json"
CREDENTIALS_STORAGE = "credentials.dat"
SCHEMAS_FILE = "schemas.json"
SCOPES = [
    "https://www.googleapis.com/auth/business.manage",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-language"
]
DATASET_ID = "alligator"
MAX_RETRIES = 10
MIN_TOKENS = 20

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
      credentials = tools.run_flow(flow, storage)

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

  def locations(self, account_id):
    data = []
    page_token = None

    while True:
      response_json = self.gmb_service.accounts().locations().list(
          parent=account_id,
          pageToken=page_token).execute(num_retries=MAX_RETRIES)

      data = data + (response_json.get("locations") or [])

      page_token = response_json.get("nextPageToken")
      if not page_token:
        break

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

    while True:
      response_json = self.bq_service.tabledata().list(
          projectId=self.PROJECT_ID,
          datasetId=DATASET_ID,
          tableId="reviews",
          selectedFields="reviewId,name,comment",
          pageToken=page_token).execute(num_retries=MAX_RETRIES)

      rows = response_json.get("rows") or []
      sentiments = []

      for row in rows:
        sentiment = {}
        sentiment["name"] = row.get("f")[1].get("v")
        sentiment["reviewId"] = row.get("f")[2].get("v")
        sentiment["comment"] = row.get("f")[0].get("v")
        sentiment["annotation"] = self.annotate_text(row.get("f")[0].get("v"))

        sentiments.append(sentiment)

      logging.info(json.dumps(sentiments, indent=2))
      self.to_bigquery(table_name="sentiments", data=sentiments)

      page_token = response_json.get("nextPageToken")
      if not page_token:
        break

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
    data = {"rows": rows, "ignoreUnknownValues": True}

    self.bq_service.tabledata().insertAll(
        projectId=self.PROJECT_ID,
        datasetId=DATASET_ID,
        tableId=table_name,
        body=data).execute(num_retries=MAX_RETRIES)
