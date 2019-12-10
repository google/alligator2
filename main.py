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
import logging
import sys

from api import API


class Alligator():

  @classmethod
  def sentiment_only(cls, project_id):
    api = API(project_id)
    api.sentiments()

  @classmethod
  def for_account_and_location(cls, project_id, account_id, location_id):
    api = API(project_id)
    api.reviews(u"accounts/{}/locations/{}".format(account_id, location_id))
    api.sentiments()

  @classmethod
  def for_account(cls, project_id, account_id):
    api = API(project_id)
    locations = api.locations(u"accounts/{}".format(account_id))

    for location in locations:
      location_name = location.get("name")
      api.reviews(location_name)

    api.sentiments()

  @classmethod
  def all(cls, project_id):
    api = API(project_id)
    accounts = api.accounts()

    for account in accounts:
      account_name = account.get("name")
      locations = api.locations(account_name)

      for location in locations:
        location_name = location.get("name")
        api.reviews(location_name)

    api.sentiments()


def main(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      "-p",
      "--project_id",
      required=True,
      type=str,
      help="a Google Cloud Project ID")
  parser.add_argument(
      "-a",
      "--account_id",
      type=str,
      help="retrieve and store all Google My Business reviews for a given Account ID"
  )
  parser.add_argument(
      "-l",
      "--location_id",
      type=str,
      help="retrieve and store all Google My Business reviews for a given Location ID (--account_id is also required)"
  )
  parser.add_argument(
      "-s",
      "--sentiment_only",
      help="process and store the sentiment of all available reviews for a project",
      action="store_true")
  parser.add_argument(
      "-v", "--verbose", help="increase output verbosity", action="store_true")
  args = parser.parse_args()

  verbose = args.verbose
  sentiment_only = args.sentiment_only
  project_id = args.project_id
  account_id = args.account_id
  location_id = args.location_id

  sys.argv.clear()

  if verbose:
    logging.basicConfig(level=logging.DEBUG)

  logging.info(u"Project ID:\t{}".format(project_id))
  logging.info(u"Account ID:\t{}".format(account_id))
  logging.info(u"Location ID:\t{}".format(location_id))

  if sentiment_only:
    print("Running sentiment analysis for all reviews in BigQuery...")
    Alligator.sentiment_only(project_id)
    sys.exit()

  print("Loading all Google My Business reviews into BigQuery...")
  if account_id and location_id:
    Alligator.for_account_and_location(project_id, account_id, location_id)
  elif account_id:
    Alligator.for_account(project_id, account_id)
  else:
    Alligator.all(project_id)

  print("Done.")


if __name__ == "__main__":
  main(sys.argv[1:])
