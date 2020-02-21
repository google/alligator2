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

INSIGHTS = 'insights'
REVIEWS = 'reviews'
SENTIMENT = 'sentiment'
DIRECTIONS = 'directions'
HOURLY_CALLS = 'hourly_calls'


class Alligator():

  @classmethod
  def sentiment_only(cls, project_id):
    api = API(project_id)
    api.sentiments()

  @classmethod
  def for_account_and_location(cls, project_id, account_id, location_id, flags):
    api = API(project_id)

    location_name = u"accounts/{}/locations/{}".format(account_id, location_id)

    api.locations(u"accounts/{}".format(account_id), location_id=location_name)

    if flags[INSIGHTS]:
      logging.info("Insights...")
      api.insights(location_name)
    if flags[DIRECTIONS]:
      logging.info("Directions...")
      api.directions(location_name)
    if flags[HOURLY_CALLS]:
      logging.info("Hourly calls...")
      api.hourly_calls(location_name)
    if flags[REVIEWS]:
      logging.info("Reviews...")
      api.reviews(location_name)
    if flags[SENTIMENT]:
      logging.info("Sentiments...")
      api.sentiments()

  @classmethod
  def for_account(cls, project_id, account_id, flags):
    api = API(project_id)
    locations = api.locations(u"accounts/{}".format(account_id))

    for location in locations:
      location_name = location.get("name")
      if flags[INSIGHTS]:
        api.insights(location_name)
      if flags[DIRECTIONS]:
        api.directions(location_name)
      if flags[HOURLY_CALLS]:
        api.hourly_calls(location_name)
      if flags[REVIEWS]:
        api.reviews(location_name)

    if flags[SENTIMENT]:
      api.sentiments()

  @classmethod
  def all(cls, project_id, flags):
    api = API(project_id)
    accounts = api.accounts()

    for account in accounts:
      account_name = account.get("name")
      locations = api.locations(account_name)

      for location in locations:
        location_name = location.get("name")
        if flags[INSIGHTS]:
          api.insights(location_name)
        if flags[DIRECTIONS]:
          api.directions(location_name)
        if flags[HOURLY_CALLS]:
          api.hourly_calls(location_name)
        if flags[REVIEWS]:
          api.reviews(location_name)

    if flags[SENTIMENT]:
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
      "--no_insights",
      help="skip the insights processing and storage",
      action="store_true")
  parser.add_argument(
      "--no_reviews",
      help="skip the reviews processing and storage",
      action="store_true")
  parser.add_argument(
      "--no_sentiment",
      help="skip the sentiment processing and storage",
      action="store_true")
  parser.add_argument(
      "--no_directions",
      help="skip the directions processing and storage",
      action="store_true")
  parser.add_argument(
      "--no_hourly_calls",
      help="skip the hourly calls processing and storage",
      action="store_true")
  parser.add_argument(
      "--sentiment_only",
      help="only process and store the sentiment of all available reviews (if --no-sentiment is provided, no action is performed)",
      action="store_true")
  parser.add_argument(
      "-v", "--verbose", help="increase output verbosity", action="store_true")
  args = parser.parse_args()

  project_id = args.project_id
  account_id = args.account_id
  location_id = args.location_id

  flags = {}
  flags[INSIGHTS] = not args.no_insights
  flags[DIRECTIONS] = not args.no_directions
  flags[HOURLY_CALLS] = not args.no_hourly_calls
  flags[REVIEWS] = not args.no_reviews
  flags[SENTIMENT] = not args.no_sentiment

  sentiment_only = args.sentiment_only
  verbose = args.verbose

  sys.argv.clear()

  if verbose:
    logging.basicConfig(level=logging.DEBUG)

  logging.info(u"Project ID:\t%s", project_id)
  logging.info(u"Account ID:\t%s", account_id)
  logging.info(u"Location ID:\t%s", location_id)

  if sentiment_only:
    if flags[SENTIMENT]:
      print("Running sentiment analysis for all reviews in BigQuery...")
      Alligator.sentiment_only(project_id)
    else:
      print(
          "No action will be performed as --no_sentiment and --sentiment_only flags have been provided."
      )
    sys.exit()

  print("Loading Google My Business reviews into BigQuery...")

  if account_id and location_id:
    Alligator.for_account_and_location(project_id, account_id, location_id,
                                       flags)
  elif account_id:
    Alligator.for_account(project_id, account_id, flags)
  else:
    Alligator.all(project_id, flags)

  print("Done.")


if __name__ == "__main__":
  main(sys.argv[1:])
