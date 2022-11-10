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

INSIGHTS = "insights"
REVIEWS = "reviews"
SENTIMENT = "sentiment"
DIRECTIONS = "directions"
HOURLY_CALLS = "hourly_calls"
TOPIC_CLUSTERING = "topic_clustering"


class Alligator:

  @classmethod
  def sentiment_only(cls, project_id, language, flags):
    api = API(project_id, language, flags)
    api.sentiments()

  @classmethod
  def for_account_and_location(
      cls, project_id, account_id, location_id, language, flags
  ):
    api = API(project_id, language, flags)

    location_name = f"locations/{location_id}"
    account_name = f"accounts/{account_id}"
    legacy_location_name = f"accounts/{account_id}/locations/{location_id}"

    api.locations(account_id=account_name, location_id=location_name)

    if flags[INSIGHTS]:
      api.insights(legacy_location_name)
    if flags[DIRECTIONS]:
      api.directions(legacy_location_name)
    if flags[HOURLY_CALLS]:
      api.hourly_calls(legacy_location_name)
    if flags[REVIEWS]:
      api.reviews(legacy_location_name)
    if flags[SENTIMENT]:
      api.sentiments()

  @classmethod
  def for_account(cls, project_id, account_id, language, flags):
    account_name = f"accounts/{account_id}"

    api = API(project_id, language, flags)
    locations = api.locations(account_id=account_name)
    num_locations = len(locations)
    loc_ctr = 1

    for location in locations:
      logging.info(f"Processing location {loc_ctr} of {num_locations}...")

      legacy_location_name = f"{account_name}/{location.get('name')}"

      if flags[INSIGHTS]:
        api.insights(legacy_location_name)
      if flags[DIRECTIONS]:
        api.directions(legacy_location_name)
      if flags[HOURLY_CALLS]:
        api.hourly_calls(legacy_location_name)
      if flags[REVIEWS]:
        api.reviews(legacy_location_name)

      loc_ctr = loc_ctr + 1

    if flags[SENTIMENT]:
      api.sentiments()

  @classmethod
  def all(cls, project_id, language, flags):
    api = API(project_id, language, flags)
    accounts = api.accounts()
    num_accounts = len(accounts)
    ac_ctr = 1

    for account in accounts:
      logging.info(f"Processing account {ac_ctr} of {num_accounts}...")

      account_name = account.get("name")
      locations = api.locations(account_name)
      num_locations = len(locations)
      loc_ctr = 1

      for location in locations:
        logging.info(f"Processing location {loc_ctr} of {num_locations}...")

        legacy_location_name = f"{account_name}/{location.get('name')}"

        if flags[INSIGHTS]:
          api.insights(legacy_location_name)
        if flags[DIRECTIONS]:
          api.directions(legacy_location_name)
        if flags[HOURLY_CALLS]:
          api.hourly_calls(legacy_location_name)
        if flags[REVIEWS]:
          api.reviews(legacy_location_name)

        loc_ctr = loc_ctr + 1
      ac_ctr = ac_ctr + 1

    if flags[SENTIMENT]:
      api.sentiments()


def main(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      "-p",
      "--project_id",
      required=True,
      type=str,
      help="a Google Cloud Project ID",
  )
  parser.add_argument(
      "-a",
      "--account_id",
      type=str,
      help=(
          "retrieve and store all Google My Business reviews for a given"
          " Account ID"
      ),
  )
  parser.add_argument(
      "-l",
      "--location_id",
      type=str,
      help=(
          "retrieve and store all Google My Business reviews for a given"
          " Location ID (--account_id is also required)"
      ),
  )
  parser.add_argument(
      "--language",
      type=str,
      help=(
          "the ISO-639-1 language code in which the Google My Business reviews"
          " are written (used for sentiment processing)"
      ),
  )
  parser.add_argument(
      "--no_insights",
      help="skip the insights processing and storage",
      action="store_true",
  )
  parser.add_argument(
      "--no_reviews",
      help="skip the reviews processing and storage",
      action="store_true",
  )
  parser.add_argument(
      "--no_sentiment",
      help="skip the sentiment processing and storage",
      action="store_true",
  )
  parser.add_argument(
      "--no_directions",
      help="skip the directions processing and storage",
      action="store_true",
  )
  parser.add_argument(
      "--no_hourly_calls",
      help="skip the hourly calls processing and storage",
      action="store_true",
  )
  parser.add_argument(
      "--no_topic_clustering",
      help="skip the extraction of topics for each reviews",
      action="store_true",
  )
  parser.add_argument(
      "--sentiment_only",
      help=(
          "only process and store the sentiment of all available reviews (if"
          " --no-sentiment is provided, no action is performed)"
      ),
      action="store_true",
  )
  parser.add_argument(
      "-q",
      "--quiet",
      help="only show warning and error messages (overrides --verbose)",
      action="store_true",
  )
  parser.add_argument(
      "-v", "--verbose", help="increase output verbosity", action="store_true"
  )
  args = parser.parse_args()

  project_id = args.project_id
  account_id = args.account_id
  location_id = args.location_id
  language = args.language

  flags = {}
  flags[INSIGHTS] = not args.no_insights
  flags[DIRECTIONS] = not args.no_directions
  flags[HOURLY_CALLS] = not args.no_hourly_calls
  flags[REVIEWS] = not args.no_reviews
  flags[SENTIMENT] = not args.no_sentiment
  flags[TOPIC_CLUSTERING] = not args.no_topic_clustering

  sentiment_only = args.sentiment_only
  quiet = args.quiet
  verbose = args.verbose

  sys.argv.clear()

  log_format = "[%(asctime)s] %(levelname)s [%(name)s] %(message)s"
  date_format = "%H:%M:%S"
  if quiet:
    logging.basicConfig(
        format=log_format, datefmt=date_format, level=logging.WARNING
    )
  elif verbose:
    logging.basicConfig(
        format=log_format, datefmt=date_format, level=logging.DEBUG
    )
  else:
    logging.basicConfig(
        format=log_format, datefmt=date_format, level=logging.INFO
    )

  logging.info("Project ID:\t%s", project_id)
  logging.info("Account ID:\t%s", account_id)
  logging.info("Location ID:\t%s", location_id)
  logging.info("Language:\t%s", language)

  if sentiment_only:
    if flags[SENTIMENT]:
      logging.info("Running sentiment analysis for all reviews in BigQuery...")
      Alligator.sentiment_only(project_id, language, flags)
    else:
      logging.warning(
          "No action will be performed as --no_sentiment and --sentiment_only"
          " flags have been provided."
      )
    sys.exit()

  logging.info("Loading Google My Business reviews into BigQuery...")

  if account_id and location_id:
    Alligator.for_account_and_location(
        project_id, account_id, location_id, language, flags
    )
  elif account_id:
    Alligator.for_account(project_id, account_id, language, flags)
  else:
    Alligator.all(project_id, language, flags)

  logging.info("Done.")


if __name__ == "__main__":
  main(sys.argv[1:])
