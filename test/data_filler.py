"""Auxiliary class to generate fake data for Alligator."""
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

from faker import Faker
from geopy.geocoders import GoogleV3
import numpy as np
from transformers import pipeline, set_seed

STORE_NAMES = [
    "ABC Hypermarket",
    "DEF Supermarket",
    "GHI Express Market",
    "JKL Gas Station",
]
TIME_ZONES = ["America/New_York"]
REGION_CODES = []

ACCOUNTS_PAGES = 1
ACCOUNTS_PER_PAGE = 1

LOCATIONS_PAGES = 3
LOCATIONS_PER_PAGE = 25

INSIGHTS_MEAN = 30
INSIGHTS_STDDEV = 30

DIRECTIONS_MEAN = 15
DIRECTIONS_STDDEV = 30
DIRECTIONS_MAX = 15

HOURLY_CALLS_MEAN = 10
HOURLY_CALLS_STDDEV = 20

REVIEWS_PAGES = 3
REVIEWS_PER_PAGE = 15
REPLY_RATIO = 65

RATINGS = [
    "ONE",
    "TWO",
    "THREE",
    "THREE",
    "THREE",
    "FOUR",
    "FOUR",
    "FOUR",
    "FIVE",
    "FIVE",
]

LANGUAGE_CODE = "en"
COUNTRIES = ["US"]
LOCALE = ["en_US"]
PRIMARY_CATEGORIES = [("gcid:supermarket", "Supermarket")]

USE_GOOGLE_MAPS = False
GOOGLE_MAPS_API_KEY = ""

USE_GPT2_FOR_REVIEWS = False
GPT2_REVIEW_SEEDS = [
    (
        "The supermarket is excellent, great lighting and space to shop safely."
        " Great produce also."
    ),
    (
        "I love going to this store. They have great customer service in store"
        " and everything was clean."
    ),
    (
        "I live above this supermarket. They have a pretty good variety in"
        " everything. They sell a small variety of vegetables as well"
    ),
    (
        "I don't think I'll come back to this store, I couldn't find anything"
        " and the quality of the products was bad."
    ),
    (
        "The supermarket has a lot a good products in shelves, and they are"
        " always rotating discounts."
    ),
    (
        "Great variety and low prices in this store, but the air conditioner"
        " was broken and it was too hot in summer."
    ),
]

fake = Faker(LOCALE)
Faker.seed(25)

if USE_GPT2_FOR_REVIEWS:
  gpt2_generator = pipeline("text-generation", model="gpt2")
  set_seed(25)

locator = None
# Note: The locator can be changed to any other provider in the geopy library.
if USE_GOOGLE_MAPS:
  locator = GoogleV3(api_key=GOOGLE_MAPS_API_KEY)


class DataFiller(object):
  """Auxiliary class to generate fake data for Alligator.

  The class follows the same structure as the gmb service object used to
  extract data from the API.
  """

  def __init__(self):
    pass

  class accounts(object):  # pylint: disable=invalid-name
    """Simulates the accounts object in the gmb service object."""

    class list(object):  # pylint: disable=invalid-name
      """Simulates the accounts/list object in the gmb service object.

      Attributes:
        page_token: the page token to be decreased.
      """

      def __init__(self, pageToken=ACCOUNTS_PAGES):
        if not pageToken:
          self.page_token = ACCOUNTS_PAGES
        else:
          self.page_token = pageToken

      def execute(self, num_retries=None):
        """Generates a list of fake accounts.

        Args:
            num_retries: parameter ignored.
        Returns:
            A list of fake accounts.
        """
        del num_retries
        data = []
        for _ in range(ACCOUNTS_PER_PAGE):
          item = self.generate_account()
          data.append(item)
        next_page_token = self.page_token - 1
        composed_data = {"accounts": data, "nextPageToken": next_page_token}
        return composed_data

      def generate_account(self):
        """Generates a single fake account.

        Args:
            None.
        Returns:
            A single fake account.
        """
        item = {}
        item["accountName"] = fake.company()
        item["accountNumber"] = fake.password(
            length=10, lower_case=False, upper_case=False, special_chars=False
        )
        account_id = fake.password(
            length=21, lower_case=False, upper_case=False, special_chars=False
        )
        item["name"] = f"{account_id}"
        item["permissionLevel"] = "OWNER_LEVEL"
        item["role"] = "MANAGER"
        item["state"] = {"status": "VERIFIED", "vettedStatus": "NOT_VETTED"}
        item["type"] = "LOCATION_GROUP"
        return item

    class locations(object):  # pylint: disable=invalid-name
      """Simulates the accounts object in the gmb service object."""

      class list(object):  # pylint: disable=invalid-name
        """Simulates the accounts/locations/list object in the gmb service obj.

        Attributes:
          account_id: the account id to associate with the locations.
          page_token: the page token to be decreased.
        """

        def __init__(
            self,
            parent,
            pageToken=LOCATIONS_PAGES,
            pageSize=None,
            readMask=None,
        ):
          del pageSize, readMask
          self.account_id = parent
          if not pageToken:
            self.page_token = LOCATIONS_PAGES
          else:
            self.page_token = pageToken

        def execute(self, num_retries=None):
          """Generates a list of fake locations.

          Args:
              num_retries: parameter ignored.
          Returns:
              A list of fake locations.
          """
          del num_retries
          data = []
          for _ in range(LOCATIONS_PER_PAGE):
            item = self.generate_location()
            data.append(item)
          next_page_token = self.page_token - 1
          composed_data = {
              "locations": data,
              "nextPageToken": next_page_token,
          }
          return composed_data

        def generate_location_address(self):
          """Generates a fake location address using external APIs.

          Args:
              None.
          Returns:
              A single fake location address.
          """
          location_address = {}

          country = fake.random_element(elements=COUNTRIES)
          faker_info = fake.local_latlng(
              country_code=country, coords_only=False
          )

          location_address["latitude"] = faker_info[0]
          location_address["longitude"] = faker_info[1]
          location_address["locality"] = faker_info[2]
          location_address["country_code"] = faker_info[3]
          location_address["timezone"] = faker_info[4]
          location_address["country"] = country
          location_address["street_address"] = fake.street_address()
          location_address["postal_code"] = fake.postcode()

          if USE_GOOGLE_MAPS:
            location = locator.reverse(
                f'{location_address["latitude"]},'
                f' {location_address["longitude"]}'
            )

            if location:
              location_address["street_address"] = location.address
              postal_code_res = next(
                  (
                      sub
                      for sub in location.raw["address_components"]
                      if sub["types"] == ["postal_code"]
                  ),
                  None,
              )
              if postal_code_res:
                location_address["postal_code"] = postal_code_res["long_name"]

          return location_address

        def generate_location(self):
          """Generates a single fake location.

          Args:
              None.
          Returns:
              A single fake location.
          """
          # Pre-generate common info
          # local_info = fake.local_latlng(country_code=COUNTRY_CODE)
          location_address = self.generate_location_address()
          location_id = fake.password(
              length=21,
              lower_case=False,
              upper_case=False,
              special_chars=False,
          )

          category_and_name = fake.random_element(elements=PRIMARY_CATEGORIES)
          primary_phone = fake.phone_number()
          additional_phone = fake.phone_number()
          place_id = fake.password(
              length=10, lower_case=True, upper_case=True, special_chars=False
          )
          request_id = fake.password(
              length=18,
              lower_case=False,
              upper_case=False,
              special_chars=False,
          )
          store_id = "abc:" + fake.ean(length=8)
          description = fake.text()
          url = fake.url()
          opening_hour = 9
          closing_hour = 22

          item = {}

          item["adWordsLocationExtensions"] = {}

          item["phoneNumbers"] = {}
          item["phoneNumbers"]["primaryPhone"] = primary_phone
          item["phoneNumbers"]["additionalPhones"] = [additional_phone]

          item["storefrontAddress"] = {
              "addressLines": [location_address["street_address"]],
              "administrativeArea": location_address["locality"],
              "languageCode": LANGUAGE_CODE,
              "locality": location_address["locality"],
              "postalCode": location_address["postal_code"],
              "regionCode": location_address["country_code"],
          }

          item["labels"] = []
          item["languageCode"] = "es"
          item["latlng"] = {
              "latitude": location_address["latitude"],
              "longitude": location_address["longitude"],
          }
          item["title"] = fake.random_element(elements=STORE_NAMES)
          item["locationState"] = {
              "canDelete": True,
              "canUpdate": True,
              "isGoogleUpdated": True,
              "isLocalPostApiDisabled": True,
              "isPublished": True,
              "isVerified": fake.boolean(chance_of_getting_true=95),
          }
          item["metadata"] = {
              "mapsUri": "https://maps.google.com/maps?cid=",
              "newReviewUri": (
                  "https://search.google.com/local/writereview?placeid="
              ),
          }
          item["name"] = f"locations/{location_id}"
          item["openInfo"] = {"canReopen": True, "status": "OPEN"}
          hour_types = []
          for types in [
              ("Access", "ACCESS"),
              ("Brunch", "BRUNCH"),
              ("Delivery", "DELIVERY"),
              ("Drive through", "DRIVE_THROUGH"),
              ("Happy hours", "HAPPY_HOUR"),
              ("Kitchen", "KITCHEN"),
              ("Online service hours", "ONLINE_SERVICE_HOURS"),
              ("Pickup", "PICKUP"),
              ("Takeout", "TAKEOUT"),
              ("Senior hours", "SENIOR_HOURS"),
          ]:
            hour_types.append({
                "displayName": types[0],
                "hoursTypeId": types[1],
                "localizedDisplayName": types[0],
            })
          item["primaryCategory"] = {
              "categoryId": category_and_name[0],
              "displayName": category_and_name[1],
              "moreHoursTypes": hour_types,
          }

          item["profile"] = {"description": description}
          periods = []
          for day in [
              "MONDAY",
              "TUESDAY",
              "WEDNESDAY",
              "THURSDAY",
              "FRIDAY",
              "SATURDAY",
              "SUNDAY",
          ]:
            period = {
                "closeDay": day,
                "closeTime": {"hours": closing_hour},
                "openDay": day,
                "openTime": {"hours": opening_hour},
            }
            periods.append(period)
          item["regularHours"] = {"periods": periods}
          item["specialHours"] = {"specialHourPeriods": []}
          item["storeCode"] = store_id
          item["websiteUri"] = url
          return item

      class reviews(object):  # pylint: disable=invalid-name
        """Simulates the reviews object in the gmb service object."""

        class list(object):  # pylint: disable=invalid-name
          """Simulates the accounts/locations/reviews/list obj. in gmb service.

          Attributes:
            account_id: the account id to associate with the locations.
            page_token: the page token to be decreased.
          """

          def __init__(self, parent, pageToken=REVIEWS_PAGES):
            self.account_id = parent
            if not pageToken:
              self.page_token = REVIEWS_PAGES
            else:
              self.page_token = pageToken

          def execute(self, num_retries=None):
            """Generates a list of fake locations.

            Args:
                num_retries: parameter ignored.
            Returns:
                A list of fake reviews.
            """
            del num_retries
            data = []
            reviews_per_page = fake.random_int(min=1, max=REVIEWS_PER_PAGE)
            data = self.generate_reviews(reviews_per_page)

            next_page_token = self.page_token - 1
            composed_data = {
                "reviews": data,
                "nextPageToken": next_page_token,
                "averageRating": -1,
                "totalReviewCount": -1,
            }
            return composed_data

          def generate_reviews(self, reviews_to_generate):
            """Generates a fake reviews report for a single location.

            Args:
                reviews_to_generate: total number of reviews to generate.
            Returns:
                A fake reviews report.
            """
            data = []
            if USE_GPT2_FOR_REVIEWS:
              data = self.generate_gpt2_reviews(reviews_to_generate)
            else:
              for _ in range(reviews_to_generate):
                item = self.generate_single_review()
                data.append(item)

            return data

          def generate_gpt2_reviews(self, reviews_to_generate):
            """Generates a fake reviews report for a single location with gpt2.

            Args:
                reviews_to_generate: total number of reviews to generate.
            Returns:
                A fake reviews report.
            """
            data = []
            seed = fake.random_element(elements=GPT2_REVIEW_SEEDS)
            sentences = gpt2_generator(
                seed,
                max_length=100,
                num_return_sequences=reviews_to_generate,
            )
            for sentence in sentences:
              review_text = sentence["generated_text"].replace(seed, "")
              review = self.generate_single_review(review_text)
              data.append(review)
            return data

          def generate_single_review(self, review_text=None):
            """Generates a single review for a single location.

            Args:
                review_text: text to be used as review text.
            Returns:
                A fake review.
            """
            # Pre-generate common info
            review_id = fake.password(
                length=80,
                lower_case=True,
                upper_case=True,
                special_chars=False,
            )
            fake_date = fake.date_time_between(start_date="-1y", end_date="now")
            fake_date_str = fake_date.strftime("%Y-%m-%dT%H:%M:%SZ")

            item = {}
            item["reviewId"] = review_id
            item["reviewer"] = {
                "profilePhotoUrl": "N/A",
                "displayName": fake.name(),
            }
            item["starRating"] = fake.random_element(elements=RATINGS)
            if review_text:
              item["comment"] = review_text
            else:
              item["comment"] = fake.text()
            item["createTime"] = fake_date_str
            item["updateTime"] = fake_date_str
            item["name"] = f"{self.account_id}/reviews/{review_id}"

            # Random replies
            if fake.boolean(chance_of_getting_true=REPLY_RATIO):
              fake_reply_date = (
                  fake_date + timedelta(days=fake.random_int(min=1, max=15))
              ).strftime("%Y-%m-%dT%H:%M:%SZ")
              item["reviewReply"] = {
                  "comment": fake.text(),
                  "updateTime": fake_reply_date,
              }
            return item

      class reportInsights(object):  # pylint: disable=invalid-name
        """Simulates the accounts/locations/list object in the gmb service obj.

        Attributes:
          account_name: the full account name identifier.
          location_name: the full location name identifier.
          body: the request body.
        """

        def __init__(self, name, body):
          self.account_name = name
          self.body = body
          self.location_name = self.body["locationNames"][0]

        def execute(self, num_retries=None):
          """Generates a list of fake locations.

          Args:
              num_retries: parameter ignored.
          Returns:
              A list of fake accounts.
          """
          del num_retries
          data = []

          # Detect the type of insight to report
          metric_type = ""
          if "basicRequest" in self.body:
            metric_type = "locationMetrics"
            # General insights or hourly calls
            metric_requests = self.body["basicRequest"]["metricRequests"]
            if (
                "metric" in metric_requests
                and metric_requests["metric"] == "ALL"
            ):
              # General insights
              time_range = self.body["basicRequest"]["timeRange"]
              start_time = time_range["startTime"]
              end_time = time_range["endTime"]
              item = self.generate_insights(start_time, end_time)
              data.append(item)
            else:
              # This assumes ACTIONS_PHONE and BREAKDOWN_HOUR_OF_DAY.
              time_range = self.body["basicRequest"]["timeRange"]
              start_time = time_range["startTime"]
              item = self.generate_hourly_calls(start_time)
              data.append(item)
          elif "drivingDirectionsRequest" in self.body:
            metric_type = "locationDrivingDirectionMetrics"
            # This assumes DIRECTIONS_NUM_DAYS
            item = self.generate_directions()
            data.append(item)

          composed_data = {
              metric_type: data,
              "location_name": self.location_name,
          }

          return composed_data

        def generate_insights(self, start_time, end_time):
          """Generates a fake insights report for a single location.

          Args:
              start_time: start time of the report.
              end_time: end time of the report.
          Returns:
              A fake insights report.
          """
          # Pre-generate common info
          time_zone = fake.random_element(elements=TIME_ZONES)

          item = {}
          item["locationName"] = self.location_name
          item["name"] = self.location_name
          item["timeZone"] = time_zone

          start_datetime = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
          end_datetime = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ")
          delta = end_datetime - start_datetime

          item["metricValues"] = []
          for metric in [
              ("QUERIES_DIRECT", 0.4),
              ("QUERIES_INDIRECT", 0.6),
              ("QUERIES_CHAIN", 0.3),
              ("VIEWS_MAPS", 1.3),
              ("VIEWS_SEARCH", 1.1),
              ("ACTIONS_WEBSITE", 0.3),
              ("ACTIONS_PHONE", 0.6),
              ("ACTIONS_DRIVING_DIRECTIONS", 1),
              ("PHOTOS_VIEWS_MERCHANT", 0.5),
              ("PHOTOS_VIEWS_CUSTOMERS", 0.5),
              ("PHOTOS_COUNT_MERCHANT", 0.5),
              ("PHOTOS_COUNT_CUSTOMERS", 0.5),
              ("LOCAL_POST_VIEWS_SEARCH", 0.8),
          ]:
            subitem = {}
            subitem["metric"] = metric[0]
            multiplier = metric[1]
            subitem["dimensionalValues"] = []

            values = (
                np.random.normal(INSIGHTS_MEAN, INSIGHTS_STDDEV, delta.days)
                .astype(int)
                .clip(0)
            )
            current_day = start_datetime
            for value in values:
              adjusted_value = value
              # This reduces metrics on Saturday and Sunday, adjust accordingly.
              current_day_of_week = current_day.weekday()
              if current_day_of_week > 4:
                adjusted_value = value * 0.2
              dimensional_value = {
                  "metricOption": metric[0],
                  "timeDimension": {
                      "timeRange": {
                          "startTime": current_day.strftime(
                              "%Y-%m-%dT%H:%M:%SZ"
                          )
                      }
                  },
                  "value": int(adjusted_value * multiplier),
              }
              subitem["dimensionalValues"].append(dimensional_value)
              current_day += timedelta(days=1)
            item["metricValues"].append(subitem)
          return item

        def generate_hourly_calls(self, start_time):
          """Generates a fake hourly calls report for a single location.

          Args:
              start_time: start time of the report.
          Returns:
              A fake hourly calls report.
          """
          # Pre-generate common info
          start_datetime = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
          day_of_week = start_datetime.weekday()
          adjusted_mean = HOURLY_CALLS_MEAN
          adjusted_stddev = HOURLY_CALLS_STDDEV
          # This reduces metrics on Saturday and Sunday, adjust accordingly.
          if day_of_week > 4:
            adjusted_mean = HOURLY_CALLS_MEAN * 0.1
            adjusted_stddev = HOURLY_CALLS_STDDEV * 0.3
          time_zone = fake.random_element(elements=TIME_ZONES)

          item = {}
          item["locationName"] = self.location_name
          item["timeZone"] = time_zone

          item["metricValues"] = []
          for metric in [("BREAKDOWN_HOUR_OF_DAY", 1)]:
            subitem = {}
            subitem["metric"] = metric[0]
            multiplier = metric[1]
            subitem["dimensionalValues"] = []

            values = (
                np.random.normal(adjusted_mean, adjusted_stddev, 24)
                .astype(int)
                .clip(0)
            )
            current_hour = 0
            for value in values:
              if current_hour < 6 or current_hour > 22:
                value = fake.random_int(max=int(HOURLY_CALLS_MEAN / 4))
              dimensional_value = {
                  "metricOption": metric[0],
                  "timeDimension": {"timeOfDay": {"hours": current_hour}},
                  "value": int(value * multiplier),
              }
              subitem["dimensionalValues"].append(dimensional_value)
              current_hour += 1
            item["metricValues"].append(subitem)
          return item

        def generate_directions(self):
          """Generates a fake directions report for a single location.

          Args:
              None.
          Returns:
              A fake directions calls report.
          """
          # Pre-generate common info
          time_zone = fake.random_element(elements=TIME_ZONES)

          item = {}
          item["locationName"] = self.location_name
          item["timeZone"] = time_zone

          item["topDirectionSources"] = []
          subitem = {}
          subitem["dayCount"] = 7
          subitem["regionCounts"] = []

          total_counts = fake.random_int(max=DIRECTIONS_MAX)
          values = (
              np.random.normal(DIRECTIONS_MEAN, DIRECTIONS_STDDEV, total_counts)
              .astype(int)
              .clip(0)
          )

          for value in values:
            # TODO(pending): relate latlng with post code.
            count = {}
            latlng = fake.latlng()
            count["latlng"] = {
                "latitude": float(latlng[0]),
                "longitude": float(latlng[1]),
            }
            count["label"] = fake.postcode()
            count["count"] = int(value)

            subitem["regionCounts"].append(count)

          item["topDirectionSources"].append(subitem)
          return item
