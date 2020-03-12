# Alligator 2.0

This tool is a Python-based solution that aggregates _Insights_ data from the Google My Business (GMB) API and stores it into Google Cloud Platform (GCP), precisely in BigQuery. `GMB Insights` data provides details around how users interact with GMB listings via Google Maps, such as the number of queries for a location, locations of where people searched for directions, number of website clicks, calls, and reviews. The tool provides a cross-account look at the GMB data instead of a per-location view.

Along with gathering stats, the Google Cloud Natural Language (NLP) API is used to provide sentiment analysis and entity detection for supported languages. This is a fully automated process that pulls data from the GMB API and places it into a BigQuery instance, processing each review's content with the NLP API to generate a sentiment score for analysis.

**_This is not an officially supported Google product. It is a reference implementation._**

## Table of Contents

* [GMB Account Prerequisites](#gmb-account-prerequisites)
* [Installation and Steup](#installation-and-setup)
  * [Install Python Libraries](#install-python-libraries)
  * [GCP Setup](#gcp-setup)
  * [Install OAuth2 Credentials](#install-oauth2-credentials)
  * [Download the GMB API Discovery Document](#download-the-gmb-api-discovery-document)
  * [Run the Solution](#run-the-solution)
  * [CLI Usage](#cli-usage)
* [Notes](#notes)

## GMB Account Prerequisites

* All locations must roll up to a `Location Group` (formerly known as `Business Account`). Click [here](https://support.google.com/business/answer/6085339?ref_topic=6085325) for more information. Multiple location groups are supported and can be queried accordingly (refer to the samples inside the [bigquery-views](samples/bigquery-views/) directory.
* All locations must be _verified_

## Installation and Setup

### Install Python Libraries

Install the required dependencies

`$ pip install --upgrade --quiet --requirement requirements.txt`

### GCP Setup

Follow the steps to **Enable the API** within the [GMB basic setup guide](https://developers.google.com/my-business/content/basic-setup) and create the necessary OAuth2 Credentials required for the next steps.

Go to [Enable the Cloud Natural Language API](https://console.cloud.google.com/flows/enableapi?apiid=language.googleapis.com).

Go to [Enable the BigQuery API](https://console.cloud.google.com/flows/enableapi?apiid=bigquery).

Please note that BigQuery provides a [sandbox](https://cloud.google.com/bigquery/docs/sandbox) if you do not want to provide a credit card or enable billing for your project. The steps in this topic work for a project whether or not your project has billing enabled. If you optionally want to enable billing, see [Learn how to enable billing](https://cloud.google.com/billing/docs/how-to/modify-project).

### Install OAuth2 Credentials

Create a file named `client_secrets.json`, with the credentials downloaded as JSON from your GCP Project API Console.

### Download the GMB API Discovery Document

Go to the [Samples page](https://developers.google.com/my-business/samples/#discovery_document), right click **Download discovery document**, and select **Save Link As**. Then, save the file as `gmb_discovery.json` in the same directory.

### Run the solution

Execute the script to start the process of retrieving the reviews for all available locations from all accessible accounts for the authorized user:

`$ python main.py --project_id=<PROJECT_ID>`

The script generates a number of tables in an `alligator` BigQuery dataset.

### CLI Usage

Usage:

```bash
$ python main.py [-h] -p PROJECT_ID [-a ACCOUNT_ID] [-l LOCATION_ID]
                 [--no_insights] [--no_reviews] [--no_sentiment]
                 [--no_directions] [--no_hourly_calls] [--sentiment_only] [-v]
```

Optional arguments:

```bash
-h, --help            show this help message and exit
-p PROJECT_ID, --project_id PROJECT_ID
                      a Google Cloud Project ID
-a ACCOUNT_ID, --account_id ACCOUNT_ID
                      retrieve and store all Google My Business reviews for
                      a given Account ID
-l LOCATION_ID, --location_id LOCATION_ID
                      retrieve and store all Google My Business reviews for
                      a given Location ID (--account_id is also required)
--no_insights         skip the insights processing and storage
--no_reviews          skip the reviews processing and storage
--no_sentiment        skip the sentiment processing and storage
--no_directions       skip the directions processing and storage
--no_hourly_calls     skip the hourly calls processing and storage
--sentiment_only      only process and store the sentiment of all available
                      reviews (if --no-sentiment is provided, no action is
                      performed)
-v, --verbose         increase output verbosity
```

## Notes

For the initial data load into BigQuery, a maximum of 18 months of insights data will be retrieved, up to 5 days prior to the current date. This is due to the posted 3-5 day delay on the data becoming available in the GMB API. For _phone calls_ and _driving directions_, only data from the last 7 days is retrieved. Finally, data is inserted into BigQuery with a batch size of 5000 to avoid running into API limits, especially when using the BigQuery Sandbox. These defaults are defined in [api.py](api.py) and can be tuned according to indiviual needs.

## Authors

* Tony Coconate (coconate@google.com) – Google
* Miguel Fernandes (miguelfc@google.com) – Google
* Mohab Fekry (mfekry@google.com) – Google
* David Harcombe (davidharcombe@google.com) – Google
