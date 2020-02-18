**This is not an officially supported Google product. It is a reference implementation.**

# Alligator 2.0

This tool is a Python solution to showcase an integration between the Google My Business API and the Cloud Natural Language API. It downloads all the reviews for a Google My Business account and processes each review's content with the Cloud Natural Language API to generate a sentiment score for analysis.

## Client Library Installation

Install the required dependencies

`$ pip install --upgrade --quiet --requirement requirements.txt`

## Set up the Google Cloud Project

Follow the steps to **Enable the API** within the [basic setup guide](https://developers.google.com/my-business/content/basic-setup) and create the necessary OAuth2 Credentials required for the next steps.

Go to [Enable the Cloud Natural Language API](https://console.cloud.google.com/flows/enableapi?apiid=language.googleapis.com).

Go to [Enable the BigQuery API](https://console.cloud.google.com/flows/enableapi?apiid=bigquery).

Please note that BigQuery provides a [sandbox](https://cloud.google.com/bigquery/docs/sandbox) if you do not want to provide a credit card or enable billing for your project. The steps in this topic work for a project whether or not your project has billing enabled. If you optionally want to enable billing, see [Learn how to enable billing](https://cloud.google.com/billing/docs/how-to/modify-project).

## Install OAuth2 Credentials

Create a file named `client_secrets.json`, with the credentials downloaded as
JSON from your Google Cloud Project API Console.

## Download the discovery document

Go to the [Samples page](https://developers.google.com/my-business/samples/#discovery_document), right click **Download discovery document**, and select **Save Link As**. Then, save the file as `gmb_discovery.json` in the same directory.

## Run the solution

Execute the script to start the process of retrieving the reviews for all available locations from all accessible accounts for the authorized user:

`$ python main.py --project_id=<PROJECT_ID>`

The script generates a number of tables in an `alligator` BigQuery dataset.

## Useage of the CLI

Useage:

```
$ python main.py [-h] -p PROJECT_ID [-a ACCOUNT_ID] [-l LOCATION_ID]
                 [--no_insights] [--no_reviews] [--no_sentiment]
                 [--no_directions] [--no_hourly_calls] [--sentiment_only] [-v]
```

Optional arguments:

```
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

## Authors

- Tony Coconate (coconate@google.com) - Google
- Miguel Fernandes (miguelfc@google.com) – Google
- David Harcombe (davidharcombe@google.com) - Google
