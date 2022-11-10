# Alligator 2.0

**This is not an officially supported Google product. It is a reference implementation.**

This tool is a Python-based solution that aggregates _Insights_ data from the Google My Business API and stores it into Google Cloud Platform, precisely in BigQuery. _Insights_ data provides details around how users interact with Google My Business listings via Google Maps, such as the number of queries for a location, locations of where people searched for directions, number of website clicks, calls, and reviews. The tool provides a cross-account look at the data instead of a per-location view.

Along with gathering stats, the Google Cloud Natural Language API is used to provide sentiment analysis and entity detection for supported languages. This is a fully automated process that pulls data from the Google My Business API and places it into a BigQuery instance, processing each review's content with the Natural Language API to generate a sentiment score for analysis. Furthermore, reviews can be classified by 'topics' to help surface areas of improvement across different locations.

## Google My Business Account Prerequisites

* All locations must roll up to a `Location Group` (formerly known as `Business Account`). Click [here](https://support.google.com/business/answer/6085339?ref_topic=6085325) for more information. Multiple location groups are supported and can be queried accordingly (refer to the samples inside the [sql](sql/) directory.
* All locations must be _verified_.

## Installation and Setup

Follow the steps below, or alternatively open the [alligator2.ipynb](alligator2.ipynb) notebook using [Google Colaboratory](https://colab.research.google.com) (preferred for better formatting) or Jupyter for a more interactive installation experience. The notebook contains additional information on maintenance and reporting, and will help you better visualize the data that will get imported into BigQuery after running the solution.

> Unlike traditional notebooks, the `alligator2.ipynb` notebook references the code in this GitHub repository rather than hosting its own version of the code.

### Install Python Libraries

Install the required dependencies:

    $ pip install --requirement requirements.txt

### Google Cloud Platform Project Setup

Follow the steps to **Enable the API** within the [Google My Business basic setup guide](https://developers.google.com/my-business/content/basic-setup) and create the necessary OAuth2 Credentials required for the next steps.

Go to [Enable the Cloud Natural Language API](https://console.cloud.google.com/flows/enableapi?apiid=language.googleapis.com).

Go to [Enable the BigQuery API](https://console.cloud.google.com/flows/enableapi?apiid=bigquery).

Please note that BigQuery provides a [sandbox](https://cloud.google.com/bigquery/docs/sandbox) if you do not want to provide a credit card or enable billing for your project. The steps in this topic work for a project whether or not your project has billing enabled. If you optionally want to enable billing, see [Learn how to enable billing](https://cloud.google.com/billing/docs/how-to/modify-project).

### Install OAuth2 Credentials

Create a file named `client_secrets.json`, with the credentials downloaded as JSON from your Google Cloud Platform Project API Console.

### Download the Legacy Google My Business API Discovery Document

Go to the [Samples page](https://developers.google.com/my-business/samples/previousVersions#discovery_document), right click **Download discovery document**, and select **Save Link As**. Then, save the file as `gmb_discovery.json` in the same directory.

### Run the Solution

Execute the script to start the process of retrieving the reviews for all available locations from all accessible accounts for the authorized user:

    $ python main.py --project_id=<PROJECT_ID>

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
--language LANG
                      the ISO-639-1 language code in which the Google My Business
                      reviews are written (used for sentiment processing). See
                      https://cloud.google.com/natural-language/docs/languages
                      for a list of supported languages
--no_insights         skip the insights processing and storage
--no_reviews          skip the reviews processing and storage
--no_directions       skip the directions processing and storage
--no_hourly_calls     skip the hourly calls processing and storage
--no_sentiment        skip the sentiment processing and storage
--no_topic_clustering skip the extraction of topics for each review
--sentiment_only      only process and store the sentiment of all available
                      reviews since the last run (if --no-sentiment is
                      provided, no action is performed)
-q, --quiet           only show warning and error messages (overrides --verbose)
-v, --verbose         increase output verbosity
```

## Notes

For the initial data load into BigQuery, a maximum of 18 months of insights data will be retrieved, up to 5 days prior to the current date. This is due to the posted 3-5 day delay on the data becoming available in the Google My Business API. For _phone calls_ and _driving directions_, only data from the last 7 days is retrieved. Finally, data is inserted into BigQuery with a batch size of 5000 to avoid running into API limits, especially when using the BigQuery Sandbox. These defaults are defined in [api.py](api.py) and can be tuned according to indiviual needs.

Furthermore, _all_ available reviews in BigQuery will be used _only_ for the first run of the sentiment analysis. Once the analysis is complete, an empty file named `sentiments_lastrun` will be created in the application's root directory, and this file's modification timestamp will be used for subsequent sentiment analysis runs so that only non-analyzed reviews are taken into consideration. Delete the file to rerun the analysis on all available reviews.

In terms of language processing, you can use the `--language` CLI flag to set the desired language that the Cloud Natural Language API should use for the sentiment analysis. This is particularly useful for reviews which may contain multiple languages. Refer to [this post](https://cloud.google.com/natural-language/docs/languages) for a list of languages supported by the API. You might need to deactivate one or more of the text annotation [features](https://cloud.google.com/natural-language/docs/reference/rest/v1/documents/annotateText#Features) in [api.py](api.py) accordingly if your language is not yet supported.

Finally, using the topic extraction feature requires the sentiment analysis to be enabled (i.e., you can't run the topic extraction with the --no_sentiment flag). This particular use case will generate a file named `cluster_labels.txt` with a list of recommended topics based on word repetition in the reviews dataset. You can fine tune this list and add your own terms. If this file exists, it will be read by the tool and used as a list of topics to cluster reviews in, otherwise, the file will be recreated and the process will use the most frequent list of nouns.

## Authors

* Tony Coconate (coconate@google.com) – Google
* Miguel Fernandes (miguelfc@google.com) – Google
* Mohab Fekry (mfekry@google.com) – Google
* Darragh Kelly (dfkelly@google.com) - Google
* David Harcombe (davidharcombe@google.com) – Google
