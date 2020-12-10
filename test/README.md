# Alligator 2.0 Data Filler

If you want to simulate data extracted from Google My Business (GMB)
without having an account associated with a store or group of stores, you can
use the data filler tool to mock the GMB API service, and generate fake data
for testing purposes.

This tool uses the faker python library, to generate the fake data about the
stores, reviews and insights. To make the geo results valid, you can use the
Geocoding API and get correct street addresses based on the random coordinates.
This last step is completely optional, but it makes the data feel a bit more
real.

## Instructions

1. Verify you have completed the installation of Alligator before proceeding.
 
   *Note: All steps assume you are located in the main alligator directory.*

2. Install the extra libraries needed for this tool.
   ```sh
   pip install -r test/requirements.txt
   ```

3. Edit the `api.py` file, and add this import statement:
   ```py
   from test.data_filler import DataFiller
   ```

4. In the same `api.py` file, comment the gmb_service assignment statement, and
   add a new one with the DataFiller service, as in this example:
   ```py
   # self.gmb_service = discovery.build_from_document(
   #     gmb_discovery_file.read(),
   #     base="https://www.googleapis.com/",
   #     http=http)
   self.gmb_service = DataFiller()
   ```

5. (Optional) If you want the geo results to be valid, you can use the 
  [Geocoding
   API](https://developers.google.com/maps/documentation/geocoding/overview)
   to extract street addresses from random latlng coordinates. If you want
   to use this API, make sure you set the `USE_GOOGLE_MAPS` variable to `True`,
   and add a valid API Key to the `GOOGLE_MAPS_API_KEY` variable, in the
   `test/data_filler.py` file.
   There are other global variables in the same file that you can tweak to
   change how the data is generated.

6. Execute the extraction as you would with the regular API object.
