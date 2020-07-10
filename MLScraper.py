import pandas as pd
import requests
import json
from dotenv import load_dotenv, find_dotenv
import os
from os.path import abspath, join, dirname
import pygsheets



def get_api_key():
  """
  Get the api key for website accessing.

  Table of key type and key value for privacy.

  Parameters
  ----------
  @api_key_id [string]: Key value in dataframe

  Returns
  -------
  [string]: API Key

  """
  # load api keys file
  dotenv_path = join(dirname(__file__), 'settings.env')
  load_dotenv(dotenv_path)


  # get api key
  api_key = os.getenv("api_key")
  # return api key
  return api_key




def api_property_list_for_sale(api_key, sqft_min, postal_code, prop_type, limit):
  # url for api
  url = "https://realtor.p.rapidapi.com/properties/v2/list-for-sale"

  # enter parameters
  querystring = {
    "sort":"price_low",
    "is_foreclosure": "false",
    "is_contingent": "false",
    "is_new_construction": "false",
    "is_new_plan": "false",
    "is_pending": "false",
    "sqft_min": sqft_min,
    "offset":"0",
    "limit":limit,
    "postal_code": postal_code,
    "prop_type":prop_type
  }

  # header
  headers = {
    'x-rapidapi-host': "realtor.p.rapidapi.com",
    'x-rapidapi-key': api_key
  }

  # response
  response = requests.request("GET", url, headers=headers, params=querystring)
  return response.json() # json format





  url = "https://realtor.p.rapidapi.com/properties/v2/detail"

  querystring = {"property_id":property_id}

  headers = {
    'x-rapidapi-host': "realtor.p.rapidapi.com",
    'x-rapidapi-key': api_key
    }

  response = requests.request("GET", url, headers=headers, params=querystring)

  return response.json() # json format




def process_list_for_sale_response(response_json):
    """
    Process the list for sale API response.

    Convert each listing to a dataframe, append to a list, and concatenate to one dataframe.

    Parameters
    ----------
    @response_json [dictionary]: API response for list for sale

    Returns
    -------
    [dataframe] Dataframe of all list for sale responses

    """

    # empty dataframe
    dataframe_list = []

    # iterate through each for sale listing
    for l in response_json['properties']:

        # convert each listing to dataframe
        _temp_df = pd.DataFrame.from_dict(l, orient='index').T

        # append to dataframe list for all listings
        dataframe_list.append(_temp_df)

    # concatenate all dataframes, for missing col values enter null value
    return pd.concat(dataframe_list, axis=0, ignore_index=True, sort=False)




#---------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------




realtor_api_key = get_api_key() # api key to access data
prop_type = "single_family"
sqft_min = "2000"
postal_code = "60647"
limit = 1



property_list_for_sale_response = api_property_list_for_sale(api_key=realtor_api_key, sqft_min= sqft_min, postal_code=postal_code,
                                                             prop_type=prop_type,
                                                             limit=limit)


df_properties_for_sale_raw = process_list_for_sale_response(response_json=property_list_for_sale_response)
df_properties_for_sale_raw.head(5)

client = pygsheets.authorize(service_file='Realtor-viz-data-b5a9fbcd94bf.json')
print("-----------------Authorized--------------------")

sheet = client.open('SubletInn MLScraper')
print("-----------------Sheet Opened------------------")

wks = sheet[0]
print("-----------------First Sheet Accessed----------")

wks.set_dataframe(df_properties_for_sale_raw.head(5),(1,1))
print("-----------------Data Updated------------------")