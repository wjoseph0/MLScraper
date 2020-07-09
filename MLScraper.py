import pandas as pd
import requests
import json
from dotenv import load_dotenv, find_dotenv
import os
from os.path import abspath, join, dirname




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

  # return api key if in dataset
  # get api key from id
  api_key = os.getenv("api_key")
  # return api key
  return api_key




def api_property_list_for_sale(api_key, city, state, prop_type, limit=200):
  # url for api
  url = "https://realtor.p.rapidapi.com/properties/v2/list-for-sale"

  # enter parameters
  querystring = {
    "sort":"relevance",
    "city":city,
    "offset":"0",
    "limit":limit,
    "state_code":state,
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


def api_property_details(api_key, property_id):
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

city = "Chicago"
state = "IL"
prop_type = "single-family"


property_list_for_sale_response = api_property_list_for_sale(api_key=realtor_api_key, 
                                                             city=city, 
                                                             state=state, 
                                                             prop_type=prop_type,
                                                             limit=1)


df_properties_for_sale_raw = process_list_for_sale_response(response_json=property_list_for_sale_response)
df_properties_for_sale_raw.head()
print(df_properties_for_sale_raw.head())
