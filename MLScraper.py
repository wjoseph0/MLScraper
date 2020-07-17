import pandas as pd
import numpy as np
import requests
import json
from dotenv import load_dotenv, find_dotenv
import os
from os.path import abspath, join, dirname
import pygsheets




def get_google_api_key():
  # load api keys file
  dotenv_path = join(dirname(__file__), 'settings.env')
  load_dotenv(dotenv_path)
  # get api key
  google_api_key = os.getenv("google_api_key")
  # return api key
  return google_api_key

def get_realtor_api_key():
  # load api keys file
  dotenv_path = join(dirname(__file__), 'settings.env')
  load_dotenv(dotenv_path)
  # get api key
  realtor_api_key = os.getenv("realtor_api_key")
  # return api key
  return realtor_api_key

def api_property_list_for_sale(api_key, postal_code, prop_type, limit):
  # url for api
  url = "https://realtor.p.rapidapi.com/properties/v2/list-for-sale"

  # enter parameters
  querystring = {
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

def post_to_sheets():
  client = pygsheets.authorize(service_file='Realtor-viz-data-b5a9fbcd94bf.json')
  print("-----------------Authorized--------------------")

  sheet = client.open('SubletInn MLScraper')
  print("-----------------Sheet Opened------------------")

  #opporList_wks = sheet[0]
  #analyzedList_wks = sheet[1]
  fullList_wks = sheet[2]
  buyList_wks = sheet[3]
  compareList = sheet[4]
  print("-----------------All Tabs Accessed----------")

  #opporList_wks.set_dataframe(df_oppor_list,(1,1))
  #analyzedList_wks.set_dataframe(df_properties_for_sale_raw,(1,3))
  fullList_wks.set_dataframe(df_properties_for_sale_raw,(1,1))
  buyList_wks.set_dataframe(df_buy_list,(1,1))
  compareList.set_dataframe(df_compare_list,(1,1))
  print("-----------------Data Updated------------------")

def post_to_comparables_tab(group_spacing):
  client = pygsheets.authorize(service_file='Realtor-viz-data-b5a9fbcd94bf.json')
  print("-----------------Authorized--------------------")

  sheet = client.open('SubletInn MLScraper')
  print("-----------------Sheet Opened------------------")

  analyzedList_wks = sheet[1]
  print("-----------------Analyzed Tab Accessed----------")

  analyzedList_wks.set_dataframe(df_analyzed_list,(group_spacing,1))
  print("-----------------Data Updated------------------")




#---------------------------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------------------------




realtor_api_key = get_realtor_api_key() # api key to access data
prop_type = "single_family"
postal_code = "60647" #Searches Logan Square, Chicago
limit = 100000



#store full list in a variable
property_list_for_sale_response = api_property_list_for_sale(api_key=realtor_api_key, postal_code=postal_code, prop_type=prop_type, limit=limit)
#convert full list json into a dataframe
df_properties_for_sale_raw = process_list_for_sale_response(response_json=property_list_for_sale_response)


df_buy_list = pd.DataFrame() #empty dataframe to be used
df_buy_list = pd.concat([df_buy_list, df_properties_for_sale_raw[df_properties_for_sale_raw['price'] <= 300000]])

df_compare_list = pd.DataFrame()
df_compare_list = pd.concat([df_compare_list, df_properties_for_sale_raw[df_properties_for_sale_raw['price'] > 300000]])


print('Number of listings found: ', df_properties_for_sale_raw['property_id'].count())

# TODO: Create for loop to create comparable groups

group_spacing = 2
include_comparables = 1

for row in df_buy_list.itertuples() :
  df_analyzed_list = pd.DataFrame()

  buyable_property = {'Property ID':row.property_id, 'Web URL':row.rdc_web_url, 'Building SQFT':row.building_size}
  df_analyzed_list = df_analyzed_list.append(buyable_property, ignore_index=True)


  #get buyable building size
  if 'dict' in str(type(row.building_size))  : #only compares if the field isn't blank
    for key, value in row.building_size.items() :
      if key == 'size':
        buyable_building_size = value

  #get building size from each compare group row
  for row in df_compare_list.itertuples() :
    if 'dict' in str(type(row.building_size)) :
      for key, value in row.building_size.items() :
        if key == 'size':
          compared_building_size = value
      if buyable_building_size - compared_building_size <= 500 and buyable_building_size - compared_building_size >= -500:
        comparable_property = {'Property ID':row.property_id, 'Web URL':row.rdc_web_url, 'Building SQFT':row.building_size}
        df_analyzed_list = df_analyzed_list.append(comparable_property, ignore_index=True)
        include_comparables = include_comparables + 1
    else:
      continue    
      
 
  post_to_comparables_tab(group_spacing)
  del df_analyzed_list
  group_spacing = 2 + include_comparables



#'property_id'	listing_id	rdc_web_url	prop_type	address	branding	prop_status	price	baths_half	baths_full	baths	beds	building_size	agents	office	last_update	client_display_flags	lead_forms	photo_count	thumbnail	page_no	rank	list_tracking	lot_size	mls	virtual_tour	open_houses




post_to_sheets()