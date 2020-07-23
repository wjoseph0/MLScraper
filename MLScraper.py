import pandas as pd
import numpy as np
import requests
import json
from dotenv import load_dotenv, find_dotenv
import os
from os.path import abspath, join, dirname
import pygsheets
from geopy.distance import great_circle
import time
import datetime
from datetime import date



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

  sheet = client.open('MLScraper ' + todaysDate)
  print("-----------------Sheet Opened------------------")

  fullList_wks = sheet[2]
  buyList_wks = sheet[3]
  compareList = sheet[4]
  print("-----------------All Tabs Accessed----------")

  fullList_wks.set_dataframe(df_properties_for_sale_raw,(1,1))
  buyList_wks.set_dataframe(df_buy_list,(1,1))
  compareList.set_dataframe(df_compare_list,(1,1))
  print("-----------------Data Updated------------------")

def post_to_opportunity_tab(spacer):
  client = pygsheets.authorize(service_file='Realtor-viz-data-b5a9fbcd94bf.json')
  print("-----------------Authorized--------------------")

  sheet = client.open('MLScraper ' + todaysDate)
  print("-----------------Sheet Opened------------------")

  opporList_wks = sheet[0]
  print("-----------------Opportunity Tab Accessed----------")

  opporList_wks.set_dataframe(df_opportunity_list,(spacer,1))
  print("-----------------Data Updated------------------")

def post_to_comparables_tab(group_spacing):
  client = pygsheets.authorize(service_file='Realtor-viz-data-b5a9fbcd94bf.json')
  print("-----------------Authorized--------------------")

  sheet = client.open('MLScraper ' + todaysDate)
  print("-----------------Sheet Opened------------------")

  analyzedList_wks = sheet[1]
  print("-----------------Analyzed Tab Accessed----------")

  analyzedList_wks.set_dataframe(df_analyzed_list,(group_spacing,1))
  print("-----------------Data Updated------------------")

def is_similar():
  def group_interior_sqft():
    if buyable_building_size - compared_building_size <= 500 and buyable_building_size - compared_building_size >= -500:
      return True

  def group_bed():
    if buyable_beds - compared_beds <= 1 and buyable_beds - compared_beds >= -1:
      return True

  def group_bath():
    if buyable_baths - compared_baths <= 1 and buyable_baths - compared_baths >= -1:
      return True

  def group_lot_sqft():
    if buyable_lot_size - compared_lot_size <= 500 and buyable_lot_size - compared_lot_size >= -500:
      return True

  def group_distance():
    if great_circle(buyable_address, compared_address).miles <= 1:
      return True

  if group_interior_sqft() and group_bed() and group_bath() and group_lot_sqft() and group_distance()== True:
    return True

def is_opportunity():
  if compared_price - buyable_price >= 10000:
    return True
#---------------------------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------------------------


now = datetime.datetime.now()
todaysDate = str(now.month) + '/' + str(now.day) + '/' + str(now.year)
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



#   POST INITIAL DATA TO SPREADSHEET 
post_to_sheets()




#   LOOP THROUGH BUY AND COMPARE GROUPS


group_spacing = 2
include_comparables = 1
spacer = 1

for buyable_row in df_buy_list.itertuples() :
  
  df_analyzed_list = pd.DataFrame()


  #GET buyable building size
  if 'dict' in str(type(buyable_row.building_size))  : #only compares if the field isn't blank
    for key, value in buyable_row.building_size.items() :
      if key == 'size':
        buyable_building_size = value # store building sqft in a variable
    
    if 'dict' in str(type(buyable_row.lot_size))  : #only compares if the field isn't blank
      for key, value in buyable_row.lot_size.items() :
        if key == 'size':
          buyable_lot_size = value # store lot sqft in a variable

      if 'dict' in str(type(buyable_row.address))  : #only compares if the field isn't blank
        for key, value in buyable_row.address.items() :
          if key == 'lat':
            buyable_lat = value # store latitude in a variable
          if key == 'lon':
            buyable_lon = value # store longitude in a variable

        buyable_address = (buyable_lat, buyable_lon)
        buyable_property = {'a_Property ID':buyable_row.property_id, 'b_Web URL':buyable_row.rdc_web_url, 'c_Price':buyable_row.price, 
          'd_Building SQFT':buyable_row.building_size, 'e_Lot SQFT':buyable_row.lot_size, 'f_Bedrooms':buyable_row.beds, 'g_Bathrooms':buyable_row.baths}
        df_analyzed_list = df_analyzed_list.append(buyable_property, ignore_index = True)
        buyable_beds = buyable_row.beds
        buyable_baths = buyable_row.baths
        buyable_price = buyable_row.price
      else:
        continue
    else:
      continue
  else:
    continue

  


  opportunity_upload = 0

  # FIND similar properties in compare group
  for row in df_compare_list.itertuples() :
    if 'dict' in str(type(row.building_size)) :
      for key, value in row.building_size.items() :
        if key == 'size':
          compared_building_size = value # store building sqft in a variable

      if 'dict' in str(type(row.lot_size)) :
        for key, value in row.lot_size.items() :
          if key == 'size':
            compared_lot_size = value # store lot sqft in a variable

        if 'dict' in str(type(row.address))  : #only compares if the field isn't blank
          for key, value in row.address.items() :
            if key == 'lat':
              compared_lat = value # store latitude in a variable
            if key == 'lon':
              compared_lon = value # store longitude in a variable
            
          compared_address = (compared_lat, compared_lon)
          compared_beds = row.beds
          compared_baths = row.baths
        else:
          continue
      else:
        continue
    else:
      continue


    if is_similar() == True:
      comparable_property = {'a_Property ID':row.property_id, 'b_Web URL':row.rdc_web_url, 'c_Price':row.price, 'd_Building SQFT':row.building_size, 
                              'e_Lot SQFT':row.lot_size, 'f_Bedrooms':row.beds, 'g_Bathrooms':row.baths} #data
      df_analyzed_list = df_analyzed_list.append(comparable_property, ignore_index = True) # append to the dataframe
      include_comparables = include_comparables + 1 # increase counter for indexing
      compared_price = row.price



      if is_opportunity() == True: #Push Opportunity listing to sheet if there is one found

        df_opportunity_list = pd.DataFrame()

        if opportunity_upload == 0:
          opportunity_property = {'a_Property ID':buyable_row.property_id, 'b_Web URL':buyable_row.rdc_web_url, 'c_Price':buyable_row.price, 
            'd_Building SQFT':buyable_row.building_size, 'e_Lot SQFT':buyable_row.lot_size, 'f_Bedrooms':buyable_row.beds, 'g_Bathrooms':buyable_row.baths} #data

          df_opportunity_list = df_opportunity_list.append(opportunity_property, ignore_index = True) # append to the dataframe

          post_to_opportunity_tab(spacer) # Push opportunity listing to the google sheet
 
          del df_opportunity_list # Clears dataframe for next loop

          opportunity_upload = opportunity_upload + 1
          spacer = spacer + 3
        else:
          continue
    else:
      continue

  


# Push grouped listings up to the google sheet and start the process over again
  post_to_comparables_tab(group_spacing)
  del df_analyzed_list
  group_spacing = 3 + include_comparables



#'property_id'	listing_id	rdc_web_url	prop_type	address	branding	prop_status	price	baths_half	baths_full	baths	beds	
# building_size	agents	office	last_update	client_display_flags	lead_forms	photo_count	thumbnail	page_no	rank	list_tracking	
# lot_size	mls	virtual_tour	open_houses