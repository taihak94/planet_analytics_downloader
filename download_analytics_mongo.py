import json
import os
import requests
import sys
import re
import pymongo
from dateutil import parser
from pymongo import MongoClient

# load custom filter from file
config = json.load(open(sys.argv[1]))

stats_endpoint_request = config["stats_endpoint_request"]

# Get the Planet API key from envitoment varible
planet_api_key = os.environ['PL_API_KEY']

# Setup db
client = MongoClient('mongodb://localhost:27017/')
db = client.planet_analytics

session = requests.Session()
session.auth = (planet_api_key, '')

count = 0

def compose_query(endpoints):
  limit = ("limit={}").format(endpoints["limit"])

  time_observed = endpoints["time"]["observed"]
  time_query = ""
  if time_observed:
    time_query = ("&properties.observed={}/{}").format(time_observed["start"], time_observed["end"])

  bbox = endpoints["bbox"]
  bbox_query = ""
  if bbox:
    bbox_query = ("&bbox={}").format(bbox)

  return ("{}{}{}").format(limit, time_query, bbox_query)  

def save_collection(features, collection):
  global count
  collection.create_index([('properties.observed', pymongo.ASCENDING)], unique=False)
  collection.create_index([('geometry.coordinates', pymongo.ASCENDING)], unique=False)
  collection.create_index([('properties.source_item', pymongo.ASCENDING)], unique=False)

  for feature in features:
    feature["properties"]["observed"] = parser.parse(feature["properties"]["observed"])
    feature["published"] = parser.parse(feature["published"])
    feature["updated"] = parser.parse(feature["updated"])
    collection.update({ "_id": feature["id"] }, feature, upsert = True)

  
  count += len(features)


query = compose_query(stats_endpoint_request)

def download_collection(collection_name, collection_title):
  global count
  collection_items_url = ("https://api.planet.com/wfs/v3/collections/{}/items?{}").format(collection_name, query)
  analytics_json = session.get(collection_items_url).json()
  collection = db[collection_title]
  
  while True:
    if analytics_json['links'] and len(analytics_json['links']) == 0 :
      break

    if count == analytics_json['numberMatched']:
      count = 0
      break
    
    save_collection(analytics_json["features"], collection)

    analytics_json = session.get(analytics_json["links"][0]['href']).json()

collections_res = session.get('https://api.planet.com/wfs/v3/collections').json()
collections_list = collections_res['collections']

for collection in collections_list:
  collection_name = collection['name']
  collection_title = (collection['title']).replace(" ", "_")

  download_collection(collection_name, collection_title)