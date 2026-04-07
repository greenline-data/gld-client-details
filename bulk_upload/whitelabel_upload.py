import os
from dotenv import load_dotenv
import csv
from google.cloud import firestore

load_dotenv()
CLOUD_PROJECT = os.getenv('GCP_PROJECT')
DATABASE = os.getenv('FIREBASE_DB')
COLLECTION = 'white_label'

db = firestore.Client(project=CLOUD_PROJECT, database=DATABASE)

def clean_data(row_dict):
    cleaned = {}
    for key, value in row_dict.items():
        if value == "" or value is None:
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned

with open('/home/chris/gld-client-details/bulk_upload/files/white_label.csv', 'r', encoding='utf-8-sig') as file:
  csvFile = csv.DictReader(file)
  for lines in csvFile:
    clean_line = clean_data(lines)
    # create the document with a custom ID
    if clean_line['drive_client_name']:
      doc_ref = db.collection(COLLECTION).document(clean_line['drive_client_name'])
      # set the fields for the given document
      doc_ref.set({
          'client_group' : clean_line['client_group'],
          'client_name_historical' : clean_line['client_name_historical'],
          'status' : True,
          'freeWheel_advertiserID' : clean_line['freeWheel_advertiserID'],
          'freeWheel_campaignNameLookup' : clean_line['freeWheel_campaignNameLookup'],
          'facebookAds_accountID' : clean_line['facebookAds_accountID'],
          'googleAds_customerID' : clean_line['googleAds_customerID'],
          'googleAnalytics_propertyID' : clean_line['googleAnalytics_propertyID'],
          'googleBusinessProfile_address' : clean_line['googleBusinessProfile_address'],
          'googleSearchConsole_siteURL' : clean_line['googleSearchConsole_siteURL'],
          'microsoftAds_accountID' : clean_line['microsoftAds_accountID'],
          'shopify_accountID' : clean_line['shopify_accountID'],
          'tiktokAds_accountID' : clean_line['tiktokAds_accountID']
      })
    else:
      pass
