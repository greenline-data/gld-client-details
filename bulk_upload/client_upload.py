import os
from dotenv import load_dotenv
import csv
from google.cloud import firestore


load_dotenv()
CLOUD_PROJECT = os.getenv('GCP_PROJECT')
DATABASE = os.getenv('FIREBASE_DB')
COLLECTION = 'all_clients'

db = firestore.Client(project=CLOUD_PROJECT, database=DATABASE)

def clean_data(row_dict):
    cleaned = {}
    for key, value in row_dict.items():
        if value == "" or value is None:
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned

with open('~/Documents/data/gld-client-details/bulk_upload/files/all_clients.csv', 'r', encoding='utf-8-sig') as file:
  csvFile = csv.DictReader(file)

  for lines in csvFile:
   # create the document with a custom ID
    clean_line = clean_data(lines)
    if clean_line['drive_client_name']:
      doc_ref = db.collection(COLLECTION).document(clean_line['drive_client_name'])      
      # set the fields for the given document
      doc_ref.set({
          'client_name' : clean_line['client_name'],
          'client_group' : clean_line['client_group'],
          'client_type' : clean_line['client_type'],
          'status_paid' : True,
          'status_seo' : True,
          'slack_webhook' : clean_line['slack_webhook'],
          'email_internal' : clean_line['email_internal'],
          'email_external' : clean_line['email_external'],
          'facebookAds_accountID' : clean_line['facebookAds_accountID'],
          'facebookOrganic_accountID' : clean_line['facebookOrganic_accountID'],
          'facebookAds_margin' : clean_line['facebookAds_margin'],
          'freeWheel_advertiserID' : clean_line['freeWheel_advertiserID'],
          'freeWheel_campaignNameLookup' : clean_line['freeWheel_campaignNameLookup'],
          'freeWheel_margin' : clean_line['freeWheel_margin'],
          'googleAds_customerID' : clean_line['googleAds_customerID'],
          'googleAds_margin' : clean_line['googleAds_margin'],
          'googleAnalytics_propertyID' : clean_line['googleAnalytics_propertyID'],
          'googleBusinessProfile_address' : clean_line['googleBusinessProfile_address'],
          'googleCampaignManager_accountID' : clean_line['googleCampaignManager_accountID'],
          'googleSearchConsole_siteURL' : clean_line['googleSearchConsole_siteURL'],
          'instagramOrganic_accountID' : clean_line['instagramOrganic_accountID'],
          'linkedInAds_accountID' : clean_line['linkedInAds_accountID'],
          'linkedInOrganic_accountID' : clean_line['linkedInOrganic_accountID'],
          'microsoftAds_accountID' : clean_line['microsoftAds_accountID'],
          'microsoftAds_margin' : clean_line['microsoftAds_margin'],
          'shopify_accountID' : clean_line['shopify_accountID'],
          'tiktokAds_accountID' : clean_line['tiktokAds_accountID'],
          'tiktokAds_margin' : clean_line['tiktokAds_margin']
      })
    else:
        pass
