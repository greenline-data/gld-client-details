import os
from dotenv import load_dotenv
import csv
from google.cloud import firestore

load_dotenv()
CLOUD_PROJECT = os.getenv('GCP_PROJECT')
DATABASE = os.getenv('FIREBASE_DB')
COLLECTION = 'enterprise'

db = firestore.Client(project=CLOUD_PROJECT, database=DATABASE)

def clean_data(row_dict):
    cleaned = {}
    for key, value in row_dict.items():
        if value == "" or value is None:
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned

with open('/home/chris/gld-client-details/bulk_upload/files/enterprise.csv', 'r', encoding='utf-8-sig') as file:
  csvFile = csv.DictReader(file)
  for lines in csvFile:
    clean_line = clean_data(lines)
    # create the document with a custom ID
    if clean_line['drive_client_name']:
      # Sanitize the document ID by replacing '/' with '-'
      doc_id = clean_line['drive_client_name'].replace('/', '-')
      doc_ref = db.collection(COLLECTION).document(doc_id)
      # set the fields for the given document
      doc_ref.set({
          'client_group' : clean_line['client_group'],
          'client_name_historical' : clean_line['client_name_historical'],
          'status' : True,
          'freeWheel_advertiserID' : clean_line['freeWheel_advertiserID'],
          'facebookAds_accountID' : clean_line['facebookAds_accountID'],
          'facebookOrganic_accountID' : clean_line['facebookOrganic_accountID'],
          'googleAds_customerID' : clean_line['googleAds_customerID'],
          'googleAnalytics_propertyID' : clean_line['googleAnalytics_propertyID'],
          'googleCampaignManager_accountID' : clean_line['googleCampaignManager_accountID'],
          'googleBusinessProfile_address' : clean_line['googleBusinessProfile_address'],
          'googleSearchConsole_siteURL' : clean_line['googleSearchConsole_siteURL'],
          'instagramOrganic_accountID' : clean_line['instagramOrganic_accountID'],
          'linkedinAds_accountID' : clean_line['linkedinAds_accountID'],
          'linkedinOrganizations_accountID' : clean_line['linkedinOrganizations_accountID'],
          'microsoftAds_accountID' : clean_line['microsoftAds_accountID'],
          'shopify_accountID' : clean_line['shopify_accountID'],
          'tiktokAds_accountID' : clean_line['tiktokAds_accountID']
      })
    else:
      pass
