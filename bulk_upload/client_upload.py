import os
from dotenv import load_dotenv
import csv
from google.cloud import firestore
from google.oauth2 import service_account
import google.auth.transport.requests


load_dotenv()
CLOUD_PROJECT = os.getenv('GCP_PROJECT')
DATABASE = os.getenv('FIREBASE_DB')
COLLECTION = 'all_clients'
SCOPES = ['https://www.googleapis.com/auth/cloud-platform']

def get_authenticated_session():

    creds = service_account.Credentials.from_service_account_file(
        '/Users/cbaca/Documents/data/gld-client-details/service-account-key.json',
        scopes=SCOPES
    )

    # credentials, project = default()
    
    return google.auth.transport.requests.AuthorizedSession(creds)


auth_session = get_authenticated_session()

db = firestore.Client(project=CLOUD_PROJECT, database=DATABASE)

def clean_data(row_dict):

    array_keys = [
                'facebookAds_accountID', 
                'facebookOrganic_accountID', 
                'freeWheel_advertiserID',
                'googleAds_customerID', 
                'googleAnalytics_propertyID', 
                'googleBusinessProfile_address', 
                'googleCampaignManager_accountID',
                'googleSearchConsole_siteURL',
                'instagramOrganic_accountID',
                'microsoftAds_accountID',
                'tiktokAds_accountID',
                'linkedinAds_accountID',
                'linkedinOrganic_accountID',
                'shopify_accountID'
            ]
    string_keys = [ 'client_id',
                'client_name', 
                'client_group',
                'client_type',
                'slack_webhook', 
                'email_internal', 
                'email_external', 
                'freeWheel_campaignNameLookup'
            ]
    float_keys = [
                'facebookAds_margin',
                'tiktokAds_margin',
                'freeWheel_margin',
                'googleAds_margin',
                'microsoftAds_margin'
            ]
    boolean_keys = [
                'status_paid', 
                'status_seo'
            ]

    cleaned = {}
    for key, value, in row_dict.items():
        if key in array_keys:
            final_array = []
            value_to_array = [addr.strip() for addr in value.split(';') if addr.strip()]
            for value in value_to_array:
                if value == 'None':
                    final_array.append(None)
                else: 
                    final_array.append(value)
            cleaned[key] = final_array
        elif key in string_keys:
            if value == 'None':
                cleaned[key] = None
            else:
                cleaned[key] = value
        elif key in float_keys:
            cleaned[key] = float(value)
        elif key in boolean_keys:
            if value == 'TRUE':
                cleaned[key] = True
            elif value == 'FALSE':
                cleaned[key] = False

    return cleaned

# def clean_data(row_dict):

#     ### Need to rework this function to explicitly set field types:
#     ### arrays for IDs/GBP, floats for margin, strings for all others

#     cleaned = {}
#     for key, value in row_dict.items():
#         if value == "" or value is None:
#             cleaned[key] = None
#         elif key == 'googleBusinessProfile_address':
#             cleaned[key] = [addr.strip() for addr in value.split(';') if addr.strip()]
#         else:
#             cleaned[key] = value
#     return cleaned

with open('/Users/cbaca/Documents/data/gld-client-details/bulk_upload/files/all_clients.csv', 'r', encoding='utf-8-sig') as file:
  csvFile = csv.DictReader(file)

  for lines in csvFile:
   # create the document with a custom ID
    clean_line = clean_data(lines)
    if clean_line['client_id']:
      doc_ref = db.collection(COLLECTION).document(clean_line['client_id'])      
      # set the fields for the given document
      doc_ref.set({
          'client_name' : clean_line['client_name'],
          'client_group' : clean_line['client_group'],
          'client_type' : clean_line['client_type'],
          'status_paid' : clean_line['status_paid'],
          'status_seo' : clean_line['status_seo'],
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
          'linkedinAds_accountID' : clean_line['linkedinAds_accountID'],
          'linkedinOrganic_accountID' : clean_line['linkedinOrganic_accountID'],
          'microsoftAds_accountID' : clean_line['microsoftAds_accountID'],
          'microsoftAds_margin' : clean_line['microsoftAds_margin'],
          'shopify_accountID' : clean_line['shopify_accountID'],
          'tiktokAds_accountID' : clean_line['tiktokAds_accountID'],
          'tiktokAds_margin' : clean_line['tiktokAds_margin']
      })
    else:
        pass
