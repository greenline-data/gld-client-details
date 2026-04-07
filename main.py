# This script is a pipeline that moves the client records from Firestore to GSC and then to GBQ client mapping tables
# Written by Chris Baca
# Last updated March 2026

import os
from dotenv import load_dotenv
import datetime
import time 
import requests
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.cloud import bigquery, storage
from google.auth import default
import google.auth.transport.requests
import logging
import google.cloud.logging

# --- CONFIGURATION ---
load_dotenv()
PROJECT_ID = os.getenv('GCP_PROJECT')
BUCKET_NAME = os.getenv('BUCKET_NAME')
FIREBASE_DB = os.getenv('FIREBASE_DB')
COLLECTION_IDS = ['automotive', 'enterprise', 'white_label', 'shift_gm', 'shift_vw', 'shift_subaru']
POLL_INTERVAL_SECONDS = 30
MAX_POLLS = 120
BQ_DATASET_ID = os.getenv('BQ_DATASET_ID')
storage_client = storage.Client()
LOG_CLIENT = google.cloud.logging.Client()
LOG_CLIENT.setup_logging()
SCOPES = ['https://www.googleapis.com/auth/cloud-platform']

# ---------------------
# Global function to fetch the authenticated session
# ---------------------

def get_authenticated_session():

    creds = service_account.Credentials.from_service_account_file(
        'service-account-key.json',
        scopes=SCOPES
    )

    # credentials, project = default()
    
    return google.auth.transport.requests.AuthorizedSession(creds)


auth_session = get_authenticated_session()


# ---------------------
# Initiate export to GCS
# ---------------------

def start_firestore_export():
    """
    Triggers a managed export and waits for it to complete using direct HTTP polling.
    """
    date_prefix = datetime.date.today().isoformat()
    output_uri_prefix = f"gs://{BUCKET_NAME}/{date_prefix}"
    parent_path = f"projects/{PROJECT_ID}/databases/{FIREBASE_DB}"
    
    print(f"Starting Firestore export for collections: {', '.join(COLLECTION_IDS)}")
    print(f"Export destination prefix: {output_uri_prefix}")

    try:
        # 1. Cleanup step - delete today's date from GSC bucket if exists
        bucket = storage_client.bucket(BUCKET_NAME)
        folder_prefix = f"{date_prefix}/" 
        
        print(f"Checking for existing data at {folder_prefix}...")
        
        blobs_to_delete = list(bucket.list_blobs(prefix=folder_prefix))
        deleted_count = len(blobs_to_delete)
        
        if deleted_count > 0:
            bucket.delete_blobs([blob.name for blob in blobs_to_delete])
            print(f"Successfully deleted {deleted_count} objects from {folder_prefix}.")
        else:
            print(f"No existing data found at {folder_prefix}. Proceeding with export.")


        # 2. Initiate the export and get the operation ID
        
        service = build('firestore', 'v1')
        
        body = { 'outputUriPrefix': output_uri_prefix, 'collectionIds': COLLECTION_IDS }

        operation = service.projects().databases().exportDocuments(
            name=parent_path, 
            body=body
        ).execute()

        operation_name = operation.get('name')
        if not operation_name:
            print("ERROR: Export operation failed to return an operation name.")
            return None

        print(f"Export initiated. Operation Name: {operation_name}. Starting poll loop...")

        # 3. Polling step - checks to make sure the Firestore export is complete
        polling_url = f"https://firestore.googleapis.com/v1/{operation_name}"

        for i in range(MAX_POLLS):
            response = auth_session.get(polling_url)
            response.raise_for_status()
            op_status = response.json()
            
            # Check if the operation is done
            if op_status.get('done'):
                print("Export operation complete!")
                
                if op_status.get('error'):
                    print(f"EXPORT FAILED with error: {op_status['error']}")
                    return None
                
                return date_prefix 

            # If not done, wait and try again
            if i < MAX_POLLS - 1:
                print(f"Polling {i+1}/{MAX_POLLS}: Operation still running. Waiting {POLL_INTERVAL_SECONDS}s...")
                time.sleep(POLL_INTERVAL_SECONDS)
       
        logging.warning(f"ERROR: Export operation timed out after {MAX_POLLS} attempts.")
        print(f"ERROR: Export operation timed out after {MAX_POLLS} attempts.")
        return None

    except Exception as e:
        logging.warning(f"FATAL ERROR during export or polling: {e}")
        print(f"FATAL ERROR during export or polling: {e}")
        return None

# ---------------------
# Initialize client import into GBQ
# ---------------------

def start_bq_load(latest_folder_prefix):
    """
    Triggers a BigQuery load job for the specified Firestore export folder in GCS.
    """
    if not latest_folder_prefix:
        print("Error: GCS prefix is missing. Cannot load data.")
        return
    
    try:
        bq_client = bigquery.Client()
        
        print(f"Loading data from export folder: {latest_folder_prefix}")

        job_names = []
        for collection_id in COLLECTION_IDS:
            # Construct the source URI based on the observed export structure
            # Example: gs://BUCKET/DATE/all_namespaces/kind_COLLECTION_ID/all_namespaces_kind_COLLECTION_ID.export_metadata
            gcs_source_uri = f"gs://{BUCKET_NAME}/{latest_folder_prefix}/all_namespaces/kind_{collection_id}/all_namespaces_kind_{collection_id}.export_metadata"
            destination_table_id = f"{PROJECT_ID}.{BQ_DATASET_ID}.{collection_id}_data"

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.DATASTORE_BACKUP,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                ignore_unknown_values=True
            )

            load_job = bq_client.load_table_from_uri(
                gcs_source_uri,
                destination_table_id,
                job_config=job_config,
            )
            
            job_names.append(load_job.job_id)
            print(f"Started load job {load_job.job_id} for table {destination_table_id}")
        
        logging.info(f"Successfully initiated BigQuery load jobs: {', '.join(job_names)}")
        print(f"Successfully initiated BigQuery load jobs: {', '.join(job_names)}")

    except Exception as e:
        logging.warning(f"FATAL ERROR during BigQuery load initiation: {e}")
        print(f"FATAL ERROR during BigQuery load initiation: {e}")

def main():
    """
    Main execution block for the VM cron job.
    """
    print("--- Pipeline Start ---")
    
    # 1. Start the export and poll for completion
    gcs_date_folder = start_firestore_export()
    
    if gcs_date_folder:
        # 2. Run the load job only if the export was successful
        start_bq_load(gcs_date_folder)
        print("--- Pipeline Complete ---")
    else:
        print("--- Pipeline Failed due to Export Error/Timeout ---")
        # Exit with a non-zero code to signal cron failure
        exit(1)

if __name__ == '__main__':
    main()
