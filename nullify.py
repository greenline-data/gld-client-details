# This script is used to nullify blank fields in Firestore. Should not be needed regularly, only if a bulk upload occurred with blank strings rather than null values.
# Written by Chris Baca
# Last updated March 2026

from google.cloud import firestore
import os
from dotenv import load_dotenv

load_dotenv()
PROJECT_ID = os.getenv('GCP_PROJECT')
FIREBASE_DB = os.getenv('FIREBASE_DB')

db = firestore.Client(project=PROJECT_ID, database=FIREBASE_DB)

def nullify_blank_strings(collection_name):
    print(f"Checking collection: {collection_name}...")
    docs = db.collection(collection_name).stream()
    
    count = 0
    for doc in docs:
        data = doc.to_dict()
        updates = {}
        
        # Check every field in the document
        for field, value in data.items():
            if value == "":
                # Setting to None creates a NULL value in Firestore
                updates[field] = None 
        
        # If we found blank strings, update the document
        if updates:
            doc.reference.update(updates)
            count += 1
            
    print(f"Finished {collection_name}. Updated {count} documents.")

# Run for your specific collections
collections = ['automotive', 'enterprise', 'white_label']
for col in collections:
    nullify_blank_strings(col)
