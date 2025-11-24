#!/usr/bin/env python3
"""
2_fetch_data.py
Connects to Gmail, downloads raw thread data (JSON) and attachments to disk.
Does ZERO processing of the content (no parsing, no cleaning).
Designed to be idempotent (safe to run repeatedly).
"""

import os
import json
import base64
import time
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- Configuration ---
GMAIL_LABEL_NAME = "boet-efter-far"
DATA_DIR = "./data"
RAW_DIR = os.path.join(DATA_DIR, "raw")
ATTACHMENTS_DIR = os.path.join(DATA_DIR, "attachments")
TOKEN_FILE = 'token.json'
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def get_gmail_service():
    """Load credentials and return the Gmail service."""
    if not os.path.exists(TOKEN_FILE):
        print(f"Error: {TOKEN_FILE} not found. Run 1_authenticate.py first.")
        return None
    
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    return build('gmail', 'v1', credentials=creds)

def get_label_id(service, label_name):
    """Finds the ID for a given label name."""
    try:
        results = service.users().labels().list(userId='me').execute()
        labels = results.get('labels', [])
        for label in labels:
            if label['name'] == label_name:
                return label['id']
        return None
    except HttpError as error:
        print(f"An error occurred fetching labels: {error}")
        return None

def list_threads(service, label_id):
    """Lists all thread IDs associated with the label."""
    threads = []
    page_token = None
    print(f"Finding threads for label ID: {label_id}...")
    
    while True:
        try:
            results = service.users().threads().list(
                userId='me', 
                labelIds=[label_id], 
                pageToken=page_token
            ).execute()
            
            threads.extend(results.get('threads', []))
            page_token = results.get('nextPageToken')
            if not page_token:
                break
        except HttpError as error:
            print(f"An error occurred listing threads: {error}")
            break
            
    return threads

def save_attachment(service, message_id, attachment_id, filename, save_path):
    """Downloads and saves an attachment if it doesn't exist."""
    if os.path.exists(save_path):
        # print(f"    [Exists] Attachment {filename}")
        return

    try:
        att = service.users().messages().attachments().get(
            userId='me', messageId=message_id, id=attachment_id
        ).execute()
        
        data = att['data']
        file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))
        
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, 'wb') as f:
            f.write(file_data)
            
        print(f"    [Downloaded] {filename}")
        
    except HttpError as error:
        print(f"    [Error] Failed to download {filename}: {error}")

def decode_text_parts(parts):
    """
    Recursively decodes text/plain and text/html parts from base64url to UTF-8 strings.
    Modifies the parts list in-place.
    """
    if not parts:
        return

    for part in parts:
        mime_type = part.get('mimeType')
        body = part.get('body', {})
        data = body.get('data')
        
        if data and mime_type in ['text/plain', 'text/html']:
            try:
                # Add padding if needed
                missing_padding = len(data) % 4
                if missing_padding:
                    data += '=' * (4 - missing_padding)
                
                decoded_bytes = base64.urlsafe_b64decode(data)
                decoded_str = decoded_bytes.decode('utf-8')
                
                # Update the part with decoded data
                part['body']['data'] = decoded_str
            except Exception as e:
                # If decoding fails, leave as is (might be binary or already decoded)
                pass
        
        # Recurse
        if part.get('parts'):
            decode_text_parts(part['parts'])

def process_attachments(service, thread_id, messages):
    """Scans messages for attachments and triggers download."""
    for msg in messages:
        message_id = msg['id']
        payload = msg.get('payload', {})
        parts = payload.get('parts', [])
        
        # Recursively find parts (sometimes they are nested)
        queue = parts[:]
        while queue:
            part = queue.pop(0)
            if part.get('parts'):
                queue.extend(part['parts'])
            
            filename = part.get('filename')
            body = part.get('body', {})
            attachment_id = body.get('attachmentId')
            
            if filename and attachment_id:
                # Create a safe filename and path
                # Structure: data/attachments/{thread_id}/{message_id}_{filename}
                # Adding message_id prefix ensures unique filenames if same file sent twice
                safe_filename = "".join([c for c in filename if c.isalpha() or c.isdigit() or c in "._- "]).strip()
                save_path = os.path.join(ATTACHMENTS_DIR, thread_id, f"{message_id}_{safe_filename}")
                
                save_attachment(service, message_id, attachment_id, filename, save_path)

def main():
    # Ensure directories exist
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(ATTACHMENTS_DIR, exist_ok=True)

    service = get_gmail_service()
    if not service:
        return

    # 1. Get Label ID
    label_id = get_label_id(service, GMAIL_LABEL_NAME)
    if not label_id:
        print(f"Error: Could not find label '{GMAIL_LABEL_NAME}'")
        return
    print(f"Found label '{GMAIL_LABEL_NAME}' (ID: {label_id})")

    # 2. List Threads
    threads = list_threads(service, label_id)
    print(f"Found {len(threads)} threads.")

    # 3. Download Loop
    for idx, thread_info in enumerate(threads):
        thread_id = thread_info['id']
        raw_file_path = os.path.join(RAW_DIR, f"{thread_id}.json")
        
        print(f"[{idx+1}/{len(threads)}] Processing Thread {thread_id}...")

        # Check if we already have the thread JSON
        if os.path.exists(raw_file_path):
            print(f"  Skipping API fetch (local file exists). Checking attachments...")
            # Load local JSON to check for attachments anyway (in case download failed previously)
            with open(raw_file_path, 'r') as f:
                thread_data = json.load(f)
        else:
            # Fetch from API
            try:
                thread_data = service.users().threads().get(
                    userId='me', id=thread_id, format='full'
                ).execute()
                
                # Decode text parts for readability/debugging
                if 'messages' in thread_data:
                    for msg in thread_data['messages']:
                        payload = msg.get('payload', {})
                        # Handle root payload if it has body/data directly
                        if payload.get('body', {}).get('data'):
                             # Wrap in list to reuse function, though structure is usually payload -> parts
                             # But sometimes payload IS the part.
                             decode_text_parts([payload])
                        
                        if payload.get('parts'):
                            decode_text_parts(payload['parts'])

                # Save Raw JSON
                with open(raw_file_path, 'w') as f:
                    json.dump(thread_data, f, indent=2)
                print(f"  Saved raw JSON.")
                
            except HttpError as error:
                print(f"  Error fetching thread {thread_id}: {error}")
                continue

        # Process Attachments
        if 'messages' in thread_data:
            process_attachments(service, thread_id, thread_data['messages'])
        
    print("\n✓ Fetch complete.")

if __name__ == '__main__':
    main()

