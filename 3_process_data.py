#!/usr/bin/env python3
"""
3_process_data.py
Reads local JSON/Attachment data, cleans text, extracts PDF content,
and generates final reports (JSON/CSV).
Optimized for AI Context: Minimal token usage.
Uses mail-parser-reply (custom fork) for superior multi-language cleaning.
"""

import os
import json
import csv
import re
import glob
import base64
from datetime import datetime
from dateutil import parser as date_parser

# 3rd Party Libraries
try:
    from bs4 import BeautifulSoup
    from mailparser_reply import EmailReplyParser
    from pypdf import PdfReader
    import pytesseract
    from pdf2image import convert_from_path
except ImportError as e:
    print(f"Error importing libraries: {e}")
    print("Please run: pip install -r requirements.txt")
    exit(1)

# --- Configuration ---
DATA_DIR = "./data"
RAW_DIR = os.path.join(DATA_DIR, "raw")
ATTACHMENTS_DIR = os.path.join(DATA_DIR, "attachments")
OUTPUT_DIR = "./output"

# --- Regex Patterns for Cleaning (Supplemental to Library) ---
EMAIL_PATTERN = re.compile(r'<([^>]+)>')

def ensure_output_dir():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def extract_pure_email(header_value):
    if not header_value:
        return None
    emails = []
    parts = header_value.split(',')
    for part in parts:
        match = EMAIL_PATTERN.search(part)
        if match:
            emails.append(match.group(1).strip())
        else:
            clean = part.strip()
            if '@' in clean:
                emails.append(clean)
    if not emails:
        return None
    return ", ".join(emails)

def extract_text_from_pdf(pdf_path):
    """
    Extracts text from a PDF file.
    Tries native extraction first. If that fails (scanned doc), falls back to OCR.
    Returns (text, was_ocr_performed)
    """
    text_content = []
    try:
        # 1. Attempt Native Extraction
        reader = PdfReader(pdf_path)
        for page in reader.pages:
            extracted = page.extract_text()
            if extracted:
                text_content.append(extracted)
        
        full_text = "\n".join(text_content).strip()
        
        # 2. Check for "Image Only" Suspicion (OCR Trigger)
        # If we have pages but almost no text, it's likely a scan.
        is_image_only = False
        if len(reader.pages) > 0 and len(full_text) < 100:
            is_image_only = True
            
        if is_image_only:
            print(f"    [OCR] Detected scanned PDF ({len(full_text)} chars). Starting OCR on: {os.path.basename(pdf_path)}...")
            try:
                # Convert PDF to images (requires poppler)
                images = convert_from_path(pdf_path)
                ocr_text = []
                for i, image in enumerate(images):
                    # Extract text from image (requires tesseract)
                    # 'dan+eng' tries both Danish and English
                    page_text = pytesseract.image_to_string(image, lang='dan+eng')
                    ocr_text.append(page_text)
                
                full_text = "\n".join(ocr_text).strip()
                print(f"    [OCR] Completed. Extracted {len(full_text)} chars.")
                return full_text, True
                
            except Exception as ocr_e:
                print(f"    [OCR] Failed: {ocr_e}. Falling back to empty/native text.")
                return full_text, False # Return whatever native text we found
            
        return full_text, False
    except Exception as e:
        print(f"Warning: Failed to parse PDF {pdf_path}: {e}")
        return "", False

def clean_email_body(payload):
    body_text = ""
    html_text = ""
    
    def get_parts(parts):
        nonlocal body_text, html_text
        for part in parts:
            mime_type = part.get('mimeType')
            data = part.get('body', {}).get('data')
            nested_parts = part.get('parts')
            
            if nested_parts:
                get_parts(nested_parts)
                continue
            if not data:
                continue
            
            # Heuristic: If data contains spaces/newlines, it's likely already decoded (Base64 doesn't have spaces)
            if ' ' in data or '\n' in data or '\r' in data:
                decoded = data
            else:
                try:
                    decoded = base64_decode(data)
                except:
                    # Fallback to raw data if decoding fails
                    decoded = data

            if mime_type == 'text/plain':
                body_text += decoded
            elif mime_type == 'text/html':
                html_text += decoded

    def base64_decode(data):
        missing_padding = len(data) % 4
        if missing_padding:
            data += '=' * (4 - missing_padding)
        return base64.urlsafe_b64decode(data).decode('utf-8', errors='replace')

    if 'parts' in payload:
        get_parts(payload['parts'])
    else:
        mime_type = payload.get('mimeType')
        data = payload.get('body', {}).get('data')
        if data:
            if ' ' in data or '\n' in data or '\r' in data:
                decoded = data
            else:
                try:
                    decoded = base64_decode(data)
                except:
                    decoded = data

            if mime_type == 'text/plain':
                body_text = decoded
            elif mime_type == 'text/html':
                html_text = decoded

    final_raw = ""
    if body_text.strip():
        final_raw = body_text
    elif html_text.strip():
        soup = BeautifulSoup(html_text, 'html.parser')
        final_raw = soup.get_text(separator='\n')
    
    if final_raw:
        # Use .parse_reply() to get the latest reply text directly
        # We rely on the library's built-in multi-language support now
        try:
            parsed_text = EmailReplyParser(languages=['da', 'en', 'no', 'sv']).parse_reply(text=final_raw)
        except:
            # Fallback
            parsed_text = EmailReplyParser(languages=['en']).parse_reply(text=final_raw)

        return parsed_text.strip(), final_raw
    
    return "", ""

def process_thread(thread_file):
    with open(thread_file, 'r') as f:
        data = json.load(f)
        
    thread_id = data['id']
    messages_raw = data.get('messages', [])
    messages_raw.sort(key=lambda x: int(x['internalDate']))
    
    timeline_messages = []
    thread_subject = "No Subject"
    
    for idx, msg in enumerate(messages_raw):
        msg_id = msg['id']
        headers = {h['name']: h['value'] for h in msg['payload']['headers']}
        
        this_subject = headers.get('Subject', '')
        if this_subject and (thread_subject == "No Subject" or thread_subject.startswith("Re:")):
             thread_subject = this_subject
             
        date_str = headers.get('Date', '')
        try:
            dt = date_parser.parse(date_str)
            date_iso = dt.isoformat()
        except:
            date_iso = date_str

        # --- Body Cleaning & Empty Check ---
        body_clean, body_raw = clean_email_body(msg['payload'])
        
        # --- Fix for Over-Aggressive Forward Cleaning ---
        # Heuristic: If it looks like a forward but the cleaner stripped almost everything, revert to raw.
        # We want to keep the content of forwarded emails, but strip history from replies.
        is_fwd_subject = re.match(r'^(Fwd|Vs|Videresend|Tr):', this_subject, re.IGNORECASE)
        is_reply_subject = re.match(r'^(Re|Sv):', this_subject, re.IGNORECASE)
        
        fwd_indicators = ["begin forwarded message", "videresendt besked", "oprindelig meddelelse", "forwarded message"]
        has_fwd_marker = any(ind in body_clean.lower() for ind in fwd_indicators)

        # If it is a Forward (and not a reply) OR explicitly has a forward marker in the stub
        if (is_fwd_subject and not is_reply_subject) or has_fwd_marker:
             # If the cleaned body is very short (< 300 chars) and we have significant raw content (> 300 chars)
             if len(body_clean) < 300 and len(body_raw) > 300:
                 # print(f"  [INFO] Restoring raw body for Forward/Stub {msg_id} (Clean: {len(body_clean)} vs Raw: {len(body_raw)})")
                 body_clean = body_raw
        
        # --- Attachments & Sidecar Text Files ---
        attachments_info = []
        search_pattern = os.path.join(ATTACHMENTS_DIR, thread_id, f"{msg_id}_*")
        found_files = glob.glob(search_pattern)
        
        for file_path in found_files:
            # Use a _to_text.txt suffix to avoid duplication
            if file_path.endswith('_to_text.txt'):
                continue

            filename = os.path.basename(file_path).split('_', 1)[1]
            
            has_text_file = False
            is_image_pdf = False
            
            if file_path.lower().endswith('.pdf'):
                text, was_ocr_performed = extract_text_from_pdf(file_path)
                
                if text.strip():
                    txt_path = file_path + "_to_text.txt"
                    with open(txt_path, 'w', encoding='utf-8') as f:
                        f.write(text)
                    has_text_file = True

                if was_ocr_performed:
                     is_image_pdf = True

            att_obj = {
                "filename": filename,
                "path": file_path
            }
            if has_text_file:
                att_obj["has_text_file"] = True
                att_obj["text_file_path"] = file_path + "_to_text.txt"
            
            if is_image_pdf:
                att_obj["is_image_pdf"] = True
                
            attachments_info.append(att_obj)

        # Debugging Empty Bodies
        if not body_clean.strip():
            if attachments_info:
                body_clean = "[Attachment Only]"
            elif body_raw.strip():
                # It was not empty before cleaning, but is empty now.
                # This usually means the regex stripped everything (maybe it was ONLY a quote?)
                print(f"  [WARN] Msg {msg_id} body became EMPTY after cleaning. Original len: {len(body_raw)}")

        # --- Build Message Object ---
        msg_obj = {
            "id": msg_id,
            "date": date_iso,
            "from": extract_pure_email(headers.get('From', '')),
            "body": body_clean
        }
        
        to_emails = extract_pure_email(headers.get('To', ''))
        if to_emails: msg_obj["to"] = to_emails
            
        cc_emails = extract_pure_email(headers.get('Cc', ''))
        if cc_emails: msg_obj["cc"] = cc_emails
            
        curr_subj_clean = re.sub(r'^(Re|Fwd|SV|VS):\s*', '', this_subject, flags=re.IGNORECASE).strip()
        thread_subj_clean = re.sub(r'^(Re|Fwd|SV|VS):\s*', '', thread_subject, flags=re.IGNORECASE).strip()
        if curr_subj_clean and curr_subj_clean.lower() != thread_subj_clean.lower():
             msg_obj["subject"] = this_subject

        if attachments_info:
            msg_obj["attachments"] = attachments_info
            
        timeline_messages.append(msg_obj)

    return {
        "id": thread_id,
        "subject": thread_subject,
        "messages": timeline_messages
    }

def main():
    ensure_output_dir()
    
    thread_files = glob.glob(os.path.join(RAW_DIR, "*.json"))
    print(f"Found {len(thread_files)} threads to process.")
    
    all_timelines = []
    csv_rows = []
    
    for idx, thread_file in enumerate(thread_files):
        timeline = process_thread(thread_file)
        all_timelines.append(timeline)
        
        for msg in timeline['messages']:
            att_names = [a['filename'] for a in msg.get('attachments', [])]
            
            csv_rows.append({
                "thread_id": timeline['id'],
                "message_id": msg['id'],
                "thread_subject": timeline['subject'],
                "date": msg['date'],
                "from": msg['from'],
                "to": msg.get('to', ''),
                "body_snippet": msg['body'][:100].replace('\n', ' ') + "...", 
                "attachment_filenames": ", ".join(att_names),
            })

    # Save JSON
    json_path = os.path.join(OUTPUT_DIR, "lawyer_case_timelines.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(all_timelines, f, indent=2, ensure_ascii=False)
    print(f"✓ Saved JSON: {json_path}")

    # Save CSV
    csv_path = os.path.join(OUTPUT_DIR, "messages_timeline.csv")
    if csv_rows:
        keys = csv_rows[0].keys()
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(csv_rows)
    print(f"✓ Saved CSV: {csv_path}")

if __name__ == '__main__':
    main()
