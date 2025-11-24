import json
import re
import os
import csv

INPUT_FILE = 'output/lawyer_case_timelines.json'
OUTPUT_JSON = 'output/lawyer_case_timelines_minimized.json'
OUTPUT_CSV = 'output/lawyer_case_timelines_minimized.csv'

# Common disclaimer starts (Danish/English)
DISCLAIMER_PATTERNS = [
    r"Denne e-mail er alene til brug for adressaten",
    r"This email is intended only for",
    r"The information contained in this email",
    r"Privileged/Confidential Information",
    r"_________________________________________________",
    r"Sent from my iPhone",
    r"Sendt fra min mobil",
    r"Sendt fra min iPhone",
    r"Begin forwarded message",
    r"Start på videresendt besked"
]

HEADER_PATTERNS = [
    r"^(From|Fra):",
    r"^(To|Til):",
    r"^(Sent|Date|Dato):",
    r"^(Subject|Emne):",
    r"^(Cc):"
]

def clean_body(text):
    if not text:
        return ""
    
    lines = text.split('\n')
    cleaned_lines = []
    
    for line in lines:
        line = line.strip()
        
        # Skip empty lines
        if not line:
            continue
            
        # Skip quoted text remnants that might have survived
        if line.startswith('>'):
            continue
            
        # Check for disclaimers
        is_disclaimer = False
        for pattern in DISCLAIMER_PATTERNS:
            if re.search(pattern, line, re.IGNORECASE):
                is_disclaimer = True
                break
        if is_disclaimer:
            continue
            
        # Check for header lines (often in forwards)
        is_header = False
        for pattern in HEADER_PATTERNS:
            if re.match(pattern, line, re.IGNORECASE):
                is_header = True
                break
        if is_header:
            continue
            
        # Skip "On ... wrote:" lines
        if re.match(r'^On .* wrote:$', line) or re.match(r'^Den .* skrev:$', line):
            continue
            
        cleaned_lines.append(line)
    
    return " ".join(cleaned_lines)

def minimize_json():
    if not os.path.exists(INPUT_FILE):
        print(f"Input file {INPUT_FILE} not found.")
        return

    with open(INPUT_FILE, 'r') as f:
        data = json.load(f)
    
    minimized_data = []
    csv_rows = []
    
    for thread in data:
        subject = thread.get("subject", "No Subject")
        
        min_thread = {
            "subject": subject,
            "messages": []
        }
        
        for msg in thread.get("messages", []):
            body_clean = clean_body(msg.get("body", ""))
            date_short = msg.get("date", "")[:10]
            sender = msg.get("from", "")
            
            min_msg = {
                "date": date_short,
                "from": sender,
                "body": body_clean
            }
            
            # Optional: Include 'to' if relevant
            if msg.get("to"):
                 min_msg["to"] = msg.get("to")
            
            # Simplify attachments
            att_filenames = []
            if "attachments" in msg:
                att_filenames = [a.get("filename") for a in msg["attachments"]]
                if att_filenames:
                    min_msg["attachments"] = att_filenames
            
            # Only add to JSON if body is not empty or has attachments
            if min_msg["body"] or "attachments" in min_msg:
                min_thread["messages"].append(min_msg)
            
            # Add to CSV rows (Flattened)
            # Limit body length for CSV to keep it manageable but useful
            csv_rows.append({
                "date": date_short,
                "from": sender,
                "subject": subject,
                "body": body_clean[:1000], # Truncate to 1000 chars
                "attachments": "; ".join(att_filenames)
            })
        
        if min_thread["messages"]:
            minimized_data.append(min_thread)
            
    # Write JSON
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(minimized_data, f, indent=None, ensure_ascii=False, separators=(',', ':'))
        
    # Write CSV
    if csv_rows:
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["date", "from", "subject", "body", "attachments"])
            writer.writeheader()
            writer.writerows(csv_rows)
    
    # Calculate stats
    orig_size = os.path.getsize(INPUT_FILE)
    json_size = os.path.getsize(OUTPUT_JSON)
    csv_size = os.path.getsize(OUTPUT_CSV) if os.path.exists(OUTPUT_CSV) else 0
    
    print(f"Original JSON: {orig_size / 1024:.2f} KB")
    print(f"Minimized JSON: {json_size / 1024:.2f} KB (Reduction: {(1 - json_size / orig_size) * 100:.1f}%)")
    print(f"Minimized CSV:  {csv_size / 1024:.2f} KB (Reduction: {(1 - csv_size / orig_size) * 100:.1f}%)")

if __name__ == "__main__":
    minimize_json()
