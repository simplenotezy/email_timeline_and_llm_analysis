import json
import re
import os

INPUT_FILE = 'output/lawyer_case_timelines.json'
OUTPUT_LLM = 'output/transcript_llm.txt'
OUTPUT_HUMAN = 'output/transcript_human.txt'

# Common clutter patterns
# "Skip" means ignore this line but continue processing
SKIP_PATTERNS = [
    r"Denne e-mail er alene til brug for adressaten",
    r"This email is intended only for",
    r"The information contained in this email",
    r"Privileged/Confidential Information",
    r"Sent from my iPhone",
    r"Sendt fra min mobil",
    r"Sendt fra min iPhone",
    r"Sendt fra min iPad",
    r"Sent from my iPad",
    r"Sendt fra Outlook til iOS",
    r"Hent Outlook til iOS",
    r"Get Outlook for iOS",
]

# "Stop" means discard this line and EVERYTHING after it (assumes top-posting)
STOP_PATTERNS = [
    r"Begin forwarded message",
    r"Start på videresendt besked",
    r"-----Original Message-----",
    r"_________________________________________________"
]

HEADER_PATTERNS = [
    r"^(From|Fra):",
    r"^(To|Til):",
    r"^(Sent|Date|Dato):",
    r"^(Subject|Emne):",
    r"^(Cc):",
    r"^On .* wrote:$",
    r"^Den .* skrev:$"
]

def normalize_line(line):
    """Strips quoting chars and whitespace for comparison."""
    return re.sub(r'^[\s>]+', '', line).strip()

def is_line_in_previous(line, prev_body_lines_set):
    """Checks if a normalized line exists in the previous message's normalized lines."""
    norm = normalize_line(line)
    if len(norm) < 10: # Don't dedup short lines (e.g. "Tak", "Hej")
        return False
    return norm in prev_body_lines_set

def clean_text_general(text):
    """Basic cleanup of disclaimers and headers."""
    if not text:
        return []
    
    lines = text.split('\n')
    kept_lines = []
    
    for line in lines:
        line_stripped = line.strip()
        if not line_stripped:
            continue
            
        # Check Stop Patterns (Aggressive Cutoff)
        if any(re.search(p, line_stripped, re.IGNORECASE) for p in STOP_PATTERNS):
            break # Stop processing lines for this message
            
        # Skip quoted blocks explicitly marked with >
        if line_stripped.startswith('>'):
            continue
            
        # Check Skip Patterns
        if any(re.search(p, line_stripped, re.IGNORECASE) for p in SKIP_PATTERNS):
            continue
            
        # Check headers (Context that wasn't caught by Stop)
        if any(re.match(p, line_stripped, re.IGNORECASE) for p in HEADER_PATTERNS):
            continue
            
        kept_lines.append(line)
    
    return kept_lines

def generate_transcripts():
    if not os.path.exists(INPUT_FILE):
        print(f"Input file {INPUT_FILE} not found.")
        return

    with open(INPUT_FILE, 'r') as f:
        data = json.load(f)

    # Sort threads by the date of the first message (Chronological Thread Order)
    # Filter out threads with no messages first
    data = [t for t in data if t.get('messages')]
    data.sort(key=lambda t: t['messages'][0].get('date', ''))

    llm_output = []
    human_output = []

    for thread in data:
        subject = thread.get("subject", "No Subject")
        messages = thread.get("messages", [])
        
        # --- Header for this Thread ---
        llm_output.append(f"# THREAD: {subject}")
        human_output.append(f"==================================================")
        human_output.append(f"THREAD ID: {thread.get('id', 'N/A')}")
        human_output.append(f"SUBJECT: {subject}")
        human_output.append(f"==================================================")
        
        # Track previous message content for deduplication
        # We use a set of normalized lines for O(1) lookup
        prev_msg_lines_set = set()

        for msg in messages:
            # Metadata
            date_short = msg.get("date", "")[:10]
            full_date = msg.get("date", "")
            sender = msg.get("from", "Unknown")
            to_recipients = msg.get("to", "")
            msg_id = msg.get("id", "N/A")
            
            # Raw body processing
            raw_body = msg.get("body", "")
            clean_lines = clean_text_general(raw_body)
            
            # Deduplication against previous message
            final_lines = []
            current_msg_lines_set = set()
            
            for line in clean_lines:
                # For the current message set, we add everything (to be history for next msg)
                current_msg_lines_set.add(normalize_line(line))
                
                # For output, we check if it was in previous
                if not is_line_in_previous(line, prev_msg_lines_set):
                    final_lines.append(line)
            
            body_text = " ".join(final_lines)
            
            # Update history (pure email threads usually accumulate, so previous msg is enough)
            # However, sometimes people reply to older messages. 
            # For strict reduction, comparing against *immediate* predecessor is usually safe for "quoting".
            prev_msg_lines_set = current_msg_lines_set

            # Attachments
            atts = msg.get("attachments", [])
            att_filenames = [a.get("filename") for a in atts]
            
            # --- Write to LLM Format ---
            # Format: [Date] [Sender]: [Body] {Attachments}
            if body_text or att_filenames:
                llm_entry = f"[{date_short}] {sender}: {body_text}"
                if att_filenames:
                    llm_entry += f" <Attachments: {', '.join(att_filenames)}>"
                llm_output.append(llm_entry)

            # --- Write to Human Format ---
            human_output.append(f"MSG ID: {msg_id}")
            human_output.append(f"Date:   {full_date}")
            human_output.append(f"From:   {sender}")
            if to_recipients:
                human_output.append(f"To:     {to_recipients}")
            human_output.append("-" * 20)
            
            if body_text:
                human_output.append(body_text)
            else:
                human_output.append("(Empty body / All text was repeated content)")
                
            if atts:
                human_output.append("\nAttachments:")
                for a in atts:
                    human_output.append(f"  - {a.get('filename')} (Path: {a.get('path')})")
            
            human_output.append("\n")

        # Separator between threads
        llm_output.append("\n") 
        human_output.append("\n\n")

    # Save Files
    with open(OUTPUT_LLM, 'w', encoding='utf-8') as f:
        f.write("\n".join(llm_output))
        
    with open(OUTPUT_HUMAN, 'w', encoding='utf-8') as f:
        f.write("\n".join(human_output))

    # Stats
    llm_size = os.path.getsize(OUTPUT_LLM)
    human_size = os.path.getsize(OUTPUT_HUMAN)
    
    print(f"Generated LLM Transcript: {llm_size / 1024:.2f} KB")
    print(f"Generated Human Transcript: {human_size / 1024:.2f} KB")

if __name__ == "__main__":
    generate_transcripts()

