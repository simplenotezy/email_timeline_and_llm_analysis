import json
import re
import os
import hashlib
import shutil

INPUT_FILE = 'output/lawyer_case_timelines.json'
OUTPUT_LLM = 'output/transcript_llm.txt'
OUTPUT_HUMAN = 'output/transcript_human.txt'
OUTPUT_ATT_DIR = 'output/transcript_attachments'

CONFIG_DIR = 'config'
ALIASES_FILE = os.path.join(CONFIG_DIR, 'aliases.txt')
IGNORED_ATT_FILE = os.path.join(CONFIG_DIR, 'ignored_attachments.txt')
IGNORED_MSG_FILE = os.path.join(CONFIG_DIR, 'ignored_messages.txt')
IGNORED_TEXT_BLOCKS_FILE = os.path.join(CONFIG_DIR, 'ignored_text_blocks.txt')

# --- Configuration Loaders ---

def load_aliases():
    aliases = {}
    if os.path.exists(ALIASES_FILE):
        with open(ALIASES_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'): continue
                parts = line.split(':', 1)
                if len(parts) == 2:
                    aliases[parts[0].strip().lower()] = parts[1].strip()
    return aliases

def load_ignored_set(filepath):
    ignored = set()
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'): continue
                ignored.add(line)
    return ignored

## TODO: Should accept the WHOLE text block, (e.g. see mistake in MSG 192f81b0d0b44970)
def load_ignored_text_blocks():
    """Loads multi-line text blocks to ignore, storing them normalized."""
    blocks = []
    if os.path.exists(IGNORED_TEXT_BLOCKS_FILE):
        with open(IGNORED_TEXT_BLOCKS_FILE, 'r', encoding='utf-8') as f:
            content = f.read()
            # Split by double newlines to separate blocks
            raw_blocks = content.split('\n\n')
            for block in raw_blocks:
                if block.strip():
                    # Store normalized version (no newlines, single spaces)
                    blocks.append(normalize_text_blob(block))
    return blocks

def normalize_text_blob(text):
    """Aggressively normalizes text: lower case, no newlines, single spaces."""
    # Replace all whitespace chars (newlines, tabs) with single space
    return re.sub(r'\s+', ' ', text).strip().lower()

def get_file_hash(filepath):
    """Calculates MD5 hash of a file."""
    try:
        md5_hash = hashlib.md5()
        with open(filepath, "rb") as f:
            # Read in chunks to handle large files
            for byte_block in iter(lambda: f.read(4096), b""):
                md5_hash.update(byte_block)
        return md5_hash.hexdigest()
    except Exception:
        return None

def is_junk_attachment(filename, size_bytes):
    """Heuristic to determine if an attachment is likely a signature image."""
    ext = os.path.splitext(filename)[1].lower()
    # Filter small images (likely icons/logos)
    if ext in ['.gif', '.png', '.jpg', '.jpeg', '.tiff', '.bmp']:
        if size_bytes < 5 * 1024: # < 5KB
            return True
    return False

# --- Constants ---

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

# --- Text Processing ---

def remove_ignored_blocks(text, ignored_blocks):
    """
    Removes specific text blocks from the body.
    Since matching exact multi-line strings is hard due to formatting,
    we use a sliding window or fuzzy approach? 
    Actually, simpler: We check if the normalized line sequence matches.
    But since we process line by line later, it's better to remove them from the raw text first.
    """
    if not text or not ignored_blocks:
        return text
        
    # We can't easily replace normalized text in the raw string.
    # Strategy: Check if a paragraph (chunk separated by newlines) matches an ignored block.
    
    paragraphs = text.split('\n\n')
    cleaned_paragraphs = []
    
    for para in paragraphs:
        norm_para = normalize_text_blob(para)
        
        is_ignored = False
        for block in ignored_blocks:
            # Check if the block is contained in the paragraph (or vice versa if block is partial)
            # Using 'in' for robust matching
            if block in norm_para or norm_para in block:
                # Only ignore if it's a significant match, not just a common word
                if len(norm_para) > 10: 
                    is_ignored = True
                    break
        
        if not is_ignored:
            cleaned_paragraphs.append(para)
            
    return "\n\n".join(cleaned_paragraphs)

def normalize_line(line):
    """Strips quoting chars and whitespace for comparison."""
    return re.sub(r'^[\s>]+', '', line).strip()

def is_line_in_previous(line, prev_body_lines_set):
    """Checks if a normalized line exists in the previous message's normalized lines."""
    norm = normalize_line(line)
    if len(norm) < 10: # Don't dedup short lines (e.g. "Tak", "Hej")
        return False
    return norm in prev_body_lines_set

def clean_text_general(text, ignored_blocks=None):
    """Basic cleanup of disclaimers and headers."""
    if not text:
        return []
        
    # 1. Remove Ignored Text Blocks first
    if ignored_blocks:
        text = remove_ignored_blocks(text, ignored_blocks)
    
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

def apply_alias(email, aliases):
    """Replaces email with alias if found."""
    if not email: return "Unknown"
    # Check exact match
    if email.lower() in aliases:
        return aliases[email.lower()]
    
    # Optional: Logic to handle "Name <email>" format if passed
    if '<' in email:
        match = re.search(r'<([^>]+)>', email)
        if match:
            clean_email = match.group(1).lower()
            if clean_email in aliases:
                return aliases[clean_email]
                
    return email

# --- Main ---

def generate_transcripts():
    if not os.path.exists(INPUT_FILE):
        print(f"Input file {INPUT_FILE} not found.")
        return
    
    # Prepare Output Directory for Attachments
    if os.path.exists(OUTPUT_ATT_DIR):
        shutil.rmtree(OUTPUT_ATT_DIR)
    os.makedirs(OUTPUT_ATT_DIR)

    aliases = load_aliases()
    ignored_atts_set = load_ignored_set(IGNORED_ATT_FILE)
    ignored_msgs_set = load_ignored_set(IGNORED_MSG_FILE)
    ignored_text_blocks = load_ignored_text_blocks()

    with open(INPUT_FILE, 'r') as f:
        data = json.load(f)

    # Sort threads by date
    data = [t for t in data if t.get('messages')]
    data.sort(key=lambda t: t['messages'][0].get('date', ''))

    llm_output = []
    human_output = []
    
    # Used to handle filename collisions in output dir
    # filename -> count
    filename_collision_map = {}
    
    # Deduplication Map: MD5 -> Existing Filename in output folder
    # This ensures we only copy identical files once.
    md5_to_filename_map = {}

    for thread in data:
        subject = thread.get("subject", "No Subject")
        messages = thread.get("messages", [])
        
        # Header
        llm_output.append(f"# THREAD: {subject}")
        human_output.append(f"==================================================")
        human_output.append(f"THREAD ID: {thread.get('id', 'N/A')}")
        human_output.append(f"SUBJECT: {subject}")
        human_output.append(f"==================================================")
        
        prev_msg_lines_set = set()

        for msg in messages:
            msg_id = msg.get("id", "N/A")
            
            # Check Ignored Messages
            if msg_id in ignored_msgs_set:
                continue

            date_short = msg.get("date", "")[:10]
            full_date = msg.get("date", "")
            
            # Apply Aliases
            raw_sender = msg.get("from", "Unknown")
            sender_display = apply_alias(raw_sender, aliases)
            
            # Body Processing
            raw_body = msg.get("body", "")
            clean_lines = clean_text_general(raw_body, ignored_text_blocks)
            final_lines = []
            current_msg_lines_set = set()
            
            for line in clean_lines:
                current_msg_lines_set.add(normalize_line(line))
                if not is_line_in_previous(line, prev_msg_lines_set):
                    final_lines.append(line)
            
            body_text = " ".join(final_lines)
            prev_msg_lines_set = current_msg_lines_set

            # Attachment Processing
            atts = msg.get("attachments", [])
            valid_att_filenames = []
            valid_att_contents = [] # If text files exist
            
            for att in atts:
                filepath = att.get("path")
                filename = att.get("filename")
                
                if not filepath or not os.path.exists(filepath):
                    continue
                    
                file_hash = get_file_hash(filepath)
                file_size = os.path.getsize(filepath)
                
                # Check explicit ignore
                if file_hash in ignored_atts_set:
                    continue
                    
                # Check junk heuristic
                if is_junk_attachment(filename, file_size):
                    continue

                # Deduplication Logic
                if file_hash in md5_to_filename_map:
                    # We've seen this exact content before!
                    existing_filename = md5_to_filename_map[file_hash]
                    
                    # If the current filename is DIFFERENT from the existing canonical one,
                    # we record the reference but point to the canonical one.
                    
                    # Add reference to the transcript
                    # Format: "current_name.pdf (See: canonical_name.pdf)"
                    if filename != existing_filename:
                        valid_att_filenames.append(f"{filename} (See: {existing_filename})")
                    else:
                        valid_att_filenames.append(existing_filename)
                    
                    # We do NOT copy the file again.
                    # We do NOT extract text again (optimization).
                    
                else:
                    # New content! Copy and register.
                    
                    # Handle filename collisions (for DIFFERENT content with SAME name)
                    base, ext = os.path.splitext(filename)
                    
                    # Prefer Text Files for LLM Readability
                    has_text = att.get("has_text_file") and att.get("text_file_path") and os.path.exists(att.get("text_file_path"))
                    source_path = att.get("text_file_path") if has_text else filepath
                    
                    # If we are exporting the text file, append .txt to the filename (preserving original ext for clarity)
                    # e.g. "Contract.pdf" -> "Contract.pdf.txt"
                    if has_text:
                        final_filename = filename + ".txt"
                        # Update base/ext for collision logic below
                        base, ext = os.path.splitext(final_filename) 
                    else:
                        final_filename = filename

                    if final_filename in filename_collision_map:
                        filename_collision_map[final_filename] += 1
                        count = filename_collision_map[final_filename]
                        # Re-assemble filename with count
                        # e.g. "Contract.pdf_1.txt" or "Contract_1.pdf"
                        if has_text:
                             # Handle .pdf.txt specially to put number before .txt or before .pdf? 
                             # Simplest: Just append number to base. 
                             # base is "Contract.pdf"
                             final_filename = f"{base}_{count}{ext}"
                        else:
                             final_filename = f"{base}_{count}{ext}"
                    else:
                        filename_collision_map[final_filename] = 0
                    
                    # Register this filename as the canonical source for this hash
                    md5_to_filename_map[file_hash] = final_filename
                    
                    dest_path = os.path.join(OUTPUT_ATT_DIR, final_filename)
                    shutil.copy2(source_path, dest_path)

                    # Check for associated text file content (for inclusion IN TRANSCRIPT)
                    text_content = ""
                    if has_text:
                         with open(source_path, 'r', encoding='utf-8') as f:
                             text_content = f.read().strip()

                    # Add to list
                    valid_att_filenames.append(final_filename)
                    if text_content:
                        valid_att_contents.append(f"[Attachment Content: {final_filename}]\n{text_content}\n[End Attachment]")
            
            # --- Write to LLM Format ---
            if body_text or valid_att_filenames:
                llm_entry = f"[{date_short}] {sender_display}: {body_text}"
                if valid_att_filenames:
                    llm_entry += f" <Attachments: {', '.join(valid_att_filenames)}>"
                
                llm_output.append(llm_entry)
                
                # Append extracted text content from attachments immediately after reference
                for content in valid_att_contents:
                     llm_output.append(content)

            # --- Write to Human Format ---
            human_output.append(f"MSG ID: {msg_id}")
            human_output.append(f"Date:   {full_date}")
            human_output.append(f"From:   {raw_sender} ({sender_display})")
            human_output.append("-" * 20)
            
            if body_text:
                human_output.append(body_text)
            else:
                human_output.append("(Empty body / All text was repeated content)")
                
            if valid_att_filenames:
                human_output.append("\nAttachments:")
                for name in valid_att_filenames:
                    human_output.append(f"  - {name}")

            human_output.append("\n")

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
    att_count = len(os.listdir(OUTPUT_ATT_DIR))
    
    print(f"Generated LLM Transcript: {llm_size / 1024:.2f} KB")
    print(f"Generated Human Transcript: {human_size / 1024:.2f} KB")
    print(f"Copied {att_count} unique attachments to {OUTPUT_ATT_DIR}")

if __name__ == "__main__":
    generate_transcripts()
