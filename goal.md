You are an expert Python engineer highly experienced with Google Gmail API.

Goal

Create a robust data pipeline to extract legal case timelines from Gmail. The system is split into three decoupled scripts to ensure reliability, easy debugging, and idempotency.

Architecture: The "Fetch-Then-Process" Pattern

1.  `1_authenticate.py`: Handles OAuth flow and saves credentials.
2.  `2_fetch_data.py` (The Archiver): Connects to Gmail, downloads raw thread data (JSON) and attachments to disk. Does ZERO processing.
3.  `3_process_data.py` (The Processor): Reads local files, cleans text (strips quotes), extracts PDF content, and generates final reports.

⸻

Prerequisites (assume already done / explain but don’t implement UI)

1.  User has created a Google Cloud project and enabled the Gmail API.
2.  User has downloaded an OAuth `oauth-credentials.json` file (Desktop app type).
3.  User will run the script locally with Python 3.9+.
4.  Required libraries (update `requirements.txt`):
    • `google-api-python-client`, `google-auth-httplib2`, `google-auth-oauthlib`
    • `pypdf` (for PDF text extraction)
    • `email-reply-parser` (for robust email quote stripping)
    • `beautifulsoup4` (for HTML to text conversion)
    • `pandas`, `python-dateutil`

⸻

Configuration

The scripts should share a common config (or simple constants at the top):
• `GMAIL_LABEL_NAME`: "boet-efter-far"
• `DATA_DIR`: "./data" (Raw storage)
• `OUTPUT_DIR`: "./output" (Final reports)

⸻

Script 1: Authentication (`1_authenticate.py`)

1.  Check if `token.json` exists.
2.  If not, run the OAuth flow using `oauth-credentials.json`.
3.  Scopes: `https://www.googleapis.com/auth/gmail.readonly`
4.  Save `token.json`.

⸻

Script 2: The Archiver (`2_fetch_data.py`)

Goal: Mirror Gmail data to local disk. Safe to run repeatedly (incremental sync).

1.  **Setup**: Load `token.json`, build Gmail service.
2.  **Resolve Label**: Find ID for `GMAIL_LABEL_NAME`.
3.  **List Threads**: Fetch all thread IDs with that label.
4.  **Download Loop**:
    For each `thread_id`:
    a. Check if `DATA_DIR/raw/{thread_id}.json` exists.
    • If yes, SKIP (assume immutable history).
    • (Optional: Add a --force flag to overwrite).
    b. Call `users.threads.get(format="full")`.
    c. Save raw JSON response to `DATA_DIR/raw/{thread_id}.json`.
    d. **Download Attachments**:
    • Parse the raw JSON to find attachment IDs.
    • Download content to `DATA_DIR/attachments/{thread_id}/{message_id}_{filename}`.
    • Skip if file already exists.
5.  **Logging**: Show progress (e.g., "Synced 10/50 threads").

⸻

Script 3: The Processor (`3_process_data.py`)

Goal: Pure data transformation. No API calls. Fast iteration.

1.  **Load Data**: Iterate through all `*.json` files in `DATA_DIR/raw/`.
2.  **Process Each Thread**:
    a. Parse JSON.
    b. Sort messages by `internalDate`.
    c. **Extract Body**:
    • Prefer `text/plain`.
    • If only `text/html`, use **BeautifulSoup** to convert to text.
    d. **Clean Content (Crucial)**:
    • Use `EmailReplyParser.read(text).reply` to strip quotes/history automatically.
    • Do NOT use custom regex for this.
    e. **Process Attachments**:
    • Look up downloaded files in `DATA_DIR/attachments/{thread_id}/`.
    • If PDF: Use `pypdf` to extract text.
    • **WARNING**: If extracted text is empty/whitespace (scanned image), LOG A WARNING: "Warning: PDF {filename} contains images only - OCR required."
    • Store text in metadata.
3.  **Build Timeline**:
    • Create `lawyer_case_timelines.json` (Structured nested data).
    • Create `messages_timeline.csv` (Flat table).
4.  **Output**: Save both files to `OUTPUT_DIR`.

⸻

Final Output Expectations

1.  `DATA_DIR/` contains a complete backup of the legal case emails (JSON + Files).
2.  `OUTPUT_DIR/lawyer_case_timelines.json`: Cleaned, de-duplicated JSON for AI analysis.
3.  `OUTPUT_DIR/messages_timeline.csv`: Human-readable summary.
