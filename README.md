# Email Extraction & Timeline Pipeline

This project scrapes email threads from Gmail, processes them to remove duplicates/signatures (including Danish support), extracts text from PDFs, and outputs a clean JSON/CSV dataset suitable for AI analysis.

## Architecture

The system follows a **Fetch-then-Process** architecture to ensure robustness and idempotency.

1.  **`1_authenticate.py`**: Handles Google OAuth2 login.
2.  **`2_fetch_data.py`** (The Archiver): Downloads raw email JSON and attachments to `./data/`. Safe to run repeatedly.
3.  **`3_process_data.py`** (The Processor): Parses local data, strips quotes/signatures, extracts PDF text, and builds the final timeline.

## Setup

1.  **Prerequisites**:

    - Python 3.9+
    - A Google Cloud Project with **Gmail API** enabled.
    - `oauth-credentials.json` (Desktop App client ID) placed in this folder.

2.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
    _Note: This installs a custom fork of `mail-parser-reply` with improved Danish language support._

## Usage

### Step 1: Authenticate

Run this once to generate `token.json`.

```bash
python 1_authenticate.py
```

### Step 2: Fetch Data

Downloads emails and attachments. Can be stopped and resumed.

```bash
python 2_fetch_data.py
```

- Data is saved to `data/raw/*.json`
- Attachments are saved to `data/attachments/<thread_id>/`

### Step 3: Process Data

Generates the clean output. No internet required.

```bash
python 3_process_data.py
```

## Output

- **`output/lawyer_case_timelines.json`**:
  - Highly optimized, token-minimized JSON structure.
  - Grouped by Thread -> Messages.
  - Signatures and reply history stripped.
  - PDF content referenced via sidecar text files.
- **`output/messages_timeline.csv`**: Flat table for quick auditing.
- **`data/attachments/.../*.txt`**: Extracted text for every PDF attachment.

## Configuration

Configuration (e.g., `GMAIL_LABEL_NAME`) is found at the top of `2_fetch_data.py`.
output directories are configured in `3_process_data.py`.
