# String Analyzer API

## Description
An API that analyzes strings and returns properties like:
- length
- palindrome status
- unique character count
- sha256 hash
- character frequency

## Tech Stack
- FastAPI
- SQLite
- Python 3.10+

## Endpoints
### POST /strings
Analyze and save a new string.

### GET /strings
List all analyzed strings.

### GET /strings/{value}
Retrieve a single string analysis.

### DELETE /strings/{value}
Delete a string.

## How to Run Locally
```bash
git clone <your_repo_url>
cd <project_folder>
pip install -r requirements.txt
uvicorn main:app --reload
