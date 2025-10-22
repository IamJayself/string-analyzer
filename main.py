from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import hashlib, sqlite3, json
from datetime import datetime, timezone

app = FastAPI(
    title="String Analyzer API",
    description="A simple API that analyzes strings and stores their properties.",
    version="1.0.0"
)

DB_NAME = "strings.db"

# -------- Database setup --------
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS strings (
        id TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        properties TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """)
    conn.commit()
    conn.close()

@app.on_event("startup")
def startup():
    init_db()

# -------- Helper functions --------
def sha256_of(text):
    return hashlib.sha256(text.encode()).hexdigest()

def compute_properties(text):
    length = len(text)
    is_palindrome = text.lower().replace(" ", "") == text.lower().replace(" ", "")[::-1]
    unique_characters = len(set(text))
    word_count = len(text.split())
    sha256_hash = sha256_of(text)
    freq = {}
    for char in text:
        freq[char] = freq.get(char, 0) + 1
    return {
        "length": length,
        "is_palindrome": is_palindrome,
        "unique_characters": unique_characters,
        "word_count": word_count,
        "sha256_hash": sha256_hash,
        "character_frequency_map": freq
    }

# -------- Models --------
class StringRequest(BaseModel):
    value: str

# -------- Routes --------

@app.post("/strings", status_code=201)
def analyze_and_store(data: StringRequest):
    text = data.value
    if not isinstance(text, str):
        raise HTTPException(status_code=422, detail="Value must be a string")

    props = compute_properties(text)
    string_id = props["sha256_hash"]

    # check if exists
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT id FROM strings WHERE id=?", (string_id,))
    if cur.fetchone():
        conn.close()
        raise HTTPException(status_code=409, detail="String already exists")

    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    cur.execute(
        "INSERT INTO strings (id, value, properties, created_at) VALUES (?, ?, ?, ?)",
        (string_id, text, json.dumps(props), created_at)
    )
    conn.commit()
    conn.close()

    return {"id": string_id, "value": text, "properties": props, "created_at": created_at}

@app.get("/strings/{string_value}")
def get_string(string_value: str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT id, value, properties, created_at FROM strings WHERE value=?", (string_value,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="String not found")

    return {
        "id": row[0],
        "value": row[1],
        "properties": json.loads(row[2]),
        "created_at": row[3]
    }

@app.get("/strings")
def list_strings(
    is_palindrome: bool | None = Query(None),
    min_length: int | None = Query(None),
    max_length: int | None = Query(None),
    word_count: int | None = Query(None),
    contains_character: str | None = Query(None)
):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT id, value, properties, created_at FROM strings")
    rows = cur.fetchall()
    conn.close()

    data = []
    for row in rows:
        props = json.loads(row[2])

        # Filtering
        if is_palindrome is not None and props["is_palindrome"] != is_palindrome:
            continue
        if min_length is not None and props["length"] < min_length:
            continue
        if max_length is not None and props["length"] > max_length:
            continue
        if word_count is not None and props["word_count"] != word_count:
            continue
        if contains_character and contains_character not in row[1]:
            continue

        data.append({
            "id": row[0],
            "value": row[1],
            "properties": props,
            "created_at": row[3]
        })

    return {
        "count": len(data),
        "filters_applied": {
            "is_palindrome": is_palindrome,
            "min_length": min_length,
            "max_length": max_length,
            "word_count": word_count,
            "contains_character": contains_character
        },
        "data": data
    }

@app.get("/strings/filter-by-natural-language")
def natural_language_filter(query: str = Query(...)):
    q = query.lower()
    filters = {}

    if "palindromic" in q or "palindrome" in q:
        filters["is_palindrome"] = True

    if "single word" in q or "one word" in q:
        filters["word_count"] = 1

    if "longer than" in q:
        nums = [int(s) for s in q.split() if s.isdigit()]
        if nums:
            filters["min_length"] = nums[0] + 1

    if "containing" in q or "contains" in q:
        for word in q.split():
            if len(word) == 1 and word.isalpha():
                filters["contains_character"] = word
                break

    # apply filters
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT id, value, properties, created_at FROM strings")
    rows = cur.fetchall()
    conn.close()

    results = []
    for row in rows:
        props = json.loads(row[2])
        if "is_palindrome" in filters and props["is_palindrome"] != filters["is_palindrome"]:
            continue
        if "min_length" in filters and props["length"] < filters["min_length"]:
            continue
        if "word_count" in filters and props["word_count"] != filters["word_count"]:
            continue
        if "contains_character" in filters and filters["contains_character"] not in row[1]:
            continue

        results.append({
            "id": row[0],
            "value": row[1],
            "properties": props,
            "created_at": row[3]
        })

    if not filters:
        raise HTTPException(status_code=400, detail="Unable to parse natural language query")

    return {
        "data": results,
        "count": len(results),
        "interpreted_query": {
            "original": query,
            "parsed_filters": filters
        }
    }

@app.delete("/strings/{string_value}", status_code=204)
def delete_string(string_value: str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("DELETE FROM strings WHERE value=?", (string_value,))
    conn.commit()
    deleted = cur.rowcount
    conn.close()
    if deleted == 0:
        raise HTTPException(status_code=404, detail="String not found")
    return

@app.get("/health")
def health_check():
    return {"status": "ok", "message": "String Analyzer API is running 🚀"}
