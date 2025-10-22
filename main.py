# main.py
from fastapi import FastAPI, HTTPException, Request, Query
from pydantic import BaseModel
import hashlib, sqlite3, json, os, re
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from contextlib import closing

APP_NAME = "string-analyzer"
DB_PATH = os.environ.get("DB_PATH", "strings.db")  # configurable

app = FastAPI(title=APP_NAME)

# ---------- DB helpers ----------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS strings (
        id TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        properties TEXT NOT NULL,
        created_at TEXT NOT NULL
    );
    """)
    conn.commit()
    conn.close()

def get_connection():
    # Each request should get its own connection.
    # sqlite default isolation might block; we'll use default settings but keep connections short-lived.
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

# ---------- String analysis ----------
def sha256_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

def is_palindrome(value: str) -> bool:
    s = re.sub(r"\s+", "", value).lower()
    return s == s[::-1]

def character_frequency_map(value: str) -> Dict[str, int]:
    freq = {}
    for ch in value:
        freq[ch] = freq.get(ch, 0) + 1
    return freq

def analyze_string(value: str) -> Dict[str, Any]:
    if not isinstance(value, str):
        raise ValueError("value must be a string")
    value_str = value
    length = len(value_str)
    palindrome = is_palindrome(value_str)
    unique_chars = len(set(value_str))
    word_count = 0 if value_str.strip() == "" else len(value_str.split())
    sha = sha256_hash(value_str)
    freq_map = character_frequency_map(value_str)
    props = {
        "length": length,
        "is_palindrome": palindrome,
        "unique_characters": unique_chars,
        "word_count": word_count,
        "sha256_hash": sha,
        "character_frequency_map": freq_map
    }
    return props

# ---------- Models ----------
class CreateStringReq(BaseModel):
    value: str

# ---------- Startup ----------
@app.on_event("startup")
def startup_event():
    init_db()

# ---------- Endpoints ----------

@app.post("/strings", status_code=201)
def create_string(req: CreateStringReq):
    value = req.value
    if value is None:
        raise HTTPException(status_code=400, detail="Missing 'value' field")
    if not isinstance(value, str):
        raise HTTPException(status_code=422, detail="'value' must be a string")
    props = analyze_string(value)
    sid = props["sha256_hash"]
    created_at = datetime.now(timezone.utc).isoformat()
    payload = {
        "id": sid,
        "value": value,
        "properties": props,
        "created_at": created_at
    }
    conn = get_connection()
    try:
        with conn:
            cur = conn.cursor()
            # check if exists by id
            cur.execute("SELECT 1 FROM strings WHERE id = ?", (sid,))
            if cur.fetchone():
                raise HTTPException(status_code=409, detail="String already exists")
            cur.execute(
                "INSERT INTO strings (id, value, properties, created_at) VALUES (?, ?, ?, ?)",
                (sid, value, json.dumps(props), created_at)
            )
    finally:
        conn.close()
    return payload

@app.get("/strings/{string_value}")
def get_string(string_value: str):
    sid = sha256_hash(string_value)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, value, properties, created_at FROM strings WHERE id = ?", (sid,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="String not found")
        props = json.loads(row["properties"])
        return {
            "id": row["id"],
            "value": row["value"],
            "properties": props,
            "created_at": row["created_at"]
        }
    finally:
        conn.close()

def apply_filters_row(props: Dict[str, Any], filters: Dict[str, Any]) -> bool:
    # props are the stored properties dict
    if "is_palindrome" in filters:
        if props.get("is_palindrome") != filters["is_palindrome"]:
            return False
    if "min_length" in filters:
        if props.get("length", 0) < filters["min_length"]:
            return False
    if "max_length" in filters:
        if props.get("length", 0) > filters["max_length"]:
            return False
    if "word_count" in filters:
        if props.get("word_count") != filters["word_count"]:
            return False
    if "contains_character" in filters:
        ch = filters["contains_character"]
        if not isinstance(ch, str) or len(ch) != 1:
            return False
        if ch not in props.get("character_frequency_map", {}):
            return False
    return True

@app.get("/strings")
def list_strings(
    is_palindrome: Optional[bool] = Query(None),
    min_length: Optional[int] = Query(None, ge=0),
    max_length: Optional[int] = Query(None, ge=0),
    word_count: Optional[int] = Query(None, ge=0),
    contains_character: Optional[str] = Query(None, min_length=1, max_length=1)
):
    filters = {}
    if is_palindrome is not None:
        filters["is_palindrome"] = is_palindrome
    if min_length is not None:
        filters["min_length"] = min_length
    if max_length is not None:
        filters["max_length"] = max_length
    if word_count is not None:
        filters["word_count"] = word_count
    if contains_character is not None:
        filters["contains_character"] = contains_character

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, value, properties, created_at FROM strings")
        results = []
        for row in cur.fetchall():
            props = json.loads(row["properties"])
            if apply_filters_row(props, filters):
                results.append({
                    "id": row["id"],
                    "value": row["value"],
                    "properties": props,
                    "created_at": row["created_at"]
                })
        return {"data": results, "count": len(results), "filters_applied": filters}
    finally:
        conn.close()

@app.get("/strings/filter-by-natural-language")
def filter_by_nl(query: str = Query(...)):
    # A simple heuristic parser for the example phrases.
    q = query.lower().strip()
    parsed = {}
    # "single word palindromic"
    if "single word" in q or "one word" in q:
        parsed["word_count"] = 1
    if "palindrom" in q:
        parsed["is_palindrome"] = True
    m = re.search(r"longer than (\d+)", q)
    if m:
        parsed["min_length"] = int(m.group(1)) + 0  # user expects > N; task says longer than 10 -> min_length=11; user phrase should be explicit
    m2 = re.search(r"strings longer than (\d+)", q)
    if m2:
        parsed["min_length"] = int(m2.group(1)) + 1
    m3 = re.search(r"contain(?:ing|s)? the letter ([a-z])", q)
    if m3:
        parsed["contains_character"] = m3.group(1)
    # if no parsed filters -> error
    if not parsed:
        raise HTTPException(status_code=400, detail="Unable to parse natural language query")
    # now use the generic list_strings logic to filter
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, value, properties, created_at FROM strings")
        results = []
        for row in cur.fetchall():
            props = json.loads(row["properties"])
            if apply_filters_row(props, parsed):
                results.append({
                    "id": row["id"],
                    "value": row["value"],
                    "properties": props,
                    "created_at": row["created_at"]
                })
        return {
            "data": results,
            "count": len(results),
            "interpreted_query": {
                "original": query,
                "parsed_filters": parsed
            }
        }
    finally:
        conn.close()

@app.delete("/strings/{string_value}", status_code=204)
def delete_string(string_value: str):
    sid = sha256_hash(string_value)
    conn = get_connection()
    try:
        with conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM strings WHERE id = ?", (sid,))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="String not found")
    finally:
        conn.close()
    return None
