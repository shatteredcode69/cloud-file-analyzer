
#!/usr/bin/env python3
"""
Serverless File Upload & Analysis — Fully Simulated (Single-File Edition)
API Gateway → S3 → Lambda → DynamoDB, all simulated on my machine.

• No external libraries
• Clear, pretty console output
• Safe to run multiple times
• Creates folders/files automatically

Author: Muhammad Abbas 
"""

from __future__ import annotations
import json
import hashlib
import mimetypes
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

# ---------- Configuration ----------
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR     = PROJECT_ROOT / "data"
S3_DIR       = DATA_DIR / "s3" / "uploads"
DB_FILE      = DATA_DIR / "db" / "dynamodb_mock.json"
LOG_FILE     = PROJECT_ROOT / "logs" / "lambda_output.log"
SAMPLES_DIR  = PROJECT_ROOT / "sample_files"

TEXT_EXTS = {".txt", ".md", ".py", ".json", ".csv", ".log", ".ini", ".cfg"}

# ---------- Utility: bootstrap ----------
def ensure_dirs() -> None:
    for p in [S3_DIR, DB_FILE.parent, LOG_FILE.parent, SAMPLES_DIR]:
        p.mkdir(parents=True, exist_ok=True)
    if not DB_FILE.exists():
        DB_FILE.write_text("[]", encoding="utf-8")
    if not LOG_FILE.exists():
        LOG_FILE.write_text("", encoding="utf-8")
    # seed sample file if missing
    sample = SAMPLES_DIR / "example.txt"
    if not sample.exists():
        sample.write_text(
            "Hello from example.txt\nThis is a sample text file.\nLine 3.\n",
            encoding="utf-8",
        )

def append_log(msg: str) -> None:
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {msg}\n")

def read_db() -> list[dict]:
    try:
        return json.loads(DB_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []

def write_db(items: list[dict]) -> None:
    DB_FILE.write_text(json.dumps(items, indent=2), encoding="utf-8")

# ---------- S3 (simulated) ----------
def s3_put_object(source_path: Path, key: str) -> Path:
    """
    Copy a local file into the simulated S3 as uploads/<key>
    """
    ensure_dirs()
    dest = S3_DIR / key
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(source_path.read_bytes())
    append_log(f"⬆️  S3 PutObject key='{key}' size={dest.stat().st_size} bytes")
    return dest

def s3_get_object_path(key: str) -> Path:
    return S3_DIR / key

# ---------- DynamoDB (simulated) ----------
def dynamodb_put_item(item: Dict[str, Any]) -> None:
    items = read_db()
    items.append(item)
    write_db(items)
    append_log(f"🗄️  DynamoDB PutItem filename='{item.get('filename')}'")

# ---------- Lambda (simulated) ----------
def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def _looks_like_text(path: Path, mime: Optional[str]) -> bool:
    if mime and mime.startswith("text/"):
        return True
    return path.suffix.lower() in TEXT_EXTS

def _count_lines_if_text(path: Path, is_text: bool) -> Optional[int]:
    if not is_text:
        return None
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return sum(1 for _ in f)
    except Exception:
        return None

def lambda_file_analyzer(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Expect event = {"Records":[{"s3":{"object":{"key":"<filename>"}}}]}
    """
    try:
        key = event["Records"][0]["s3"]["object"]["key"]
    except Exception:
        append_log("❌ Lambda received malformed event")
        return {"statusCode": 400, "body": json.dumps({"error": "Malformed event"})}

    path = s3_get_object_path(key)
    if not path.exists():
        append_log(f"❌ Lambda file not found: {key}")
        return {"statusCode": 404, "body": json.dumps({"error": "File not found", "key": key})}

    size = path.stat().st_size
    mime, _enc = mimetypes.guess_type(str(path))
    mime = mime or "application/octet-stream"
    digest = _sha256(path)
    is_text = _looks_like_text(path, mime)
    line_count = _count_lines_if_text(path, is_text)

    metadata = {
        "filename": key,
        "size_bytes": size,
        "mime_type": mime,
        "sha256": digest,
        "line_count": line_count,
        "processed_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "status": "Processed",
    }

    dynamodb_put_item(metadata)
    append_log(f"✅ Lambda processed '{key}' ({size} bytes, {mime})")
    return {"statusCode": 200, "body": json.dumps(metadata, indent=2)}

# ---------- API Gateway (simulated) ----------
def api_upload(local_file: Path) -> Dict[str, Any]:
    """
    Simulate POST /upload:
    1) PutObject to S3
    2) Trigger Lambda with an S3 event
    """
    ensure_dirs()
    if not local_file.exists():
        return {"statusCode": 400, "body": f"Local file not found: {local_file}"}
    key = local_file.name
    s3_put_object(local_file, key)
    event = {"Records": [{"s3": {"object": {"key": key}}}]}
    return lambda_file_analyzer(event)

# ---------- Styled printing ----------
def hr(title: str = "", ch: str = "─", width: int = 70) -> None:
    if title:
        pad = max(0, width - len(title) - 2)
        print(f"{title} {ch * pad}")
    else:
        print(ch * width)

def show_db() -> None:
    items = read_db()
    if not items:
        print("No records yet.")
        return
    for i, it in enumerate(items, 1):
        print(f"[{i}] {it.get('filename')}  •  {it.get('size_bytes')} bytes  •  {it.get('mime_type')}  •  {it.get('processed_utc')}")
    print()
    print(json.dumps(items, indent=2))

def show_logs() -> None:
    print(LOG_FILE.read_text(encoding="utf-8") or "(empty logs)")

def list_samples() -> list[Path]:
    ensure_dirs()
    return sorted([p for p in SAMPLES_DIR.glob("*") if p.is_file()])

# ---------- Demo / Menu ----------
def run_demo() -> None:
    """
    Process every sample file once.
    """
    ensure_dirs()
    files = list_samples()
    if not files:
        print("No sample files found.")
        return
    for p in files:
        resp = api_upload(p)
        print(json.dumps(resp, indent=2))

def main_menu() -> None:
    ensure_dirs()
    while True:
        hr("SERVERLESS FILE UPLOAD & ANALYSIS (SIMULATED) ⚡")
        print("1) Upload sample file")
        print("2) Choose a sample file to upload")
        print("3) Upload a custom file (enter path)")
        print("4) Show DynamoDB (mock) records")
        print("5) Show Lambda logs")
        print("6) Run full demo on all samples")
        print("7) Exit")
        choice = input("\nSelect an option: ").strip()

        if choice == "1":
            resp = api_upload(SAMPLES_DIR / "example.txt")
            print(json.dumps(resp, indent=2))
        elif choice == "2":
            files = list_samples()
            if not files:
                print("No sample files found.")
            else:
                for i, p in enumerate(files, 1):
                    print(f"{i}) {p.name}")
                idx = input("Pick a file number: ").strip()
                try:
                    sel = files[int(idx) - 1]
                    resp = api_upload(sel)
                    print(json.dumps(resp, indent=2))
                except Exception:
                    print("Invalid selection.")
        elif choice == "3":
            path = input("Enter full path to a local file: ").strip()
            resp = api_upload(Path(path).expanduser())
            print(json.dumps(resp, indent=2))
        elif choice == "4":
            hr("DYNAMODB (MOCK) RECORDS")
            show_db()
        elif choice == "5":
            hr("LAMBDA LOGS")
            show_logs()
        elif choice == "6":
            hr("RUN DEMO")
            run_demo()
        elif choice == "0":
            print("Goodbye! 👋")
            break
        else:
            print("Unknown option.")
        input("\nPress Enter to continue...")

# ---------- Entry ----------
if __name__ == "__main__":
    ensure_dirs()
    main_menu()
