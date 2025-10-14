"""
Task 2: REST API Implementation
Central Server for Note Management (Client-Server Backbone)

This represents the client-server component of the architecture.
Handles authentication, access control, and long-term storage.
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
from datetime import datetime
import json, time, redis

app = FastAPI(title="Central Notes Server (REST)")
# temporary in-memory storage
DOCS: Dict[str, Dict[str, Any]] = {}   # {f"{course_id}:{note_id}": {"content": str, "last_ts": int}}

# connect to local Redis server for string pub-sub
r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

# data
class LiveEditIn(BaseModel):
    course_id: str
    note_id: str
    student_id: str
    action: str         # e.g., "insert 'limit' @pos=5"
    meta: Dict[str, Any] | None = None
    ts: int | None = None

# represents chat message
class ChatIn(BaseModel):
    course_id: str
    student_id: str
    student_name: str
    message: str
    ts: int | None = None

# helper function
def nowts() -> int:
    return int(time.time())

# publishes JSON payload to Redis channel
def publish_event(course_id: str, payload: Dict[str, Any]):
    # one channel per course; P2P node will psubscribe "course:*:events"
    channel = f"course:{course_id}:events"
    r.publish(channel, json.dumps(payload))

# check if server is up
@app.get("/health")
def health():
    return {"ok": True}

# handles live edits
@app.post("/notes/live_edit")
def live_edit(inp: LiveEditIn):
    ts = inp.ts or nowts()
    key = f"{inp.course_id}:{inp.note_id}"
    doc = DOCS.get(key, {"content": "", "last_ts": 0})
    if ts >= doc["last_ts"]: # ensure edits are applied if they are newer
        doc["content"] += f"\n[{inp.student_id}@{ts}] {inp.action}"
        doc["last_ts"] = ts
    DOCS[key] = doc

    # publish WS-compatible payload
    event = {
        "type": "live_edit",
        "course_id": inp.course_id,
        "student_id": inp.student_id,
        "edit": {"note_id": inp.note_id, "action": inp.action, "meta": inp.meta or {}},
        "timestamp": datetime.utcnow().isoformat()
    }
    publish_event(inp.course_id, event)
    return {"ok": True, "note_key": key, "last_ts": doc["last_ts"], "content": doc["content"]}

# retrieves content/timestamp of note
@app.get("/notes/{course_id}/{note_id}")
def get_note(course_id: str, note_id: str):
    key = f"{course_id}:{note_id}"
    if key not in DOCS:
        raise HTTPException(404, "note not found")
    return {"course_id": course_id, "note_id": note_id, **DOCS[key]}

# publish chat message event through Redis
@app.post("/chat/send")
def send_chat(inp: ChatIn):
    ts = inp.ts or nowts()
    event = {
        "type": "chat_message",
        "course_id": inp.course_id,
        "student_id": inp.student_id,
        "student_name": inp.student_name,
        "message": inp.message,
        "timestamp": datetime.utcnow().isoformat()
    }
    publish_event(inp.course_id, event)
    return {"ok": True, "ts": ts}
