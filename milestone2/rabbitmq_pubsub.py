"""
Task 3: Indirect Communication - Message Queue (Pub/Sub)
Event Notification System for Campus Platform

This demonstrates asynchronous, decoupled communication.
Different services subscribe to note events without direct coupling.
Examples: Notifications, search indexing, backup services
"""

import asyncio, json
import websockets
import redis.asyncio as aioredis

WS_URL = "ws://127.0.0.1:8765"   # your Task-1 server
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379

# Maintain one WS connection per (course_id, student_id)
connections = {}  # { (course_id, student_id): websocket }

async def ensure_join(course_id: str, student_id: str, student_name: str):
    key = (course_id, student_id)
    ws = connections.get(key)
    if ws and ws.open:
        return ws
    # open new connection and join the session
    ws = await websockets.connect(WS_URL)
    await ws.send(json.dumps({
        "type": "join_session",
        "course_id": course_id,
        "student_id": student_id,
        "student_name": student_name or student_id
    }))
    connections[key] = ws
    return ws

async def handle_event(evt: dict):
    etype = evt.get("type")
    course_id = evt.get("course_id")
    student_id = evt.get("student_id") or "server_bridge"
    student_name = evt.get("student_name") or student_id

    if not course_id or not etype:
        return

    ws = await ensure_join(course_id, student_id, student_name)

    if etype == "live_edit":
        edit = evt.get("edit", {})
        # Your server expects: {"type":"live_edit","edit":{...}}
        await ws.send(json.dumps({
            "type": "live_edit",
            "edit": edit
        }))

    elif etype == "chat_message":
        msg = evt.get("message", "")
        # Your server expects: {"type":"chat_message","message":"..."}
        await ws.send(json.dumps({
            "type": "chat_message",
            "message": msg
        }))

async def main():
    print("[ws_bridge] starting; subscribing to course:*:events")
    r = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    p = r.pubsub()
    await p.psubscribe("course:*:events")

    async for msg in p.listen():
        if msg["type"] not in {"message", "pmessage"}:
            continue
        try:
            evt = json.loads(msg["data"])
            await handle_event(evt)
        except Exception as e:
            print("[ws_bridge] error:", e)

if __name__ == "__main__":
    asyncio.run(main())
