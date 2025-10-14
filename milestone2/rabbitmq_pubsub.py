"""
Task 3: Indirect Communication - Message Queue (Pub/Sub)
Event Notification System for Campus Platform

This demonstrates asynchronous, decoupled communication.
Different services subscribe to note events without direct coupling.
Examples would include notifications, search indexing, backup services
"""

import asyncio, json
import websockets
import redis.asyncio as aioredis # using Redis as a lightweight pub/sub broker 

WS_URL = "ws://127.0.0.1:8765"   # connects to our Task-1 P2P WebSocket server
REDIS_HOST = "127.0.0.1" # assumes Redis is running locally 
REDIS_PORT = 6379 # default Redis port

# keep track of active WebSocket conenctions per (course_id, student_id)
connections = {}  # { (course_id, student_id): websocket }

async def ensure_join(course_id: str, student_id: str, student_name: str):
    key = (course_id, student_id)
    ws = connections.get(key)

    # reuse an open connection if available 
    if ws and ws.open:
        return ws
    # otherwise open new connection to the WebSocket server
    ws = await websockets.connect(WS_URL)

    # send a registration payload to join the collaboration session 
    await ws.send(json.dumps({
        "type": "join_session",
        "course_id": course_id,
        "student_id": student_id,
        "student_name": student_name or student_id
    }))

    # cache the WebSocket for reuse 
    connections[key] = ws
    return ws

async def handle_event(evt: dict):
    etype = evt.get("type")
    course_id = evt.get("course_id")
    student_id = evt.get("student_id") or "server_bridge"
    student_name = evt.get("student_name") or student_id

    # ignore the malformed events
    if not course_id or not etype:
        return

    # make sure the WebSocket connection exists
    ws = await ensure_join(course_id, student_id, student_name)

    # handle edit-type events coming from Redis
    if etype == "live_edit":
        edit = evt.get("edit", {})
        # the WebSocket server expects messages shaped like {"type": "live_edit", "edit":{...}}
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
