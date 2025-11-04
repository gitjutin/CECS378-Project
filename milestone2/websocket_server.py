"""
Task 1: IPC using TCP Sockets
P2P Node for Real-Time Collaboration

This represents the peer-to-peer component of the architecture.
Enables low-latency real-time editing and chat between students.
Uses TCP sockets for reliable inter-process communication.
"""

import asyncio
import websockets
import argparse
import json
from datetime import datetime

# -------------------------
# Session tracking (yours)
# -------------------------
active_sessions = {}   # course_id -> set(websocket)
student_info = {}      # websocket -> {student_id, student_name, course_id}

# -------------------------
# Concurrency config (CLI-tunable)
# -------------------------
use_lock = True
max_section_writes = 3
race_delay_sec = 0.0005

# -------------------------
# Per-course concurrent state
# -------------------------
class CourseState:
    def __init__(self):
        self.lock = asyncio.Lock()                       # protects shared state
        self.apply_gate = asyncio.Semaphore(max_section_writes)
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self.total_ops = 0                               # shared counter (race demo)
        self.shutdown = asyncio.Event()
        self.consumer_task: asyncio.Task | None = None

course_state: dict[str, CourseState] = {}

def get_state(course_id: str) -> CourseState:
    if course_id not in course_state:
        course_state[course_id] = CourseState()
    return course_state[course_id]

# -------------------------
# Background worker
# -------------------------
async def apply_worker(course_id: str):
    """
    Pull from per-course queue, throttle with Semaphore, demo race (lock on/off),
    then broadcast to peers.
    """
    state = get_state(course_id)
    try:
        while not state.shutdown.is_set():
            try:
                item = await asyncio.wait_for(state.queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

            async with state.apply_gate:
                if use_lock:
                    async with state.lock:
                        tmp = state.total_ops
                        await asyncio.sleep(race_delay_sec)  # widen race window
                        state.total_ops = tmp + 1
                else:
                    tmp = state.total_ops
                    await asyncio.sleep(race_delay_sec)
                    state.total_ops = tmp + 1

                await broadcast_to_peers(course_id, item["broadcast"], exclude=item.get("exclude"))
    finally:
        print(f"[CONC] worker stopped course={course_id} total_ops={state.total_ops} USE_LOCK={use_lock}")

# -------------------------
# Broadcast helper (keep this ONE)
# -------------------------
async def broadcast_to_peers(course_id, message, exclude=None):
    peers = active_sessions.get(course_id)
    if not peers:
        return
    message_json = json.dumps(message)
    for peer in list(peers):  # snapshot to avoid 'set changed size' during iter
        if peer is exclude:
            continue
        try:
            await peer.send(message_json)
        except Exception:
            pass  # best-effort

# -------------------------
# WebSocket handler
# -------------------------
async def handle_peer_connection(websocket):  # <- 1 arg for modern websockets
    course_id = None
    student_id = None

    try:
        async for message in websocket:
            data = json.loads(message)
            msg_type = data.get('type')

            # ---- join_session ----
            if msg_type == 'join_session':
                course_id = data.get('course_id')
                student_id = data.get('student_id')
                student_name = data.get('student_name', 'Anonymous')

                active_sessions.setdefault(course_id, set()).add(websocket)
                student_info[websocket] = {
                    'student_id': student_id,
                    'student_name': student_name,
                    'course_id': course_id
                }

                # start per-course worker if needed
                state = get_state(course_id)
                if state.consumer_task is None or state.consumer_task.done():
                    state.shutdown.clear()
                    state.consumer_task = asyncio.create_task(apply_worker(course_id))
                    print(f"[CONC] started worker for course {course_id}")

                print(f"[P2P] {student_name} joined editing session for {course_id}")

                await broadcast_to_peers(course_id, {
                    'type': 'peer_joined',
                    'student_id': student_id,
                    'student_name': student_name,
                    'timestamp': datetime.now().isoformat()
                }, exclude=websocket)

            # ---- live_edit -> enqueue ----
            elif msg_type == 'live_edit' and course_id is not None:
                edit_data = data.get('edit', {})
                print(f"[P2P] Live edit from {student_id}: {edit_data.get('action')}")
                await get_state(course_id).queue.put({
                    "broadcast": {
                        'type': 'live_edit',
                        'student_id': student_id,
                        'edit': edit_data,
                        'timestamp': datetime.now().isoformat()
                    },
                    "exclude": websocket
                })

            # ---- chat_message -> enqueue ----
            elif msg_type == 'chat_message' and course_id is not None:
                message_text = data.get('message', '')
                name = student_info.get(websocket, {}).get('student_name', 'Anonymous')
                print(f"[P2P Chat] {student_id}: {message_text}")
                await get_state(course_id).queue.put({
                    "broadcast": {
                        'type': 'chat_message',
                        'student_id': student_id,
                        'student_name': name,
                        'message': message_text,
                        'timestamp': datetime.now().isoformat()
                    }
                })

    except websockets.exceptions.ConnectionClosed:
        print(f"[P2P] Connection closed for {student_id}")

    finally:
        info = student_info.pop(websocket, None)
        if info:
            course_id = info['course_id']
            peers = active_sessions.get(course_id)
            if peers:
                peers.discard(websocket)
                # if last peer left, stop worker
                if not peers:
                    state = get_state(course_id)
                    state.shutdown.set()
                    if state.consumer_task:
                        state.consumer_task.cancel()
                    print(f"[CONC] stopped worker for empty course {course_id} (total_ops={state.total_ops})")
            await broadcast_to_peers(course_id, {
                'type': 'peer_left',
                'student_id': info['student_id'],
                'student_name': info['student_name'],
                'timestamp': datetime.now().isoformat()
            })

# -------------------------
# Server bootstrap
# -------------------------
async def main(host: str, port: int):
    print("="*60)
    print("Campus Note Collaboration - P2P Node (IPC)")
    print("Real-time editing and chat via WebSockets")
    print(f"Listening on: ws://{host}:{port}")
    print(f"USE_LOCK={use_lock}  MAX_SECTION_WRITES={max_section_writes}  RACE_DELAY_SEC={race_delay_sec}")
    print("="*60)
    async with websockets.serve(handle_peer_connection, host, port):
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--no-lock", action="store_true", help="disable lock to demonstrate race condition")
    parser.add_argument("--lanes", type=int, default=3, help="semaphore capacity (concurrent applies)")
    parser.add_argument("--race-delay", type=float, default=0.0005, help="delay to widen race window")
    args = parser.parse_args()

    # apply CLI toggles BEFORE any CourseState is created
    use_lock = not args.no_lock
    max_section_writes = args.lanes
    race_delay_sec = args.race_delay

    asyncio.run(main(args.host, args.port))
