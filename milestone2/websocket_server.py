"""
Task 1: IPC using TCP Sockets
P2P Node for Real-Time Collaboration

This represents the peer-to-peer component of the architecture.
Enables low-latency real-time editing and chat between students.
Uses TCP sockets for reliable inter-process communication.
"""

import asyncio
import websockets



import json
from datetime import datetime

#-------------------------------------
# Track active P2P sessions per course
#-------------------------------------

# Active collaboration sessions per course.
# Each course_id maps to a set of connected WebSocket objects (students currently online).
active_sessions = {}  # {course_id: {student_websockets}}

# Tracks information about each connected student.
# Key = the WebSocket connection, Value = { student_id, student_name, course_id }
student_info = {}     # {websocket: student_data}

# -------------------------------------------------------------------
#  Main connection handler: triggered every time a new peer connects.
# -------------------------------------------------------------------
async def handle_peer_connection(websocket, path):
    """
    Handle P2P connection between students
    TCP socket-based IPC for real-time collaboration

    This function continuously listens for messages from a connected peer
    and acts based on the 'type' of message received.
    """
    course_id = None
    student_id = None
    
    try:
        # Asynchronous loop: keeps reading messages from this websocket
        async for message in websocket:
            data = json.loads(message)          # Parse JSON payload
            msg_type = data.get('type')         # Determine msg purpose 
                                                # Ex: 'join_session' or 'live_edit'

            # -------------------------------------------------------
            # Case 1: Student joins a session (register themselves)
            # -------------------------------------------------------
            if msg_type == 'join_session':
                # Student joins P2P editing session
                course_id = data.get('course_id')
                student_id = data.get('student_id')
                student_name = data.get('student_name', 'Anonymous')
                
                # Create session entry if it doesn’t exist
                if course_id not in active_sessions:
                    active_sessions[course_id] = set()
                # Add this student's connection to the session set
                active_sessions[course_id].add(websocket)
                
                # Store data about this connection
                student_info[websocket] = {
                    'student_id': student_id,
                    'student_name': student_name,
                    'course_id': course_id
                }
                
                print(f"[P2P] {student_name} joined editing session for {course_id}")
                
                # Notify all other peers in the same course session
                await broadcast_to_peers(course_id, {
                    'type': 'peer_joined',
                    'student_id': student_id,
                    'student_name': student_name,
                    'timestamp': datetime.now().isoformat()
                }, exclude=websocket)

            # -------------------------------------------------------
            # Case 2: Live edit message — broadcast real-time edits
            # -------------------------------------------------------    
            elif msg_type == 'live_edit':
                # Student making real-time edit (P2P broadcast)
                edit_data = data.get('edit')
                print(f"[P2P] Live edit from {student_id}: {edit_data.get('action')}")
                
                # Broadcast edit to all peers in same session
                await broadcast_to_peers(course_id, {
                    'type': 'live_edit',
                    'student_id': student_id,
                    'edit': edit_data,
                    'timestamp': datetime.now().isoformat()
                }, exclude=websocket)

            # -------------------------------------------------------
            # Case 3: Chat message — real-time peer chat
            # -------------------------------------------------------    
            elif msg_type == 'chat_message':
                # P2P chat between students
                message_text = data.get('message')
                print(f"[P2P Chat] {student_id}: {message_text}")
                
                # Broadcast chat to all peers
                await broadcast_to_peers(course_id, {
                    'type': 'chat_message',
                    'student_id': student_id,
                    'student_name': student_info[websocket]['student_name'],
                    'message': message_text,
                    'timestamp': datetime.now().isoformat()
                })
    
    # -------------------------------------------------------------------
    # If a connection drops unexpectedly (user closes tab or disconnects)
    # -------------------------------------------------------------------
    except websockets.exceptions.ConnectionClosed:
        print(f"[P2P] Connection closed for {student_id}")
    

    # -------------------------------------------------------------------
    # Always run this cleanup logic when a peer disconnects
    # -------------------------------------------------------------------
    finally:
        # Cleanup on disconnect
        if websocket in student_info:
            info = student_info[websocket]
            course_id = info['course_id']
            
            # Remove student from P2P session list
            if course_id in active_sessions:
                active_sessions[course_id].discard(websocket)
                if len(active_sessions[course_id]) == 0:
                    del active_sessions[course_id]
            
            # Remove student record
            del student_info[websocket]
            
            # Notify peers someone left
            await broadcast_to_peers(course_id, {
                'type': 'peer_left',
                'student_id': info['student_id'],
                'student_name': info['student_name'],
                'timestamp': datetime.now().isoformat()
            })

async def broadcast_to_peers(course_id, message, exclude=None):
    """
    Broadcast message to all peers in a P2P session
    Simulates direct process-to-process communication
    """
    if course_id not in active_sessions:
        return
    
    message_json = json.dumps(message)
    
    for peer in active_sessions[course_id]:
        if peer != exclude:
            try:
                await peer.send(message_json)
            except:
                pass

# -------------------------------------------------------------------
#  Start the WebSocket server
# -------------------------------------------------------------------
async def main():
    """Start P2P coordination server"""
    print("="*60)
    print("Campus Note Collaboration - P2P Node (IPC)")
    print("Real-time editing and chat via TCP sockets")
    print("Listening on: ws://localhost:8765")  # WebSocket URL clients connect to
    print("="*60)
    
    # websockets.serve() creates an async TCP server that speaks the WebSocket protocol
    async with websockets.serve(handle_peer_connection, "0.0.0.0", 8765):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())