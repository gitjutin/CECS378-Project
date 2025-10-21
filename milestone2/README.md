Component Purpose
- The websocket_server.py demonstrates peer-to-peer communication as to which we know is IPC. This implments real-time collaboration and chat features.
- Websockets also enable low-latency and direct connections between users within the same course.

- The rest_api_server.py provides a centralized entry point for publishing messages and events that go on.
- The FASTAPI creates endpoints for students to post chats or edit messages for asynchronous processing.

- The rabbitmq_pubsub.py lets students subscribe to courses and their channels so they can get any forwarded messages that is connected to the websocket client.
- This ensures that any class note updates are delivered asynchronously and demonstrates decoupled communication between the websocket layers.

How to Run Each Module (This may differ)
- Using MacOs:
- Install all dependencies inside a virtual environment
  
  python3 -m venv .venv
  source .venv/bin/activate
  python3 -m pip install fastapi uvicorn websockets redis asyncio

  **Some additional services**
  
  brew install redis
  brew services start redis

  **WebSocket Server**
  
  cd milestone2
  source .venv/bin/activate
  python3 websocket_server.py

  **Rest API Server**
  
  cd milestone2
  source .venv/bin/activate
  uvicorn rest_api_server:app --reload --port 8000

  **Redis Pub/Sub**
  cd milestone2
  source .venv/bin/activate
  python3 rabbitmq_pubsub.py





