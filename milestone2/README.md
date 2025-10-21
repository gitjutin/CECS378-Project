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
  
  python3 -m venv .venv <br>
  source .venv/bin/activate <br>
  python3 -m pip install fastapi uvicorn websockets redis asyncio <br>

  **Some additional services** <br>
  
  brew install redis <br>
  brew services start redis <br>

  **WebSocket Server** <br>
  
  cd milestone2 <br>
  source .venv/bin/activate <br>
  python3 websocket_server.py <br>

  **Rest API Server**
  
  cd milestone2 <br>
  source .venv/bin/activate <br>
  uvicorn rest_api_server:app --reload --port 8000 <br>

  **Redis Pub/Sub** <br>
  
  cd milestone2 <br>
  source .venv/bin/activate <br>
  python3 rabbitmq_pubsub.py <br>





