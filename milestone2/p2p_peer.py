import asyncio
import argparse
import json
import uuid
import websockets


class PeerNode:

    def __init__(self, name: str, port: int, peer_urls: list[str]):
        self.name = name
        self.port = port
        self.known_peers: set[str] = set(peer_urls)  
        self.connections: set[websockets.WebSocketClientProtocol] = set()
        self.seen_messages: set[str] = set()

        # ---- lamport logical clock ----

        self.clock: int = 0
        self.event_log: list[tuple[int, str]] = [] #(timestamp, description)

        # ---- ricard-agrawla distributed mutual exclusion ----
        self.state = "RELEASED"       # RELEASED, WANTED, HELD
        self.request_ts = None        # timestamp of our CS request
        self.replies_pending = set()  # peers we are waiting for replies from
        self.deferred_replies = set()



 # ---- lamport clock helpers ----

 # increments the logical clock and returns new value
    def tick(self) -> int:
        self.clock += 1
        return self.clock
    
    #merges the remote time stamp into local clock
    def update_from_remote(self, remote_ts: int | None) -> int:
        if remote_ts is None:
            return self.tick()
        self.clock = max(self.clock, remote_ts) + 1
        return self.clock
    
    def log_event(self, label: str) -> None:
        self.event_log.append((self.clock, label))
        print(f"[{self.name}][ts = {self.clock}]{label}")



  # ---- server ----
    async def start_server(self):
        async def handler(ws):
            self.connections.add(ws)
            print(f"[{self.name}] inbound connection")
            try:
                async for raw in ws:
                    await self.handle_message(raw, ws)
            except websockets.exceptions.ConnectionClosed:
                pass
            finally:
                self.connections.discard(ws)
                print(f"[{self.name}] inbound connection closed")

        server = await websockets.serve(handler, "localhost", self.port)
        print(f"[{self.name}] listening on ws://localhost:{self.port}")
        return server

    # client
    async def connect_to_peer_loop(self, url: str):
        """
        Keep trying to connect to the given peer URL.
        Demonstrates fault tolerance (retry on failure).
        """
        while True:
            try:
                print(f"[{self.name}] trying to connect to {url}")
                ws = await websockets.connect(url)
                self.connections.add(ws)
                print(f"[{self.name}] connected to {url}")

                await self.send_hello(ws)

                async for raw in ws:
                    await self.handle_message(raw, ws)

            except Exception as e:
                print(f"[{self.name}] connection to {url} failed: {e} (will retry)")
                await asyncio.sleep(2)
            finally:
                if "ws" in locals():
                    self.connections.discard(ws)

    async def connect_to_peers(self):
        for url in list(self.known_peers):
            asyncio.create_task(self.connect_to_peer_loop(url))

    # message handling
    async def send_hello(self, ws=None):
        msg = {
            "type": "hello",
            "from": self.name,
            "peers": list(self.known_peers),
        }
        await self.send_message(msg, ws)

    async def send_chat(self, text: str):
        msg = {
            "type": "chat",
            "from": self.name,
            "text": text,
        }
        await self.broadcast(msg)

    async def handle_message(self, raw: str, src_ws):
        data = json.loads(raw)
        msg_id = data.get("id")


        # ---- lamport clock: merge remote timestamp ----
        incoming_ts = data.get("timestamp")
        new_ts = self.update_from_remote(incoming_ts)
        self.log_event(
            f"recv {data.get('type')} from {data.get('from')}(remote_ts = {incoming_ts}) -> ts = {new_ts}"
        )


        # ----loop prevention----
        if msg_id and msg_id in self.seen_messages:
            return
        if msg_id:
            self.seen_messages.add(msg_id)

        msg_type = data.get("type")

        if msg_type == "hello":
            # decentralized discovery: merge peer lists
            for p in data.get("peers", []):
                if p != f"ws://localhost:{self.port}":
                    if p not in self.known_peers:
                        print(f"[{self.name}] learned new peer {p}")
                    self.known_peers.add(p)

        elif msg_type == "chat":
            print(f"[{self.name}] chat from {data.get('from')}: {data.get('text')}")

        # ----ricart- agrawla: incoming request ----
        elif msg_type == "cs_request":
            requester = data.get("from")
            req_ts = data.get("timestamp")
            self.log_event(f"RA: got REQUEST from {requester} (ts={req_ts})")

            
            have_priority = (
                self.state == "HELD"
                or (
                    self.state == "WANTED"
                    and self.request_ts is not None
                    and (self.request_ts, self.name) < (req_ts, requester)
                )
            )

            if have_priority:
                # Defer reply until we exit the critical section
                self.deferred_replies.add(src_ws)
                self.log_event(f"RA: deferring REPLY to {requester}")
            else:
                # Reply immediately
                await self.send_message({"type": "cs_reply", "from": self.name}, ws=src_ws)
                self.log_event(f"RA: immediate REPLY to {requester}")

        # -- ricart-agrawla: incoming reply ----
        elif msg_type == "cs_reply":
            # This peer has granted us permission
            if src_ws in self.replies_pending:
                self.replies_pending.discard(src_ws)
                self.log_event(
                    f"RA: got REPLY from {data.get('from')} "
                    f"(pending={len(self.replies_pending)})"
                )

        elif msg_type == "cs_release":
            self.log_event(f"RA: {data.get('from')} released CS")


        # gossip: forward to other peers
        await self.broadcast(data, exclude=src_ws)

    async def send_message(self, data: dict, ws=None):

        data = dict(data)

        ts = self.tick()

        if "timestamp" not in data:
            data["timestamp"] = ts

        self.log_event(f"send {data.get('type')} -> ts={data['timestamp']}")

        if "id" not in data:
            data = dict(data)
            data["id"] = str(uuid.uuid4())
            self.seen_messages.add(data["id"])

        payload = json.dumps(data)

        if ws is not None:
            try:
                await ws.send(payload)
            except Exception:
                pass
        else:
            await self.broadcast(data)

    async def broadcast(self, data: dict, exclude=None):
        data = dict(data)


        if "timestamp" in data:
            self.tick()
        else:
            ts = self.tick()
            data["timestamp"] = ts

        self.log_event(f"broadcast {data.get('type')} -> ts={data['timestamp']}")
        


        if "id" not in data:
            data = dict(data)
            data["id"] = str(uuid.uuid4())
            self.seen_messages.add(data["id"])

        payload = json.dumps(data)
        for ws in list(self.connections):
            if ws is exclude:
                continue
            try:
                await ws.send(payload)
            except Exception:
                self.connections.discard(ws)

    async def request_cs(self):
        if self.state != "RELEASED":
            return
        
        self.state = "WANTED"
        self.request_ts = self.tick()

        
        self.log_event(f"RA: REQUEST_CS ts={self.request_ts}")

        self.replies_pending = set(self.connections)

        await self.broadcast({
            "type": "cs_request",
            "from": self.name,
            "timestamp": self.request_ts,
        })

        while self.replies_pending:
            await asyncio.sleep(0.1)

        self.state = "HELD"
        self.log_event("RA: ENTER_CS")

    async def release_cs(self):
       
        if self.state != "HELD":
            return

        self.log_event("RA: LEAVE_CS")
        self.state = "RELEASED"
        self.request_ts = None

        for ws in list(self.deferred_replies):
            await self.send_message({"type": "cs_reply", "from": self.name}, ws=ws)
            self.deferred_replies.discard(ws)

        await self.broadcast({
            "type": "cs_release",
            "from": self.name,
        })

    #period chat for demo
    async def periodic_chat(self, interval: float):
        while True:
            await asyncio.sleep(interval)
            await self.request_cs()

            text = f"hello from {self.name}"
            print(f"[{self.name}] broadcasting: {text}")
            await self.send_chat(text)

            await asyncio.sleep(1.0)

            await self.release_cs()

    async def run(self, chat_interval: float):
        await self.start_server()
        await self.connect_to_peers()
        asyncio.create_task(self.periodic_chat(chat_interval))
        await asyncio.Future()  # keep running forever


async def main():
    parser = argparse.ArgumentParser(description="P2P peer node")
    parser.add_argument("--name", required=True, help="Logical name of this peer")
    parser.add_argument("--port", type=int, required=True, help="Local port to listen on")
    parser.add_argument(
        "--peer",
        action="append",
        default=[],
        help="WebSocket URL of another peer, e.g. ws://localhost:9001 (can repeat)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Seconds between automatic chat broadcasts",
    )
    args = parser.parse_args()

    node = PeerNode(name=args.name, port=args.port, peer_urls=args.peer)
    await node.run(chat_interval=args.interval)


if __name__ == "__main__":
    asyncio.run(main())
