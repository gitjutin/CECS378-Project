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

  # server 
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

        # loop prevention
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

        # gossip: forward to other peers
        await self.broadcast(data, exclude=src_ws)

    async def send_message(self, data: dict, ws=None):
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

    #period chat for demo
    async def periodic_chat(self, interval: float):
        while True:
            await asyncio.sleep(interval)
            text = f"hello from {self.name}"
            print(f"[{self.name}] broadcasting: {text}")
            await self.send_chat(text)

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
