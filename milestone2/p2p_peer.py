import asyncio
import argparse
import json
import uuid
import websockets

class Transaction:
    """
    A transaction is like a temporary workspace where we collect changes
    before deciding to save them or throw them away.
    
    write_buffer explanation:
    - When the transaction wants to change a value, we don't update the
    real store right away.
    - Instead, we hold the change here in write_buffer for now.
    - Only if the transaction fully succeeds (everyone says "YES"),
    we apply these changes to the real store.
    - If the transaction fails, we just delete this buffer and nothing
    in the real store was changed.

    Example:
        txn_write("counter", +1)
        write_buffer becomes {"counter": 1}
        The real store is updated ONLY when we commit.
    """
    def __init__(self, txn_id: str, coordinator: str, expected_votes: int = 0):
        self.txn_id = txn_id
        self.coordinator = coordinator
        self.state = "ACTIVE"
        self.resources: set[str] = set()

        # Temporary changes for this transaction.
        # These are not applied to the real store yet.
        self.write_buffer: dict[str, int] = {}

        # Stores YES/NO responses from other nodes.
        self.votes: dict[str, bool] = {}
        self.expected_votes = expected_votes
    
class PeerNode:

    def __init__(self, name: str, port: int, peer_urls: list[str]):
        self.name = name
        self.port = port

        self.peer_urls = peer_urls
        
        self.known_peers: set[str] = set(peer_urls)  
        self.connections: set[websockets.WebSocketClientProtocol] = set()
        self.seen_messages: set[str] = set()

        # ---- simple replicated key/value state for transactions ----
        # This is the shared "data" all peers will update transactionally.
        self.store: dict[str, int] = {}          # e.g., {"shared_counter": 5}
        self.lock_table: dict[str, str] = {}     # resource_id -> holding txn_id
        self.transactions: dict[str, Transaction] = {}

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


# ---------- Transaction / lock helpers ----------

    def new_txn_id(self) -> str:
        """Use Lamport time + node name for a unique, ordered txn id."""
        ts = self.tick()
        return f"{ts}-{self.name}"

    def _acquire_lock(self, txn_id: str, resource_id: str) -> bool:
        """Strict 2PL: only one txn can hold the write lock at a time."""
        holder = self.lock_table.get(resource_id)
        if holder is None or holder == txn_id:
            self.lock_table[resource_id] = txn_id
            return True
        return False

    def _release_locks(self, txn_id: str) -> None:
        """Release all locks held by this transaction."""
        to_release = [res for res, holder in self.lock_table.items() if holder == txn_id]
        for res in to_release:
            del self.lock_table[res]

    def _apply_write_buffer(self, txn: Transaction) -> None:
        """
        Apply transactional writes to the replicated store.
        We treat writes as 'delta' increments on integer values.
        """
        for resource_id, delta in txn.write_buffer.items():
            prev = self.store.get(resource_id, 0)
            new_val = prev + delta
            self.store[resource_id] = new_val
            self.log_event(
                f"TXN {txn.txn_id} APPLY {resource_id}: {prev} -> {new_val}"
            )
        txn.write_buffer.clear()

# ---------- Public transaction API ----------

    def begin_transaction(self) -> str:
        """Begin a new transaction where this node is the coordinator."""
        txn_id = self.new_txn_id()
        txn = Transaction(txn_id, coordinator=self.name)
        self.transactions[txn_id] = txn
        self.log_event(f"TXN {txn_id} BEGIN (coordinator={self.name})")
        return txn_id

    def txn_write(self, txn_id: str, resource_id: str, delta: int) -> bool:
        """
        Buffer a write (increment) inside a transaction.
        Uses strict 2PL for concurrency control.
        """
        txn = self.transactions.get(txn_id)
        if txn is None or txn.state != "ACTIVE":
            return False

        if not self._acquire_lock(txn_id, resource_id):
            # lock conflict -> caller should abort
            self.log_event(f"TXN {txn_id} LOCK CONFLICT on {resource_id}")
            return False

        txn.resources.add(resource_id)
        current = txn.write_buffer.get(resource_id, 0)
        txn.write_buffer[resource_id] = current + delta
        self.log_event(f"TXN {txn_id} WRITE {resource_id} += {delta}")
        return True

    async def commit_transaction(self, txn_id: str, timeout: float = 20.0):
        """
        Coordinator side of a simple 2-Phase Commit:
        1) Broadcast PREPARE with writes
        2) Collect VOTEs
        3) If all YES -> COMMIT, else -> ABORT
        """
        txn = self.transactions.get(txn_id)
        if txn is None or txn.state != "ACTIVE":
            return

        # how many other participants do we expect votes from?
        txn.expected_votes = len(self.connections)
        txn.state = "PREPARED"

        prepare_msg = {
            "type": "txn_prepare",
            "from": self.name,
            "coordinator": self.name,
            "txn_id": txn_id,
            "writes": txn.write_buffer,
        }
        await self.broadcast(prepare_msg)

        # if there are no other peers, we can commit locally
        if txn.expected_votes == 0:
            self.log_event(f"TXN {txn_id} COMMIT (single node)")
            self._apply_write_buffer(txn)
            txn.state = "COMMITTED"
            self._release_locks(txn_id)
            return

        # wait for votes or timeout
        loop = asyncio.get_event_loop()
        start = loop.time()
        while len(txn.votes) < txn.expected_votes and (loop.time() - start) < timeout:
            await asyncio.sleep(0.1)

        all_yes = (
            txn.votes
            and len(txn.votes) == txn.expected_votes
            and all(txn.votes.values())
        )

        if all_yes:
            self.log_event(f"TXN {txn_id} COMMIT (votes={txn.votes})")
            self._apply_write_buffer(txn)
            txn.state = "COMMITTED"
            self._release_locks(txn_id)
            await self.broadcast({
                "type": "txn_commit",
                "from": self.name,
                "coordinator": self.name,
                "txn_id": txn_id,
            })
        else:
            self.log_event(f"TXN {txn_id} ABORT (votes={txn.votes})")
            await self.abort_transaction(txn_id, reason="vote failure or timeout")

    async def abort_transaction(self, txn_id: str, reason: str = ""):
        """
        Roll back a transaction: discard buffered writes and release locks.
        If we're the coordinator, broadcast an ABORT to participants.
        """
        txn = self.transactions.get(txn_id)
        if txn is None or txn.state in ("COMMITTED", "ABORTED"):
            return

        txn.state = "ABORTED"
        txn.write_buffer.clear()
        self._release_locks(txn_id)
        self.log_event(f"TXN {txn_id} ABORT local ({reason})")

        if txn.coordinator == self.name:
            await self.broadcast({
                "type": "txn_abort",
                "from": self.name,
                "coordinator": self.name,
                "txn_id": txn_id,
                "reason": reason,
            })


  # ---- server ----
    async def start_server(self):

        async def handler(ws):
            print(f"[{self.name}] inbound connection")

            try:
                # ------------------------------------------
                # 1. First message MUST be hello
                # ------------------------------------------
                raw = await ws.recv()
                data = json.loads(raw)
                msg_type = data.get("type")
                remote_name = data.get("from")

                if msg_type != "hello":
                    print(f"[{self.name}] inbound missing hello → closing")
                    await ws.close()
                    return

                # ------------------------------------------
                # 2. HARD inbound duplicate protection
                # ------------------------------------------
                for existing in list(self.connections):
                    if getattr(existing, "peer_name", None) == remote_name:
                        print(f"[{self.name}] closing duplicate inbound from {remote_name}")
                        await ws.close()
                        return

                # ------------------------------------------
                # 3. Register this inbound WS
                # ------------------------------------------
                ws.peer_name = remote_name
                self.connections.add(ws)
                self.log_event(f"inbound connection from {remote_name}")

                # ------------------------------------------
                # 4. Process hello immediately
                # ------------------------------------------
                await self.handle_message(raw, ws)

                # ------------------------------------------
                # 5. Main receive loop
                # ------------------------------------------
                async for raw in ws:
                    await self.handle_message(raw, ws)

            except websockets.exceptions.ConnectionClosed:
                pass

            finally:
                if ws in self.connections:
                    self.connections.discard(ws)
                print(f"[{self.name}] inbound connection closed")

        # Start WS server
        server = await websockets.serve(handler, host="", port=self.port)
        print(f"[{self.name}] listening on ws://localhost:{self.port}")
        return server


    # client
    async def connect_to_peer_loop(self, url):
        while True:
            try:
                print(f"[{self.name}] trying to connect to {url}")
                ws = await websockets.connect(url)

                # Send our hello
                await self.send_hello(ws)

                # Wait for hello from remote
                raw = await ws.recv()
                data = json.loads(raw)
                remote_name = data.get("from")

                if data.get("type") != "hello":
                    print(f"[{self.name}] outbound connection missing hello → closing")
                    await ws.close()
                    await asyncio.sleep(3)
                    continue

                # ------------------------------------------
                # Deduplicate outbound connections
                # ------------------------------------------
                duplicate = False
                for existing in list(self.connections):
                    if hasattr(existing, "peer_name") and existing.peer_name == remote_name:
                        print(f"[{self.name}] closing duplicate outbound connection to {remote_name}")
                        await ws.close()
                        duplicate = True
                        break

                if duplicate:
                    await asyncio.sleep(3)
                    continue

                # ------------------------------------------
                # Register outbound connection
                # ------------------------------------------
                ws.peer_name = remote_name
                self.connections.add(ws)
                self.log_event(f"connected to {remote_name}")

                # Process the remote hello
                await self.handle_message(raw, ws)

                # ------------------------------------------
                # Process all other messages from this socket
                # ------------------------------------------
                async for raw in ws:
                    await self.handle_message(raw, ws)

            except Exception as e:
                print(f"[{self.name}] connection to {url} failed: {e} (will retry)")
                await asyncio.sleep(3)

    async def connect_to_peers(self):
        def get_port(url):
            return int(url.split(":")[-1])

        my_port = self.port

        for url in self.peer_urls:
            peer_port = get_port(url)

            # IMPORTANT: Only connect to peers with lower port number
            if peer_port < my_port:
                print(f"[{self.name}] outbound allowed → {url}")
                asyncio.create_task(self.connect_to_peer_loop(url))
            else:
                print(f"[{self.name}] skipping outbound to {url} (higher port)")


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

# ---------- Transaction protocol messages ----------

# This keeps all message-type handling in one place AND 
# ensures these messages get processed before they're rebroadcast.
        elif msg_type == "txn_prepare":
            await self._handle_txn_prepare(data)

        elif msg_type == "txn_vote":
            self._handle_txn_vote(data)

        elif msg_type == "txn_commit":
            self._handle_txn_commit(data)

        elif msg_type == "txn_abort":
            self._handle_txn_abort(data)

        # gossip: forward to other peers
        await self.broadcast(data, exclude=src_ws)

    # ============================================================
    #  Transaction Message Handlers
    #  These respond to: txn_prepare, txn_vote, txn_commit, txn_abort
    # ============================================================

    async def _handle_txn_prepare(self, data: dict):
        """
        Another node is asking us to prepare a transaction.

        What we do here:
        1. Try to lock the resources they want to change.
        2. If we can lock all of them → we vote YES.
        3. If not → we vote NO.
        4. Store their changes in our write_buffer (but do not apply them yet).
        """

        txn_id = data["txn_id"]
        coordinator = data.get("coordinator")
        writes: dict[str, int] = data.get("writes", {})

        # If this is the first time we hear of this transaction, create a record for it.
        txn = self.transactions.get(txn_id)
        if txn is None:
            txn = Transaction(txn_id, coordinator=coordinator)
            self.transactions[txn_id] = txn

        # Try to lock every resource they want to modify.
        can_lock_all = True
        for resource_id in writes.keys():
            if not self._acquire_lock(txn_id, resource_id):
                can_lock_all = False
                break

        # If we can lock everything, save the writes.
        if can_lock_all:
            txn.state = "PREPARED"
            txn.resources.update(writes.keys())

            # Add the incoming "delta changes" into our write_buffer
            for resource_id, delta in writes.items():
                prev = txn.write_buffer.get(resource_id, 0)
                txn.write_buffer[resource_id] = prev + delta

            vote = True
        else:
            # Could not lock → must reject
            txn.state = "ABORTED"
            self._release_locks(txn_id)
            vote = False

        self.log_event(f"TXN {txn_id}: PREPARE from {coordinator} → vote={vote}")

        # Send vote back (gossiped)
        vote_msg = {
            "type": "txn_vote",
            "from": self.name,
            "coordinator": coordinator,
            "txn_id": txn_id,
            "vote": vote,
        }

        await self.broadcast(vote_msg)


    def _handle_txn_vote(self, data: dict):
        """
        Only the coordinator cares about votes.

        Here we store who voted YES/NO so the coordinator
        can later decide whether to COMMIT or ABORT.
        """

        coordinator = data.get("coordinator")

        # Ignore if we are not the coordinator.
        if coordinator != self.name:
            return

        txn_id = data["txn_id"]
        voter = data.get("from")
        vote = bool(data.get("vote"))

        txn = self.transactions.get(txn_id)
        if txn is None:
            return

        txn.votes[voter] = vote

        self.log_event(
            f"TXN {txn_id}: received vote from {voter} → {vote} "
            f"({len(txn.votes)}/{txn.expected_votes})"
        )


    def _handle_txn_commit(self, data: dict):
        """
        The coordinator decided to COMMIT the transaction.

        Steps:
        1. Apply the stored write_buffer changes to the real store.
        2. Mark transaction as COMMITTED.
        3. Release locks.
        """

        txn_id = data["txn_id"]
        txn = self.transactions.get(txn_id)

        # If we somehow missed the prepare stage, create a txn record
        if txn is None:
            txn = Transaction(txn_id, coordinator=data.get("coordinator", ""))
            self.transactions[txn_id] = txn

        # If already committed, nothing to do
        if txn.state == "COMMITTED":
            return

        self._apply_write_buffer(txn)
        txn.state = "COMMITTED"
        self._release_locks(txn_id)

        self.log_event(
            f"TXN {txn_id}: COMMIT from coordinator={data.get('coordinator')}"
        )


    def _handle_txn_abort(self, data: dict):
        """
        The coordinator has ABORTED the transaction.

        Steps:
        1. Throw away any temporary changes.
        2. Release locks.
        3. Mark the transaction as ABORTED.
        """

        txn_id = data["txn_id"]
        txn = self.transactions.get(txn_id)
        if txn is None:
            return

        # If already aborted, no work to do.
        if txn.state == "ABORTED":
            return

        # Delete uncommitted changes and unlock resources
        txn.write_buffer.clear()
        self._release_locks(txn_id)
        txn.state = "ABORTED"

        self.log_event(
            f"TXN {txn_id}: ABORT from coordinator={data.get('coordinator')} "
            f"(reason={data.get('reason')})"
        )

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
        """
        Every few seconds:
        1. Ask for the distributed lock (Ricart–Agrawala).
        2. Start a transaction that increments a shared counter.
        3. Try to commit the transaction.
        4. Broadcast a chat message showing the current store.
        """

        while True:
            await asyncio.sleep(interval)

            # 1. Get into critical section (distributed mutex)
            await self.request_cs()

            # 2. Begin a transaction and add a write
            txn_id = self.begin_transaction()
            ok = self.txn_write(txn_id, "shared_counter", delta=1)

            if ok:
                # 3. Try to commit across all peers
                await self.commit_transaction(txn_id)
            else:
                # If we could not get a lock, abort
                await self.abort_transaction(txn_id, reason="lock conflict")

            # 4. Send a chat message with the current shared data
            text = f"hello from {self.name}, store={self.store}"
            print(f"[{self.name}] broadcasting: {text}")
            await self.send_chat(text)

            await asyncio.sleep(1.0)

            # Leave critical section
            await self.release_cs()


    async def run(self, chat_interval: float):
        await self.start_server()
        await self.connect_to_peers()
        await asyncio.sleep(5)
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

    node = PeerNode(
        name=args.name, 
        port=args.port, 
        peer_urls=args.peer
    )
    
    await node.run(chat_interval=args.interval)


if __name__ == "__main__":
    asyncio.run(main())
