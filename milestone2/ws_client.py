import asyncio
import json
import argparse
import websockets

async def main():
    parser = argparse.ArgumentParser(description="Simple WebSocket client for Campus Notes P2P server")
    parser.add_argument("--url", default="ws://localhost:8765", help="WebSocket server URL")
    parser.add_argument("--course", required=True, help="Course ID, e.g. CECS327")
    parser.add_argument("--username", required = True)
    parser.add_argument("--chat", help="Send a chat message")
    parser.add_argument("--edit", help="Send a live edit action")
    args = parser.parse_args()

    async with websockets.connect(args.url) as ws:
        # join session
        await ws.send(json.dumps({
            "type": "join_session",
            "course_id": args.course,
            "username": args.username
        }))

        print(f"[CLIENT] Joined {args.course} as {args.username}")

        # optional chat
        if args.chat:
            await ws.send(json.dumps({
                "type": "chat_message",
                "course_id": args.course,
                "message": args.chat
            }))

            print(f"[CLIENT] Sent chat: {args.chat}")

        # optional edit
        if args.edit:
            await ws.send(json.dumps({
                "type": "live_edit",
                "course_id": args.course,
                "edit": {"action": args.edit, "text": f"{args.username} editing"}
            }))
            print(f"[CLIENT] Sent edit: {args.edit}")

        # Wait briefly to receive any broadcast responses
        try:
            while True:
                response = await asyncio.wait_for(ws.recv(), timeout=2)
                print(f"[SERVER] {response}")
        except asyncio.TimeoutError:
            pass  # exit after short idle period

if __name__ == "__main__":
    asyncio.run(main())
