import asyncio
import json


class LiveBroadcast:
    """Keep the latest transcript snapshot for one publisher and many viewers."""

    def __init__(self):
        self.active = False
        self.snapshot = None
        self.state = "waiting"
        self.event = 'data: {"state":"waiting","snapshot":null}\n\n'
        self.changed = asyncio.Event()

    def update(self, state):
        self.state = state
        self.event = f"data: {json.dumps({'state': state, 'snapshot': self.snapshot})}\n\n"
        self.changed.set()
        self.changed = asyncio.Event()

    def start(self):
        self.active = True
        self.snapshot = None
        self.update("live")

    def publish(self, snapshot):
        if snapshot.get("status") == "error":
            self.update("error")
            return
        self.snapshot = snapshot
        self.update("live")

    def release(self):
        self.active = False
        if self.state == "live":
            self.update("disconnected")

    async def events(self):
        previous = None
        while True:
            changed = self.changed
            if changed is not previous:
                previous = changed
                yield self.event
            try:
                await asyncio.wait_for(changed.wait(), timeout=15)
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"
