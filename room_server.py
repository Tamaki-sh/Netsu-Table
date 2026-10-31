"""Netsu room-code relay. Run with Python 3.10+ and websockets 16.0."""
import asyncio, base64, json, os, secrets, time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed
from http import HTTPStatus

# Plain HTTP requests (hosting health checks, browser visits) get a short OK instead of an error.
def http_ok(connection, request):
    if request.headers.get('Upgrade','').lower()!='websocket':
        return connection.respond(HTTPStatus.OK, 'Netsu room service is running.\n')
    return None
PROTOCOL = 'netsu-rooms-1'
ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'
MAX_ROOMS = int(os.getenv('MAX_ROOMS', '500'))
MAX_CONNECTIONS = int(os.getenv('MAX_CONNECTIONS', '2048'))
MAX_PACKET = 262144
@dataclass
class Room:
    code: str
    mode: str
    length: int
    peers: dict = field(default_factory=dict)
    next_peer: int = 2
    locked: bool = False
class Relay:
    def __init__(self):
        self.rooms = {}
        self.connections = 0
        self.per_ip = defaultdict(int)
        self.attempts = defaultdict(deque)
    async def send(self, ws, value):
        if ws is not None:
            try: await asyncio.wait_for(ws.send(json.dumps(value, separators=(',', ':'))), 5)
            except (ConnectionClosed, asyncio.TimeoutError): pass
    async def error(self, ws, message):
        await self.send(ws, {'type':'error','message':message})
    async def handler(self, ws):
        ip = ws.remote_address[0] if ws.remote_address else 'unknown'
        if self.connections >= MAX_CONNECTIONS or self.per_ip[ip] >= 32:
            await ws.close(1013, 'Server busy'); return
        self.connections += 1; self.per_ip[ip] += 1
        room = None; peer_id = 0; times = deque()
        try:
            async for raw in ws:
                if not isinstance(raw, str):
                    await ws.close(1003, 'Text frames required'); break
                now = time.monotonic()
                while times and now-times[0] > 1: times.popleft()
                times.append(now)
                if len(times)>80:
                    await ws.close(1008, 'Too many messages'); break
                try: packet = json.loads(raw)
                except (ValueError, TypeError):
                    await self.error(ws, 'Invalid request.'); continue
                if not isinstance(packet, dict):
                    await self.error(ws, 'Invalid request.'); continue
                kind = packet.get('type')
                if kind in ('create','join'):
                    if room is not None:
                        await self.error(ws, 'Already in a room.'); continue
                    recent=self.attempts[ip]
                    while recent and now-recent[0]>60:recent.popleft()
                    if len(recent)>=40:
                        await self.error(ws,'Too many room requests; try again in a minute.'); continue
                    recent.append(now)
                    if packet.get('protocol') != PROTOCOL:
                        await self.error(ws,'Use the same current game build.'); continue
                    if kind=='create':
                        mode=packet.get('mode')
                        if mode not in ('riichi','pokajan','duel') or packet.get('length',8) not in (3,4,6,8):
                            await self.error(ws,'Invalid game mode.'); continue
                        if len(self.rooms)>=MAX_ROOMS:
                            await self.error(ws,'Room service is full; try later.'); continue
                        code=''.join(secrets.choice(ALPHABET) for _ in range(8))
                        while code in self.rooms:code=''.join(secrets.choice(ALPHABET) for _ in range(8))
                        room=Room(code,mode,int(packet.get('length',8)));peer_id=1;room.peers[1]=ws;self.rooms[code]=room
                    else:
                        code=str(packet.get('code','')).upper().replace('-','').replace(' ','')
                        candidate=self.rooms.get(code)
                        if candidate is None:
                            await self.error(ws,'Room code not found. Check the code and server address.'); continue
                        if candidate.locked:
                            await self.error(ws,'That match has already started.'); continue
                        if len(candidate.peers)>=(2 if candidate.mode=='duel' else (3 if candidate.length in (3,6) else 4)):
                            await self.error(ws,'That room is full.'); continue
                        room=candidate;peer_id=room.next_peer;room.next_peer+=1;room.peers[peer_id]=ws
                        await self.send(room.peers.get(1),{'type':'peer_joined','peer':peer_id})
                    await self.send(ws,{'type':'welcome','peer':peer_id,'code':room.code,'mode':room.mode,'length':room.length})
                elif room is None:
                    await self.error(ws,'Create or join a room first.')
                elif kind=='lock' and peer_id==1:
                    room.locked=bool(packet.get('locked',True))
                elif kind=='kick' and peer_id==1:
                    target=packet.get('target')
                    if isinstance(target,int) and target>1 and target in room.peers:
                        await room.peers[target].close(1008,'Removed by host')
                elif kind=='relay':
                    target=packet.get('target');payload=packet.get('payload')
                    if not isinstance(target,int) or not isinstance(payload,str) or len(payload)>MAX_PACKET-256:
                        await self.error(ws,'Invalid message.');continue
                    if (peer_id!=1 and target!=1) or target==peer_id or target not in room.peers:
                        await self.error(ws,'Invalid recipient.');continue
                    try:base64.b64decode(payload,validate=True)
                    except (ValueError,TypeError):
                        await self.error(ws,'Invalid payload.');continue
                    # Sender identity is assigned by this service, never trusted from the client.
                    await self.send(room.peers[target],{'type':'relay','sender':peer_id,'payload':payload})
                else:
                    await self.error(ws,'Unsupported request.')
        except ConnectionClosed: pass
        finally:
            self.connections-=1;self.per_ip[ip]-=1
            if not self.per_ip[ip]:self.per_ip.pop(ip,None)
            if room is not None:
                room.peers.pop(peer_id,None)
                if peer_id==1:
                    self.rooms.pop(room.code,None)
                    remaining=list(room.peers.values());room.peers.clear()
                    for client in remaining:
                        await self.send(client,{'type':'room_closed','message':'The host left. This room has closed.'})
                        await client.close(1000,'Host left')
                elif 1 in room.peers:
                    await self.send(room.peers[1],{'type':'peer_left','peer':peer_id})
            if not self.per_ip.get(ip):self.attempts.pop(ip,None)
async def main():
    host=os.getenv('HOST','0.0.0.0');port=int(os.getenv('PORT','8787'));relay=Relay()
    async with serve(relay.handler,host,port,process_request=http_ok,max_size=MAX_PACKET,max_queue=32,compression=None,ping_interval=20,ping_timeout=20,close_timeout=3):
        print(f'Netsu room relay listening on {host}:{port}',flush=True)
        await asyncio.Future()
if __name__=='__main__':asyncio.run(main())
