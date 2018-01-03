import asyncio
from asyncio import get_event_loop
import itertools
import json
import logging

import websockets


class Socket:

    def __init__(self, websock):
        self.websock = websock
        self.ack = {}
        self.sub = {}
        self.count = itertools.count(1)
        self.authenticated = False
        self.authtoken = get_event_loop().create_future()
        self.consumer = get_event_loop().create_task(self.consumer())

    async def emit(self, event, data=None):
        message = {'event': event, 'data': data}
        await self.websock.send(json.dumps(message))

    async def emitack(self, event, data=None):
        cid = next(self.count)
        self.ack[cid] = get_event_loop().create_future()
        message = {'event': event, 'data': data, 'cid': cid}
        await self.websock.send(json.dumps(message))
        return await self.ack[cid]

    async def handshake(self, token=None):
        data = await self.emitack('#handshake', {} if token is None else {'authToken': token})
        logging.info('Handshake complete: ' + str(data))
        if data['isAuthenticated']:
            self.authenticated = True
            self.authtoken.set_result(token)
        return data

    async def subscribe(self, *channels):
        queue = asyncio.Queue()
        for each in channels:
            self.sub[each] = queue
            get_event_loop().create_task(self.emit('#subscribe', {'channel': each}))
        while True:
            yield await queue.get()

    async def consumer(self):
        async for message in self.websock:
            if message == '#1':
                await self.websock.send('#2')
                continue
            try:
                message = json.loads(message)
                if 'rid' in message:
                    rid = message['rid']
                    self.ack.pop(rid).set_result(message.get('data'))
                elif 'event' in message:
                    event = message['event']
                    if event == '#disconnect':
                        logging.info('Stopping on #disconnect')
                        return
                    elif event == '#publish':
                        data = message['data']
                        channel = data['channel']
                        await self.sub[channel].put(data)
                    elif event == '#setAuthToken':
                        logging.info('Authentication complete')
                        self.authenticated = True
                        self.authtoken.set_result(message.get('data', {}).get('token'))
                    else:
                        logging.warning('Unknown event: ' + str(event))
                else:
                    logging.warning('Missing event and rid: ' + str(message))
            except Exception:
                logging.exception('Failed to consume message')


class Connect:

    def __init__(self, uri, **kwargs):
        self._wsconnect = websockets.connect(uri, **kwargs)
        self._authtoken = None

    def authtoken(self, authtoken):
        self._authtoken = authtoken
        return self

    def __await__(self):
        ws = yield from self._wsconnect.__await__()
        sc = Socket(ws)
        yield from sc.handshake(self._authtoken).__await__()
        return sc

    async def __aenter__(self):
        ws = await self._wsconnect.__aenter__()
        sc = Socket(ws)
        await sc.handshake(self._authtoken)
        return sc

    async def __aexit__(self, *exception):
        return await self._wsconnect.__aexit__(*exception)


connect = Connect
