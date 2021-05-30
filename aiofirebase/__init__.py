"""aiofirebase package."""
import asyncio
import json
import posixpath

from aiohttp import ClientSession


class StreamCancelled(Exception):
    """Signals the stream has been cancelled."""


class StreamAuthRevoked(Exception):
    """Signals the stream has been cancelled due to the authentication being revoked."""


class FirebaseHTTP:
    """
    HTTP Client for Firebase.

    Args:
        base_url (str): URL to your data.
        auth (string): Auth key.
        loop (class:`asyncio.BaseEventLoop`): Loop.
    """

    def __init__(self, base_url, auth=None, loop=None):
        """Initialise the class."""
        self._loop = loop or asyncio.get_event_loop()
        self._base_url = base_url
        self._auth = auth
        self._session = ClientSession(loop=self._loop)

    async def close(self):
        """Gracefully close the session."""
        await self._session.close()

    async def get(self, *, path=None, params=None):
        """Perform a GET request."""
        return await self._request(method='GET', path=path, params=params)

    async def put(self, *, value, path=None, params=None):
        """Perform a put request."""
        return await self._request(method='PUT', value=value, path=path, params=params)

    async def post(self, *, value, path=None, params=None):
        """Perform a POST request."""
        return await self._request(method='POST', value=value, path=path, params=params)

    async def patch(self, *, value, path=None, params=None):
        """Perform a PATCH request."""
        return await self._request(method='PATCH', value=value, path=path, params=params)

    async def delete(self, *, path=None, params=None):
        """Perform a DELETE request."""
        return await self._request(method='DELETE', path=path, params=params)

    async def stream(self, *, callback, path=None):
        """Hook up to the EventSource stream."""
        url = posixpath.join(self._base_url, path) if path else self._base_url
        url += '.json'
        headers = {'accept': 'text/event-stream'}
        async with self._session.get(url, headers=headers, timeout=None) as resp:
            while True:
                await FirebaseHTTP._iterate_over_stream(resp.content, callback)

    @staticmethod
    async def _iterate_over_stream(iterable, callback):
        """Iterate over the EventSource stream and pass the event and data to the callback as and when we receive it."""
        async for msg in iterable:
            msg_str = msg.decode('utf-8').strip()

            if not msg_str:
                continue

            key, value = msg_str.split(': ', 1)

            if key == 'event' and value == 'cancel':
                raise StreamCancelled('The requested location is no longer allowed due to security/rules changes.')
            elif key == 'event' and value == 'auth_revoked':
                raise StreamAuthRevoked('The auth credentials has expired.')
            elif key == 'event':
                event = value
            elif key == 'data':
                await callback(event=event, data=json.loads(value))

    async def _request(self, *, method, value=None, path=None, params=None):
        """Perform a request to Firebase."""
        url = posixpath.join(self._base_url, path.strip('/')) if path else self._base_url
        url += '.json'
        data = json.dumps(value) if value else None
        async with self._session.request(method, url, data=data, params=params) as resp:
            assert resp.status == 200
            return await resp.json()
