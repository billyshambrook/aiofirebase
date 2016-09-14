"""Test aiofirebase package."""
from unittest import mock

import asynctest
import pytest

import aiofirebase


class AsyncContextManagerMock(mock.Mock):
    """Mock async context manager."""

    async def __aenter__(self, *args, **kwargs):
        async def default(*args, **kwargs):
            return type(self)(*args, **kwargs)

        return await self.__dict__.get('__aenter__', default)(*args, **kwargs)

    async def __aexit__(self, *args, **kwargs):
        async def default(*args, **kwargs):
            return None

        return await self.__dict__.get('__aexit__', default)(*args, **kwargs)


class AsyncIterWrapper:
    """Async wrapper for synchronous iterables."""

    def __init__(self, iterable):
        """Initialize the wrapper with some iterable."""
        self._iterable = iter(iterable)

    async def __aiter__(self):
        """Return self."""
        return self

    async def __anext__(self):
        """Fetch the next value from the iterable."""
        try:
            return next(self._iterable)
        except StopIteration as exc:
            raise StopAsyncIteration() from exc


@pytest.fixture
def http_client():
    """A FirebaseHTTP instance with the ClientSession.request mocked."""
    client = aiofirebase.FirebaseHTTP('http://mydatabase')

    mock_response = mock.Mock()
    mock_response.status = 200
    mock_response.json = asynctest.CoroutineMock()

    mock_response_ctx = AsyncContextManagerMock()
    mock_response_ctx.__aenter__ = asynctest.CoroutineMock(return_value=mock_response)

    client._session.request = mock.Mock(return_value=mock_response_ctx)

    return client


@pytest.mark.asyncio
async def test_request(http_client):
    """Test ClientSession.request arguments."""
    await http_client._request(method='GET')
    assert http_client._session.request.called
    http_client._session.request.assert_called_with('GET', 'http://mydatabase.json', data=None, params={})


@pytest.mark.asyncio
@pytest.mark.parametrize("test_input,expected", [
    (None, 'http://mydatabase.json'),
    ('app/users', 'http://mydatabase/app/users.json'),
    ('/app/users', 'http://mydatabase/app/users.json'),
    ('/app/users/', 'http://mydatabase/app/users.json'),
])
async def test_request_path(http_client, test_input, expected):
    """Test ClientSession.request arguments for different path values."""
    await http_client._request(method='GET', path=test_input)
    assert http_client._session.request.called
    http_client._session.request.assert_called_with('GET', expected, data=None, params={})


@pytest.mark.asyncio
async def test_request_value(http_client):
    """Test ClientSession.request arguments when a value is provided."""
    await http_client._request(method='POST', value={'hello': 'world'})
    assert http_client._session.request.called
    http_client._session.request.assert_called_with(
        'POST', 'http://mydatabase.json', data='{"hello": "world"}', params={})


@pytest.mark.asyncio
async def test_request_params(http_client):
    """Test ClientSession.request arguments when params are provided."""
    await http_client._request(method='POST', params={'hello': 'world'})
    assert http_client._session.request.called
    http_client._session.request.assert_called_with(
        'POST', 'http://mydatabase.json', data=None, params={'hello': 'world'})


@pytest.mark.asyncio
async def test_cancel_event():
    """Ensure exception is thrown when cancel event is received."""
    iterable = AsyncIterWrapper([b'event:cancel\n'])
    with pytest.raises(aiofirebase.StreamCancelled):
        await aiofirebase.FirebaseHTTP._iterate_over_stream(iterable=iterable, callback=None)


@pytest.mark.asyncio
async def test_auth_revoked_event():
    """Ensure exception is thrown when auth_revoked event is received."""
    iterable = AsyncIterWrapper([b'event:auth_revoked\n'])
    with pytest.raises(aiofirebase.StreamAuthRevoked):
        await aiofirebase.FirebaseHTTP._iterate_over_stream(iterable=iterable, callback=None)


@pytest.mark.asyncio
async def test_stream_events():
    """Ensure the callback is called on each event."""
    iterable = AsyncIterWrapper(
        [b'event:patch\n', b'data:{"hello": "world"}\n', b'\n', b'event:put\n', b'data:{"foo": "bar"}\n'])
    events = []

    async def on_event(event, data):
        events.append({event: data})

    await aiofirebase.FirebaseHTTP._iterate_over_stream(iterable=iterable, callback=on_event)
    assert events == [{'patch': {'hello': 'world'}}, {'put': {'foo': 'bar'}}]
