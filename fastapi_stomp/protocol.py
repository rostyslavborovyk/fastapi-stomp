import abc
import typing as t
import uuid

from fastapi_stomp.engine import AsyncStompEngine
from fastapi_stomp.exception import ProtocolError, AuthError
from fastapi_stomp.util import frames
from fastapi_stomp.util.frames import Frame, ErrorFrame, ReceiptFrame, VALID_COMMANDS

TAsyncStompEngine = t.TypeVar('TAsyncStompEngine', bound=AsyncStompEngine)


class AsyncSTOMP(abc.ABC, t.Generic[TAsyncStompEngine]):
    _engine: TAsyncStompEngine

    def __init__(self, engine: TAsyncStompEngine):
        self._engine: TAsyncStompEngine = engine

    async def _stomp(self, frame: Frame):
        await self._connect(frame)

    @abc.abstractmethod
    async def process_frame(self, frame: Frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def _connect(self, frame: Frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def _send(self, frame: Frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def _subscribe(self, frame: Frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def _unsubscribe(self, frame: Frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def _disconnect(self, frame: Frame):
        raise NotImplementedError


class AsyncSTOMP12(AsyncSTOMP, t.Generic[TAsyncStompEngine]):
    SUPPORTED_VERSION = '1.2'

    async def process_frame(self, frame: Frame):
        """
        Dispatches a received frame to the appropriate internal method.
        """
        cmd_method = frame.cmd.lower()

        if cmd_method not in VALID_COMMANDS:
            raise ProtocolError(f"Invalid STOMP command: {frame.cmd}")

        method = getattr(self, cmd_method, None) or getattr(self, f"_{cmd_method}", None)

        if not self._engine.connected and method not in (self._connect, self._stomp):
            raise ProtocolError("Not connected, send CONNECT frame first")

        if not method:
            raise ProtocolError(
                f"Invalid STOMP command: {frame.cmd}, server does not support specified command"
            )

        try:
            await method(frame)
        except Exception as e:
            self._engine.log.error("Error processing STOMP frame: %s" % e)
            self._engine.log.exception(e)
            try:
                await self._engine.connection.send_frame(ErrorFrame(str(e), str(e)))
            except Exception as e:  # pragma: no cover
                self._engine.log.error("Could not send error frame: %s" % e)
                self._engine.log.exception(e)
        else:
            if frame.headers.get('receipt') and method != self._connect:
                await self._engine.connection.send_frame(ReceiptFrame(
                    receipt=frame.headers.get('receipt')))

    async def _send(self, frame):
        """
        Handle the SEND command: Delivers a message to a queue or topic (default).
        """
        dest = frame.headers.get('destination')
        if not dest:
            raise ProtocolError('Missing destination for SEND command.')

        if dest.startswith('/queue/'):
            await self._engine.queue_manager.send(frame)
        else:
            await self._engine.topic_manager.send(frame)

    async def _connect(self, frame, response=None):
        await self._check_protocol(frame)

        # todo add heartbeat
        # heart_beat = frame.headers.get('heart-beat', '0,0')
        # if heart_beat:
        #     await self.enable_heartbeat(*map(int, heart_beat.split(',')), response=connected_frame)

        self._engine.log.debug("CONNECT")

        if token := frame.headers.get('token'):
            if not await self._engine.authenticator.authenticate_from_token(token):
                raise AuthError("Authentication from token failed")
        else:
            raise AuthError("Token is missing in the headers")

        self._engine.connected = True

        response = response or Frame(frames.CONNECTED)
        response.headers['session'] = uuid.uuid4()

        await self._engine.connection.send_frame(response)

    async def _subscribe(self, frame):
        if "id" not in frame.headers:
            raise ProtocolError("No 'id' specified for SUBSCRIBE command.")

        dest = frame.headers.get('destination')
        if not dest:
            raise ProtocolError('Missing destination for SUBSCRIBE command.')

        id = frame.headers["id"]
        if dest.startswith('/queue/'):
            await self._engine.queue_manager.subscribe(self._engine.connection, dest, id=id)
        else:
            await self._engine.topic_manager.subscribe(self._engine.connection, dest, id=id)

    async def _unsubscribe(self, frame):
        if "id" not in frame.headers:
            raise ProtocolError("No 'id' specified for UNSUBSCRIBE command.")

        dest = frame.headers.get('destination')
        if not dest:
            raise ProtocolError('Missing destination for UNSUBSCRIBE command.')

        id = frame.headers["id"]
        if dest.startswith('/queue/'):
            await self._engine.queue_manager.unsubscribe(self._engine.connection, dest, id=id)
        else:
            await self._engine.topic_manager.unsubscribe(self._engine.connection, dest, id=id)

    async def _disconnect(self, frame):
        """
        Handles the DISCONNECT command: Unbinds the connection.

        Clients are supposed to send this command, but in practice it should not be
        relied upon.
        """
        self._engine.log.debug("Disconnect")
        self._engine.unbind()

    async def _check_protocol(self, frame):
        client_versions = frame.headers.get('accept-version')
        if not client_versions:
            await self._engine.connection.send_frame(Frame(
                frames.ERROR,
                headers={'version': self.SUPPORTED_VERSION, 'content-type': frames.TEXT_PLAIN},
                body=f"No protocol version specified, specify 'accept-version' header"
            ))
        if self.SUPPORTED_VERSION not in set(client_versions.split(',')):
            await self._engine.connection.send_frame(Frame(
                    frames.ERROR,
                    headers={'version': self.SUPPORTED_VERSION, 'content-type': frames.TEXT_PLAIN},
                    body=f'Supported protocol versions are {self.SUPPORTED_VERSION}'
            ))
