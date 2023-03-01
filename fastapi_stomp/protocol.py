import abc
import typing as t
import uuid

from fastapi_stomp.engine import AsyncStompEngine
from fastapi_stomp.exception import ProtocolError, AuthError
from fastapi_stomp.util import frames
from fastapi_stomp.util.frames import Frame, ErrorFrame, ReceiptFrame, VALID_COMMANDS

TAsyncStompEngine = t.TypeVar('TAsyncStompEngine', bound=AsyncStompEngine)


class AsyncSTOMP(abc.ABC, t.Generic[TAsyncStompEngine]):
    engine: TAsyncStompEngine

    def __init__(self, engine: TAsyncStompEngine):
        self.engine: TAsyncStompEngine = engine

    async def stomp(self, frame: Frame):
        await self.connect(frame)

    @abc.abstractmethod
    async def process_frame(self, frame: Frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def connect(self, frame: Frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def send(self, frame: Frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def subscribe(self, frame: Frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def unsubscribe(self, frame: Frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def disconnect(self, frame: Frame):
        raise NotImplementedError


class AsyncSTOMP12(AsyncSTOMP, t.Generic[TAsyncStompEngine]):
    SUPPORTED_VERSION = '1.2'

    async def process_frame(self, frame: Frame):
        """
        Dispatches a received frame to the appropriate internal method.
        """
        cmd_method = frame.cmd.lower()

        if cmd_method not in VALID_COMMANDS:
            raise ProtocolError("Invalid STOMP command: {}".format(frame.cmd))

        method = getattr(self, cmd_method, None)

        if not self.engine.connected and method not in (self.connect, self.stomp):
            raise ProtocolError("Not connected.")

        try:
            await method(frame)
        except Exception as e:
            self.engine.log.error("Error processing STOMP frame: %s" % e)
            self.engine.log.exception(e)
            try:
                await self.engine.connection.send_frame(ErrorFrame(str(e), str(e)))
            except Exception as e:  # pragma: no cover
                self.engine.log.error("Could not send error frame: %s" % e)
                self.engine.log.exception(e)
        else:
            if frame.headers.get('receipt') and method != self.connect:
                await self.engine.connection.send_frame(ReceiptFrame(
                    receipt=frame.headers.get('receipt')))

    async def send(self, frame):
        """
        Handle the SEND command: Delivers a message to a queue or topic (default).
        """
        dest = frame.headers.get('destination')
        if not dest:
            raise ProtocolError('Missing destination for SEND command.')

        if dest.startswith('/queue/'):
            await self.engine.queue_manager.send(frame)
        else:
            await self.engine.topic_manager.send(frame)

    async def connect(self, frame, response=None):
        await self._check_protocol(frame)

        # todo add heartbeat
        # heart_beat = frame.headers.get('heart-beat', '0,0')
        # if heart_beat:
        #     await self.enable_heartbeat(*map(int, heart_beat.split(',')), response=connected_frame)

        self.engine.log.debug("CONNECT")

        if token := frame.headers.get('token'):
            if not await self.engine.authenticator.authenticate_from_token(token):
                raise AuthError("Authentication from token failed")
        else:
            raise AuthError("Token is missing in the headers")

        self.engine.connected = True

        response = response or Frame(frames.CONNECTED)
        response.headers['session'] = uuid.uuid4()

        await self.engine.connection.send_frame(response)

    async def subscribe(self, frame):
        if "id" not in frame.headers:
            raise ProtocolError("No 'id' specified for SUBSCRIBE command.")

        dest = frame.headers.get('destination')
        if not dest:
            raise ProtocolError('Missing destination for SUBSCRIBE command.')

        id = frame.headers.get('id')
        if dest.startswith('/queue/'):
            await self.engine.queue_manager.subscribe(self.engine.connection, dest, id=id)
        else:
            await self.engine.topic_manager.subscribe(self.engine.connection, dest, id=id)

    async def unsubscribe(self, frame):
        if "id" not in frame.headers:
            raise ProtocolError("No 'id' specified for UNSUBSCRIBE command.")

        dest = frame.headers.get('destination')
        if not dest:
            raise ProtocolError('Missing destination for UNSUBSCRIBE command.')

        id = frame.headers.get('id')
        if dest.startswith('/queue/'):
            await self.engine.queue_manager.unsubscribe(self.engine.connection, dest, id=id)
        else:
            await self.engine.topic_manager.unsubscribe(self.engine.connection, dest, id=id)

    async def disconnect(self, frame):
        """
        Handles the DISCONNECT command: Unbinds the connection.

        Clients are supposed to send this command, but in practice it should not be
        relied upon.
        """
        self.engine.log.debug("Disconnect")
        self.engine.unbind()

    async def _check_protocol(self, frame):
        client_versions = frame.headers.get('accept-version')
        if not client_versions:
            await self.engine.connection.send_frame(Frame(
                frames.ERROR,
                headers={'version': self.SUPPORTED_VERSION, 'content-type': frames.TEXT_PLAIN},
                body=f"No protocol version specified, specify 'accept-version' header"
            ))
        if self.SUPPORTED_VERSION not in set(client_versions.split(',')):
            await self.engine.connection.send_frame(Frame(
                    frames.ERROR,
                    headers={'version': self.SUPPORTED_VERSION, 'content-type': frames.TEXT_PLAIN},
                    body=f'Supported protocol versions are {self.SUPPORTED_VERSION}'
            ))
