import typing as t

SEND = 'SEND'
CONNECT = 'CONNECT'
MESSAGE = 'MESSAGE'
ERROR = 'ERROR'
CONNECTED = 'CONNECTED'
SUBSCRIBE = 'SUBSCRIBE'
UNSUBSCRIBE = 'UNSUBSCRIBE'
BEGIN = 'BEGIN'
COMMIT = 'COMMIT'
ABORT = 'ABORT'
ACK = 'ACK'
NACK = 'NACK'
DISCONNECT = 'DISCONNECT'

VALID_COMMANDS = [
    'message',
    'connect',
    'connected',
    'error',
    'send',
    'subscribe',
    'unsubscribe',
    'begin',
    'commit',
    'abort',
    'ack',
    'disconnect',
    'nack',
]

TEXT_PLAIN = 'text/plain'


def parse_from_text(message: str):
    """
    Called to unpack a STOMP message into a dictionary.
    """
    headers = {}
    body = []

    breakdown = message.split('\n')

    # Get the message command:
    cmd = breakdown[0]
    breakdown = breakdown[1:]

    def head_data(f):
        # find the first ':' everything to the left of this is a
        # header, everything to the right is data:
        index = f.find(':')
        if index:
            header = f[:index].strip()
            data = f[index+1:].strip()
            headers[header.strip()] = data.strip()

    def body_data(f):
        f = f.strip()
        if f:
            body.append(f)

    # Recover the header fields and body data
    handler = head_data
    for field in breakdown:
        if field.strip() == '':
            # End of headers, if body data next.
            handler = body_data
            continue

        handler(field)

    body = "".join(body)
    body = body.replace('\x00', '')
    return cmd, headers, body


class Frame:
    """
    A STOMP frame (or message).

    :param cmd: the protocol command
    :param headers: a map of headers for the frame
    :param body: the content of the frame.
    """

    def __init__(self, cmd, headers=None, body=None):
        self.cmd = cmd
        self.headers = headers or {}
        self.body = body or ''

    def __str__(self):
        return '{{cmd={0},headers=[{1}],body={2}}}'.format(
            self.cmd,
            self.headers,
            str(self.body),
        )

    def __eq__(self, other):
        """ Override equality checking to test for matching command, headers, and body. """
        return all([isinstance(other, Frame),
                    self.cmd == other.cmd,
                    self.headers == other.headers,
                    self.body == other.body])

    @property
    def transaction(self):
        return self.headers.get('transaction')

    @classmethod
    def from_text(cls, text: str) -> 'Frame':
        cmd, headers, body = parse_from_text(text)
        return cls(cmd, headers=headers, body=body)

    def pack(self):
        """
        Create a string representation from object state.
        """

        self.headers.setdefault('content-length', len(self.body))

        # Convert and append any existing headers to a string as the
        # protocol describes.
        header_parts = ("{0}:{1}\n".format(key, value)
                        for key, value in self.headers.items())

        return f"{self.cmd}\n{''.join(header_parts)}\n{self.body}\x00".encode('utf-8')


class ConnectedFrame(Frame):
    """ A CONNECTED server frame (response to CONNECT).
    """

    def __init__(self, session: str, extra_headers=None):
        super(ConnectedFrame, self).__init__(
            cmd='connected', headers=extra_headers or {})
        self.headers['session'] = session


class HeaderValue:
    """
    An descriptor class that can be used when a calculated header value is needed.

    For example, to use this class to generate the content-length header:

        >>> body = 'asdf'
        >>> headers = {}
        >>> headers['content-length'] = HeaderValue(calculator=lambda: len(body))
        >>> str(headers['content-length'])
        '4'
    """

    def __init__(self, calculator: t.Callable[[], int | str]):
        if not callable(calculator):
            raise ValueError("Non-callable param: %s" % calculator)
        self.calc = calculator

    def __get__(self, obj, objtype):
        return self.calc()

    def __str__(self):
        return str(self.calc())

    def __set__(self, obj, value):
        self.calc = value

    def __repr__(self):
        return '<%s calculator=%s>' % (self.__class__.__name__, self.calc)


class ErrorFrame(Frame):
    """ An ERROR server frame. """

    def __init__(self, message, body=None, extra_headers=None):
        super(ErrorFrame, self).__init__(cmd='error',
                                         headers=extra_headers or {}, body=body)
        self.headers['message'] = message
        self.headers[
            'content-length'] = HeaderValue(calculator=lambda: len(self.body))

    def __repr__(self):
        return '<%s message=%r>' % (self.__class__.__name__, self.headers['message'])


class ReceiptFrame(Frame):
    """ A RECEIPT server frame. """

    def __init__(self, receipt: str, extra_headers=None):
        super(ReceiptFrame, self).__init__(
            'RECEIPT', headers=extra_headers or {})
        self.headers['receipt-id'] = receipt
