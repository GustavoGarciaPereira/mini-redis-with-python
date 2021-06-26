from gevent import socket
from gevent.pool import Pool
from geven.server import StreamServer

from collections import namedtuple
from io import BytesIO
from socket import error as socket_error


class CommandError(Exception):

    pass


class Disconnect(Exception):

    pass


Error = namedtuple('Error', ('message',))


class ProtocolHandler(object):
    def __init__(self):
        self.handlers = {
            '+': self.handle_simple_string,
            '-': self.handle_error,
            ':': self.handle_integer,
            '$': self.handle_string,
            '*': self.handle_array,
            '%': self.handle_dict}

    def handle_request(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte:
            raise Disconnect()

        try:
            return self.handlers[first_byte](socket_file)
        except KeyError:
            raise CommandError('bad request')

    def handle_simple_string(self, socket_file):
        pass

    def handle_error(self, socket_file):
        pass

    def handle_integer(self, socket_file):
        pass

    def handle_string(self, socket_file):
        pass

    def handle_array(self, socket_file):
        pass

    def handle_dict(self, socket_file):
        pass


    def write_request(self, socket_file, data):

        pass


class Server(object):
    def __init__(self, host='127.0.0.1', port=31337, max_clients=64):
        self._pool = Pool(max_clients)
        self._server = StreamServer(
            (host, port),
            self.connection_handle,
            spawn=self._pool)

        self._protocol = ProtocolHandler()
        self._kv = {}

    def connection_handle(self, conn, address):
        socket_file = conn.makefile('rwb')

        while True:
            try:
                data = self._protocol.handle_request(socket_file)
            except Disconnect:
                break

            try:
                resp = self.get_response(data)
            except CommandError as exc:
                resp = Error(exc.args[0])

            self._protocol.write_request(socket_file, resp)

    def response(self, data):
        pass

    def run(self):
        self._server.server_forever()