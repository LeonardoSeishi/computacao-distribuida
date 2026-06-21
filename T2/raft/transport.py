import json
import logging
import socket
import threading
from typing import Callable, Dict

logger = logging.getLogger(__name__)

_RECV_BUF = 65536


class Transport:
    """
    TCP transport layer built on Berkeley Sockets.

    Each message is sent as a separate short-lived connection.
    The server side keeps the connection open long enough to optionally
    send a reply (used by client-request handlers), then closes it.

    on_message(raw_dict, reply_fn) — called for every incoming message.
      reply_fn(response_dict) sends a response on the same connection;
      Raft-internal RPCs never call reply_fn (their replies are new outbound
      connections to the sender's known address).
    """

    def __init__(self, host: str, port: int,on_message: Callable[[Dict, Callable], None]):
        self.host = host
        self.port = port
        self.on_message = on_message
        self._server_sock: socket.socket | None = None
        self._running = False

    # Lifecycle
    def start(self):
        self._running = True
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(32)
        t = threading.Thread(target=self._accept_loop, daemon=True,
                              name=f'transport-accept-{self.port}')
        t.start()
        logger.debug(f"Transport listening on {self.host}:{self.port}")

    def stop(self):
        self._running = False
        if self._server_sock:
            try:
                self._server_sock.close()
            except OSError:
                pass


    # Server side
    def _accept_loop(self):
        while self._running:
            try:
                conn, _ = self._server_sock.accept()
                threading.Thread(target=self._handle_conn, args=(conn,),
                                 daemon=True).start()
            except OSError:
                break

    def _handle_conn(self, conn: socket.socket):
        replied = [False]

        def reply_fn(response: Dict):
            if not replied[0]:
                replied[0] = True
                try:
                    conn.sendall(json.dumps(response).encode())
                except OSError:
                    pass

        try:
            data = b''
            conn.settimeout(5.0)
            while True:
                chunk = conn.recv(_RECV_BUF)
                if not chunk:
                    break
                data += chunk
            if data:
                msg = json.loads(data.decode())
                self.on_message(msg, reply_fn)
        except Exception as e:
            logger.debug(f"Connection handler error: {e}")
        finally:
            conn.close()


    # Client side (fire-and-forget)
    def send(self, host: str, port: int, msg: Dict) -> bool:
        """Send a message. Returns True on success, False if unreachable."""
        try:
            data = json.dumps(msg).encode()
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.5)
                s.connect((host, port))
                s.sendall(data)
            return True
        except Exception as e:
            logger.debug(f"send to {host}:{port} failed: {e}")
            return False

    def request(self, host: str, port: int, msg: Dict, timeout: float = 10.0) -> Dict | None:
        """Send a message and wait for a reply (used by quiz clients)."""
        try:
            data = json.dumps(msg).encode()
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                s.connect((host, port))
                s.sendall(data)
                s.shutdown(socket.SHUT_WR)
                reply = b''
                while True:
                    chunk = s.recv(_RECV_BUF)
                    if not chunk:
                        break
                    reply += chunk
            return json.loads(reply.decode()) if reply else None
        except Exception as e:
            logger.debug(f"request to {host}:{port} failed: {e}")
            return None
