import json
import logging
import socket
import threading
from typing import Callable, Dict

logger = logging.getLogger(__name__)

_RECV_BUF = 65536


class Transport:
    """
    Camada de transporte TCP com Berkeley Sockets.
    """

    def __init__(self, host: str, port: int,on_message: Callable[[Dict, Callable], None]):
        self.host = host
        self.port = port
        self.on_message = on_message
        self._server_sock: socket.socket | None = None
        self._running = False

    
    def start(self):
        self._running = True
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(32)
        t = threading.Thread(target=self._accept_loop, daemon=True,
                              name=f'transport-accept-{self.port}')
        t.start()
        logger.debug(f"Transporte escutando em {self.host}:{self.port}")

    def stop(self):
        self._running = False
        if self._server_sock:
            try:
                self._server_sock.close()
            except OSError:
                pass


    # Server-side
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
            logger.debug(f"Erro de conexão: {e}")
        finally:
            conn.close()


    # Client-side
    def send(self, host: str, port: int, msg: Dict) -> bool:
        """Envia mensagem. Retorna True se sucesso ou False se host inalcançável"""
        try:
            data = json.dumps(msg).encode()
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.5)
                s.connect((host, port))
                s.sendall(data)
            return True
        except Exception as e:
            logger.debug(f"Envio para {host}:{port} falhou: {e}")
            return False

    def request(self, host: str, port: int, msg: Dict, timeout: float = 10.0) -> Dict | None:
        """Envia uma mensagem e aguarda por resposta (usada para clientes no quiz)"""
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
            logger.debug(f"Requisição para {host}:{port} falhou: {e}")
            return None
