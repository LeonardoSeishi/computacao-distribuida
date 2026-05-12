# interceptor.py
import socket
import threading
import requests
import time

from collections import OrderedDict


# CONFIGURAÇÃO
def carregar_config():
    config = {}
    try:
        with open('config.txt') as f:
            for linha in f:
                linha = linha.strip()

                if not linha or linha.startswith('#') or '=' not in linha:
                    continue

                chave, valor = linha.split('=', 1)
                config[chave.strip()] = valor.strip()
    except FileNotFoundError:
        pass
    return config

cfg = carregar_config()

BASE_URL = f"http://{cfg['SERVIDOR_HOST']}:{cfg['SERVIDOR_PORTA']}"

HOST = cfg['INTERCEPTADOR_HOST']
PORTA = int(cfg['INTERCEPTADOR_PORTA'])

CACHE_CAPACIDADE = int(cfg['CACHE_CAPACIDADE'])
CACHE_TTL = int(cfg['CACHE_TTL'])
REQUEST_TIMEOUT = int(cfg.get('REQUEST_TIMEOUT', 5))

CIRCUIT_BREAKER_FALHAS = int(cfg.get('CIRCUIT_BREAKER_FALHAS', 3))
CIRCUIT_BREAKER_TIMEOUT = int(cfg.get('CIRCUIT_BREAKER_TIMEOUT', 10))


# CACHE LRU + TTL
class CacheLRU:
    def __init__(self, capacidade, ttl):
        self.capacidade = capacidade
        self.ttl = ttl

        self.cache = OrderedDict()
        self.lock = threading.Lock()

    def get(self, chave):
        with self.lock:

            if chave not in self.cache:
                return None

            valor, timestamp = self.cache[chave]

            # verifica TTL
            if time.time() - timestamp > self.ttl:
                del self.cache[chave]
                return None

            # atualiza posição LRU
            self.cache.move_to_end(chave)

            return valor

    def set(self, chave, valor):
        with self.lock:

            # atualiza ordem caso exista
            if chave in self.cache:
                self.cache.move_to_end(chave)

            self.cache[chave] = (valor, time.time())

            # remove item menos recentemente usado
            if len(self.cache) > self.capacidade:
                removido = self.cache.popitem(last=False)
                print(f"[Cache] LRU REMOVE -> {removido[0]}")

    def invalidate(self, chave):
        with self.lock:
            self.cache.pop(chave, None)


cache = CacheLRU(
    CACHE_CAPACIDADE,
    CACHE_TTL
)


# CIRCUIT BREAKER
class CircuitBreakerAberto(Exception):
    pass


class CircuitBreaker:
    FECHADO = 'FECHADO'
    ABERTO = 'ABERTO'
    MEIO_ABERTO = 'MEIO_ABERTO'

    def __init__(self, limite_falhas, tempo_recuperacao):
        self.limite_falhas = limite_falhas
        self.tempo_recuperacao = tempo_recuperacao
        self.estado = self.FECHADO
        self.falhas = 0
        self.ultima_falha = None
        self.lock = threading.Lock()

    def pode_chamar(self):
        with self.lock:
            if self.estado == self.FECHADO:
                return True

            if self.estado == self.MEIO_ABERTO:
                return False

            tempo_passado = time.time() - self.ultima_falha
            if tempo_passado >= self.tempo_recuperacao:
                self.estado = self.MEIO_ABERTO
                print("[CircuitBreaker] MEIO_ABERTO -> testando servidor")
                return True

            return False

    def sucesso(self):
        with self.lock:
            if self.estado != self.FECHADO or self.falhas > 0:
                print("[CircuitBreaker] FECHADO -> servidor recuperado")

            self.estado = self.FECHADO
            self.falhas = 0
            self.ultima_falha = None

    def falha(self):
        with self.lock:
            self.falhas += 1
            self.ultima_falha = time.time()

            if self.estado == self.MEIO_ABERTO or self.falhas >= self.limite_falhas:
                self.estado = self.ABERTO
                print("[CircuitBreaker] ABERTO -> bloqueando chamadas ao servidor")
            else:
                print(f"[CircuitBreaker] Falha {self.falhas}/{self.limite_falhas}")


circuit_breaker = CircuitBreaker(
    CIRCUIT_BREAKER_FALHAS,
    CIRCUIT_BREAKER_TIMEOUT
)


def chamar_servidor_com_circuit_breaker(metodo, url, **kwargs):
    if not circuit_breaker.pode_chamar():
        raise CircuitBreakerAberto(
            "Circuit breaker aberto: servidor temporariamente indisponivel"
        )

    try:
        resp = requests.request(
            metodo,
            url,
            timeout=REQUEST_TIMEOUT,
            **kwargs
        )

        # Apenas falhas do servidor contam para abrir o circuito.
        if resp.status_code >= 500:
            resp.raise_for_status()

        circuit_breaker.sucesso()
        return resp

    except Exception:
        circuit_breaker.falha()
        raise


# PROCESSAMENTO DOS COMANDOS
def processar(comando, parametro):

    try:

        # ENCURTA
        if comando == 'ENCURTA':
            url = f"{BASE_URL}/shorten"
            try:
                resp = chamar_servidor_com_circuit_breaker('POST', url, json={'url': parametro})
                resp.raise_for_status()
                codigo = resp.json()['shortcode']

                # cache-aside
                cache.set(codigo, parametro)

                return f"OK {codigo}\n"
            except Exception as e:
                return f"ERRO {str(e)}\n"

        # RESOLVE
        elif comando == 'RESOLVE':
            # tenta cache primeiro
            url = cache.get(parametro)
            if url is not None:
                print(f"[Cache] HIT -> {parametro}")
                return f"OK {url}\n"
            print(f"[Cache] MISS -> {parametro}")

            # Se não estiver no cache, consulta o servidor
            url = f"{BASE_URL}/resolve/{parametro}"
            try:
                resp = chamar_servidor_com_circuit_breaker('GET', url)
                resp.raise_for_status()
                url = resp.json()['url']

                # adiciona ao cache
                cache.set(parametro, url)
                return f"OK {url}\n"
            except Exception as e:
                return f"ERRO {str(e)}\n"

        # REMOVE
        elif comando == 'REMOVE':

            url = f"{BASE_URL}/{parametro}"
            try:
                resp = chamar_servidor_com_circuit_breaker('DELETE', url)
                resp.raise_for_status()
                cache.invalidate(parametro)
                return f"OK\n"
            except Exception as e:
                return f"ERRO {str(e)}\n"

        else:
            return "ERRO Comando desconhecido\n"

    except Exception as e:
        return f"ERRO {str(e)}\n"


# CLIENTE TCP
def lidar_com_cliente(conn, addr):

    print(f"Cliente conectado: {addr}")

    try:

        while True:

            dados = conn.recv(4096).decode()

            if not dados:
                break

            for linha in dados.split('\n'):

                if not linha:
                    continue

                partes = linha.split(maxsplit=1)

                if len(partes) < 2:
                    conn.sendall(b"ERRO Formato invalido\n")
                    continue

                comando = partes[0].upper()
                parametro = partes[1]

                resposta = processar(comando, parametro)

                conn.sendall(resposta.encode())

    except Exception as e:
        print(f"Erro com {addr}: {e}")

    finally:
        conn.close()
        print(f"Cliente {addr} desconectado")


# SERVIDOR TCP
def iniciar():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORTA))
    server.listen(5)

    print(f"Interceptador escutando em {HOST}:{PORTA}")

    while True:
        conn, addr = server.accept()

        threading.Thread(
            target=lidar_com_cliente,
            args=(conn, addr),
            daemon=True
        ).start()


if __name__ == '__main__':
    iniciar()
