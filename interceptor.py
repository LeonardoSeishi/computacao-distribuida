# interceptor.py  (versão mínima – proxy puro)
import socket
import threading
import requests

# CONFIGURAÇÃO (valores padrão + leitura opcional do config.txt)
def carregar_config():
    config = {
        'SERVIDOR_HOST': '127.0.0.1',
        'SERVIDOR_PORTA': '5000',
        'INTERCEPTADOR_PORTA': '9000',
    }
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
PORTA = int(cfg['INTERCEPTADOR_PORTA'])



# CACHE LRU COM TTL (PADRÃO CACHE-ASIDE)
'''class CacheLRU:
    """Cache com política LRU e expiração por TTL.
       Armazena pares (chave -> valor, timestamp) em OrderedDict."""
    def __init__(self, capacidade, ttl):
        self.capacidade = capacidade
        self.ttl = ttl
        self.cache = OrderedDict()

    def get(self, chave):
        """Retorna o valor se presente e não expirado, senão None."""
        if chave not in self.cache:
            return None
        valor, ts = self.cache[chave]
        if time.time() - ts > self.ttl:
            del self.cache[chave]            # expurga entrada vencida
            return None
        self.cache.move_to_end(chave)        # promove a mais recente (LRU)
        return valor

    def set(self, chave, valor):
        """Insere ou atualiza uma entrada."""
        if chave in self.cache:
            self.cache.move_to_end(chave)
        self.cache[chave] = (valor, time.time())
        if len(self.cache) > self.capacidade:
            self.cache.popitem(last=False)   # remove o mais antigo

    def invalidate(self, chave):
        """Remove a entrada (usado após DELETE)."""
        if chave in self.cache:
            del self.cache[chave]

cache = CacheLRU(CACHE_CAP, CACHE_TTL)'''



# CHAMADA HTTP AO SERVIDOR REST
def chamar_api(metodo, caminho, dados=None):
    """Encaminha a requisição ao servidor REST e devolve a resposta HTTP."""
    url = f"{BASE_URL}{caminho}"
    if metodo == 'POST':
        resp = requests.post(url, json=dados, timeout=5)
    elif metodo == 'GET':
        resp = requests.get(url, timeout=5)
    elif metodo == 'DELETE':
        resp = requests.delete(url, timeout=5)
    else:
        raise ValueError("Método inválido")
    resp.raise_for_status()
    return resp

# TRATAMENTO DE COMANDOS (simples repasse)
def processar(comando, parametro):
    """
    Traduz o comando do cliente em chamada HTTP e retorna a resposta formatada.
    Nesta versão NÃO há cache nem proteção contra falhas.
    """
    try:
        if comando == 'ENCURTA':
            resp = chamar_api('POST', '/shorten', {'url': parametro})
            codigo = resp.json()['shortcode']
            return f"OK {codigo}\n"

        elif comando == 'RESOLVE':
            # Cache-Aside
            #url = cache.get(parametro)          # tenta obter do cache para evitar chamada HTTP
            #if url is not None:
            #    print(f"[Cache] HIT para {parametro}")
            #    return f"OK {url}\n

            resp = chamar_api('GET', f'/resolve/{parametro}')
            url = resp.json()['url']
            return f"OK {url}\n"

        elif comando == 'REMOVE':
            chamar_api('DELETE', f'/{parametro}')
            #cache.invalidate(parametro)
            return "OK\n"

        else:
            return "ERRO Comando desconhecido\n"

    except Exception as e:
        return f"ERRO {str(e)}\n"

# ATENDIMENTO DE UM CLIENTE TCP
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
                cmd = partes[0].upper()
                param = partes[1]
                resposta = processar(cmd, param)
                conn.sendall(resposta.encode())
    except Exception as e:
        print(f"Erro com {addr}: {e}")
    finally:
        conn.close()
        print(f"Cliente {addr} desconectado")

# INICIALIZAÇÃO DO SERVIDOR TCP
def iniciar():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', PORTA))
    server.listen(5)
    print(f"Interceptador escutando na porta {PORTA}")
    while True:
        conn, addr = server.accept()
        threading.Thread(target=lidar_com_cliente, args=(conn, addr), daemon=True).start()

if __name__ == '__main__':
    iniciar()