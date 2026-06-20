# cliente_lib.py
import socket

INTERCEPTADOR_HOST = '127.0.0.1'
INTERCEPTADOR_PORTA = 9000

def _comunicar(comando_e_parametro):
    """Envia comando, recebe e processa resposta."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((INTERCEPTADOR_HOST, INTERCEPTADOR_PORTA))
            s.sendall(comando_e_parametro.encode())
            resposta = s.recv(4096).decode().strip()
            return resposta
    except socket.timeout:
        return "ERRO Timeout na conexão"
    except ConnectionRefusedError:
        return "ERRO Conexão recusada - Interceptador está rodando?"
    except Exception as e:
        return f"ERRO {e}"

def encurta(url_original):
    """
    Encurta uma URL.
    Retorna (0, codigo_curto) em sucesso, (-1, mensagem_erro) em falha.
    """
    resp = _comunicar(f"ENCURTA {url_original}\n")
    if resp.startswith("OK "):
        return 0, resp[3:].strip()
    else:
        return -1, resp

def resolve(codigo_curto):
    """
    Resolve um código curto.
    Retorna (0, url_original) em sucesso, (-1, mensagem_erro) em falha.
    """
    resp = _comunicar(f"RESOLVE {codigo_curto}\n")
    if resp.startswith("OK "):
        return 0, resp[3:].strip()
    else:
        return -1, resp

def remove_url(codigo_curto):
    """
    Remove um mapeamento.
    Retorna 0 em sucesso, -1 em falha.
    """
    resp = _comunicar(f"REMOVE {codigo_curto}\n")
    return 0 if resp == "OK" else -1