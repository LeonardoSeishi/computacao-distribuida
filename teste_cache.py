from cliente_lib import resolve
import requests

# Teste para demonstrar cache MISS seguido de cache HIT.
# A URL é criada diretamente no servidor REST para que o código exista,
# mas ainda não esteja armazenado no cache do interceptador.
# Depois, o mesmo código é resolvido 3 vezes via interceptador.


# Cria a URL diretamente no servidor REST, sem passar pelo interceptador
resp = requests.post(
    "http://127.0.0.1:5000/shorten",
    json={"url": "https://ufsc.br"}
)

codigo = resp.json()["shortcode"]
print(f"Código criado direto no servidor REST: {codigo}")

# Primeira resolução via interceptador
print("\nPrimeira resolução via interceptador:")
status, url = resolve(codigo)
print(f"Status: {status}")
print(f"URL: {url}")

# Segunda resolução via interceptador
print("\nSegunda resolução via interceptador:")
status, url = resolve(codigo)
print(f"Status: {status}")
print(f"URL: {url}")

# Terceira resolução via interceptador
print("\nTerceira resolução via interceptador:")
status, url = resolve(codigo)
print(f"Status: {status}")
print(f"URL: {url}")