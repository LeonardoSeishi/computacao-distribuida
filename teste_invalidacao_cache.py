from cliente_lib import encurta, resolve, remove_url

# Teste para demonstrar a invalidação do cache.
# Após remover uma URL encurtada, o sistema tenta resolver o mesmo código novamente.

url_original = "https://github.com"

status, codigo = encurta(url_original)

if status != 0:
    print("Falha ao encurtar URL")
    exit(1)

print(f"Encurtado: {codigo}")

print("\nResolução antes da remoção:")
status, url = resolve(codigo)
print(f"Status: {status}")
print(f"URL: {url}")

print("\nRemovendo URL:")
status = remove_url(codigo)
print(f"Status remoção: {status}")

print("\nTentando resolver após remoção:")
status, resposta = resolve(codigo)
print(f"Status: {status}")
print(f"Resposta: {resposta}")