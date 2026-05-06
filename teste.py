# exemplo_cliente_py.py
from cliente_lib import encurta, resolve, remove_url

# buffers para receber resultados (tamanhos máximos)
codigo = bytearray(20)
url = bytearray(500)

# Encurtar
if encurta("https://www.exemplo.com/muito/longa", codigo) == 0:
    print(f"Encurtado: {codigo.decode().strip('\0')}")
else:
    print("Falha ao encurtar")

# Resolver
if resolve(codigo.decode().strip('\0'), url) == 0:
    print(f"URL original: {url.decode().strip('\0')}")
else:
    print("Falha ao resolver")

# Remover
if remove_url(codigo.decode().strip('\0')) == 0:
    print("Removido com sucesso")
else:
    print("Falha ao remover")