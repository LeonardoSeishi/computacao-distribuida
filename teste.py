from cliente_lib import encurta, resolve, remove_url

# Encurtar
status, codigo = encurta("https://www.google.com")
if status == 0:
    print(f"Encurtado: {codigo}")
    
    # Resolver
    status, url = resolve(codigo)
    if status == 0:
        print(f"URL: {url}")
        
        # Remover
        status = remove_url(codigo)
        if status == 0:
            print("Removido!")
        else:
            print("Falha ao remover")
    else:
        print("Falha ao resolver")
else:
    print("Falha ao encurtar")