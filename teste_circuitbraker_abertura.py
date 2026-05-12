"""
Testa a ABERTURA do Circuit Breaker.
Cenário: backend parado → duas chamadas falham → circuito abre → chamadas rejeitadas.
Após o timeout de recuperação, uma tentativa é feita (MEIO_ABERTO) se o backend
continuar indisponível, o circuito volta a abrir imediatamente.

INSTRUÇÕES:
-Configure o interceptador com:
    CIRCUIT_BREAKER_FALHAS=2
    CIRCUIT_BREAKER_TIMEOUT=5
    REQUEST_TIMEOUT=2
-Inicie o interceptador.
-PARE o servidor backend (responsável por http://127.0.0.1:5000).
-Execute este script.
"""

import time
from cliente_lib import resolve, encurta


# Podemos simplesmente usar um shortcode inventado, pois o backend não
# responderá, gerando erro de conexão.
CODIGO = "abc1234"   # qualquer código não existente no cache

def tentar_resolver(descricao):
    print(f"\n>>> {descricao}")
    status, msg = resolve(CODIGO)
    if status == 0:
        print(f"    Sucesso (não esperado): {msg}")
    else:
        print(f"    Erro: {msg}")

# Primeira falha (limite é 2, então o circuito ainda não abre)
# Gera exceção porque o backend está offline → ConnectionError.
tentar_resolver("1ª tentativa (falha) – backend offline")

# Segunda falha → atinge o limite e ABRE o circuito
tentar_resolver("2ª tentativa (abre o circuito)")

# Dentro do período de recuperação (5 segundos), qualquer chamada
# deve ser barrada com "Circuito aberto".
print("\n-- Aguardando 1 segundo (dentro do timeout) --")
time.sleep(1)
tentar_resolver("Tentativa durante ABERTO (bloqueada)")

# Após o timeout de recuperação, o circuito passa para MEIO_ABERTO
# e permite UMA chamada de teste. Como o backend continua offline,
# essa chamada falha e o circuito volta a ABRIR.
print("\n-- Aguardando tempo total de recuperação (5 s) --")
time.sleep(5)
tentar_resolver("MEIO_ABERTO (backend offline → falha e volta a ABERTO)")

# Nova chamada logo em seguida ainda deve ser barrada.
tentar_resolver("Tentativa imediatamente após (bloqueada novamente)")

print("\nTeste de abertura concluído. O circuito deve estar ABERTO.")
input("Pressione ENTER para encerrar...")