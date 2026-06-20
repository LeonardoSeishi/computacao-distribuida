"""
Testa a ABERTURA do Circuit Breaker.
Cenário: backend parado → tres chamadas falham → circuito abre → chamadas rejeitadas.
Após o timeout de recuperação, uma tentativa é feita (MEIO_ABERTO) se o backend
continuar indisponível, o circuito volta a abrir imediatamente.

INSTRUÇÕES:
-Configure o interceptador com:
    CIRCUIT_BREAKER_FALHAS=3
    CIRCUIT_BREAKER_TIMEOUT=5    # tempo curto para testes
    REQUEST_TIMEOUT=2
-Inicie o interceptador.
-PARE o servidor backend (responsável por http://127.0.0.1:5000).
-Execute este script.
"""

import time
from cliente_lib import resolve, encurta


# Podemos simplesmente usar um shortcode inventado, pois o backend não
# responderá, gerando erro de conexão.
CODIGO = "Teste1"

def tentar_resolver(descricao):
    print(f"\n>>> {descricao}")
    status, msg = resolve(CODIGO)
    if status == 0:
        print(f"    Sucesso (não esperado): {msg}")
    else:
        print(f"    Erro: {msg}")
    print()

# Primeira falha (limite é 2, então o circuito ainda não abre)
# Gera exceção porque o backend está offline → ConnectionError.
tentar_resolver("1º tentativa (falha): backend offline")

# Segunda falha
tentar_resolver("2º tentativa (falha)")

# Terceira falha:  atinge o limite e ABRE o circuito
tentar_resolver("3º tentativa (falha) (abre o circuito)")

# Dentro do período de recuperação (5 segundos), qualquer chamada
# deve ser barrada com "Circuito aberto".
print("\n-- Aguardando 1 segundo (dentro do timeout) --")
time.sleep(1)
tentar_resolver("Tentativa durante ABERTO (bloqueada)")

# Após o timeout de recuperação, o circuito passa para MEIO_ABERTO
# e permite UMA chamada de teste. Como o backend continua offline,
# essa chamada falha e o circuito volta a ABRIR.
print("\n-- Aguardando tempo total de recuperação (5 s) --")
time.sleep(6)
tentar_resolver("MEIO_ABERTO (backend offline → falha e volta a ABERTO)")

# Nova chamada logo em seguida ainda deve ser barrada.
tentar_resolver("Tentativa imediatamente após (bloqueada novamente)")

#-------------------------------------------------------------------------------------------------------#


print("\nTeste de abertura concluído. O circuito deve estar ABERTO.")
print("Certifique-se de que o backend esteja LIGADO para o próximo teste de fechamento.")
input("Pressione ENTER para iniciar...")

CODIGO = "Teste2" # código diferente para evitar cache (se o backend voltar ao ar)

print("\n-- Aguardando timeout de recuperação (6 s) --")
time.sleep(6)

# Agora o circuito tenta MEIO_ABERTO. Como o backend está no ar,
# a chamada é bem‑sucedida e o circuito fecha (FECHADO).
tentar_resolver("MEIO_ABERTO (backend no ar → sucesso, circuito fecha)")

# Chamada seguinte deve fluir normalmente (estado FECHADO).
tentar_resolver("Confirmação (circuito FECHADO)")

print("\nTeste de fechamento concluído. Sistema normal.")
input("Pressione ENTER para encerrar...")