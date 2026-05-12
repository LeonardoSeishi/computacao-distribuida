"""
Testa o FECHAMENTO do Circuit Breaker.
Cenário: com o circuito ABERTO, o backend volta ao ar.
Após o timeout de recuperação, a chamada de teste (MEIO_ABERTO) obtém sucesso
e o circuito fecha, permitindo requisições subsequentes.

INSTRUÇÕES:
-Mesmas configurações do teste de abertura.
-Execute o teste de abertura primeiro (ou garanta que o circuito está ABERTO).
-INICIE o servidor backend antes de executar este script.
-Execute este script.
"""

import time
from cliente_lib import resolve

# Código não cacheado, garantindo ida ao backend (agora no ar).
CODIGO = "teste123"

def tentar_resolver(descricao):
    print(f"\n>>> {descricao}")
    status, msg = resolve(CODIGO)
    if status == 0:
        print(f"    Sucesso: {msg}")
    else:
        print(f"    Erro: {msg}")

print("Certifique-se de que o circuito está ABERTO e o backend está RODANDO.")
input("Pressione ENTER para iniciar...")

# Primeira tentativa com o circuito ABERTO – será barrada.
tentar_resolver("Tentativa com circuito ABERTO (bloqueada)")

# Aguardar o tempo de recuperação para forçar transição para MEIO_ABERTO.
print("\n-- Aguardando timeout de recuperação (6 s) --")
time.sleep(6)

# Agora o circuito tenta MEIO_ABERTO. Como o backend está no ar,
# a chamada é bem‑sucedida e o circuito fecha (FECHADO).
tentar_resolver("MEIO_ABERTO (backend no ar → sucesso, circuito fecha)")

# Chamada seguinte deve fluir normalmente (estado FECHADO).
tentar_resolver("Confirmação (circuito FECHADO)")

print("\nTeste de fechamento concluído. Sistema normal.")
input("Pressione ENTER para encerrar...")