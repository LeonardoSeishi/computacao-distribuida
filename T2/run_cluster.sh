#!/usr/bin/env bash
# run_cluster.sh — Inicia um cluster Raft com 3 nós em background.
#
# Uso:
#   ./run_cluster.sh           # inicia os 3 nós (logs em logs/)
#   ./run_cluster.sh --kill    # encerra todos os nós
#   ./run_cluster.sh --reset   # encerra, apaga estado persistido e reinicia do zero
#   ./run_cluster.sh --demo    # cenário: submissões concorrentes

set -euo pipefail

NODES=3
BASE_PORT=5000
LOGDIR="logs"
DATADIR="data"

mkdir -p "$LOGDIR" "$DATADIR"

# ── Kill mode ────────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--kill" ]]; then
    echo "Encerrando todos os nós Raft..."
    pkill -f "python3 main.py" 2>/dev/null || true
    echo "Pronto."
    exit 0
fi

# ── Reset mode ───────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--reset" ]]; then
    echo "Encerrando todos os nós Raft..."
    pkill -f "python3 main.py" 2>/dev/null || true
    echo "Apagando estado persistido ($DATADIR/)..."
    rm -f "$DATADIR"/raft_state_*.json
    echo "Apagando logs ($LOGDIR/)..."
    rm -f "$LOGDIR"/node*.log "$LOGDIR"/node*.pid
    echo "Pronto. Execute './run_cluster.sh' para iniciar um cluster limpo."
    exit 0
fi

# ── Demo mode ────────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--demo" ]]; then
    echo "=== DEMO: submissões concorrentes ==="
    sleep 2
    echo ""
    # Q1: "Qual protocolo o Raft usa para replicar entradas?" → correta: b (AppendEntries)
    # Marina e Maria acertam (Marina ganha 10pts, Maria ganha 5pts por não ser a primeira)
    # Leo erra (alternativa a)
    echo "Enviando respostas de Marina (nó 1), Maria (nó 2) e Leo (nó 3) ao mesmo tempo..."
    python3 client.py --node 1 --player Marina --question 1 --answer b &
    python3 client.py --node 2 --player Maria  --question 1 --answer b &
    python3 client.py --node 3 --player Leo    --question 1 --answer a &
    wait
    echo ""
    exit 0
fi

# ── Start cluster ────────────────────────────────────────────────────────────
echo "Iniciando cluster Raft com $NODES nós..."
echo ""

for i in $(seq 1 $NODES); do
    PORT=$((BASE_PORT + i))
    echo "  Nó $i → porta $PORT | log: $LOGDIR/node${i}.log"
    python3 main.py "$i" --nodes "$NODES" --data-dir "$DATADIR" \
        > "$LOGDIR/node${i}.log" 2>&1 &
    echo $! > "$LOGDIR/node${i}.pid"
done

echo ""
echo "Cluster iniciado. Logs em $LOGDIR/"
echo ""
echo "Comandos úteis:"
echo "  tail -f $LOGDIR/node1.log                                          # ver log do nó 1"
echo "  python3 client.py --questions                                      # listar as 6 perguntas"
echo "  python3 client.py --node 1 --player Alice --question 1 --answer b  # responder questão"
echo "  python3 client.py --node 1 --scoreboard                           # ver placar"
echo "  ./run_cluster.sh --demo                                            # cenário de demonstração"
echo "  ./run_cluster.sh --kill                                            # encerrar tudo"
echo "  ./run_cluster.sh --reset                                           # encerrar + apagar estado (sem reiniciar)"
echo ""
echo "Cenário de falha do líder:"
echo "  Descubra qual nó é o líder nos logs, depois:"
echo "  pkill -f "main.py id "
echo "  Observe nos outros logs a eleição de um novo líder."
