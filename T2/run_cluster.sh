#!/usr/bin/env bash
# run_cluster.sh — Inicia um cluster Raft com 3 nós em terminais separados.
#
# Uso:
#   ./run_cluster.sh           # inicia os 3 nós em background, logs em logs/
#   ./run_cluster.sh --kill    # encerra todos os nós
#   ./run_cluster.sh --demo    # cenário 1: submissões concorrentes

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

# ── Demo mode ────────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--demo" ]]; then
    echo "=== DEMO: submissões concorrentes ==="
    echo "Aguardando líder eleger-se (2s)..."
    sleep 2
    echo ""
    echo "Enviando resposta de Alice (nó 1) e Bob (nó 2) simultaneamente..."
    python3 client.py --node 1 --player Alice --question 1 --answer AppendEntries &
    python3 client.py --node 2 --player Bob   --question 2 --answer 3 &
    wait
    echo ""
    echo "Placar final (todos os nós devem ser idênticos):"
    for i in $(seq 1 $NODES); do
        echo -n "  Nó $i: "
        python3 client.py --node "$i" --scoreboard 2>/dev/null | grep -v '^$' | tail -n +2 || echo "(indisponível)"
    done
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
echo "  tail -f $LOGDIR/node1.log              # ver log do nó 1"
echo "  python3 client.py --questions            # listar perguntas"
echo "  python3 client.py --node 1 --player Alice --question 1 --answer AppendEntries"
echo "  python3 client.py --node 1 --scoreboard # ver placar"
echo "  ./run_cluster.sh --demo                # cenário de demonstração"
echo "  ./run_cluster.sh --kill                # encerrar tudo"
echo ""
echo "Cenário de falha do líder:"
echo "  Descubra qual nó é o líder nos logs, depois:"
echo "  kill \$(cat $LOGDIR/node<id>.pid)"
echo "  Observe nos outros logs a eleição de um novo líder."
