# T2 — Consenso via Raft (INE 5418 · UFSC · 2026/1)

Implementação do algoritmo Raft em Python puro (sem dependências externas),
com uma aplicação de quiz distribuído como demonstração.

## Requisitos

- Python 3.10+
- Sem pacotes externos (usa apenas a stdlib)

## Estrutura

```
raft/
  node.py       # máquina de estados Raft (eleição, log, commit)
  messages.py   # dataclasses: RequestVote, AppendEntries, etc.
  transport.py  # servidor/cliente TCP com Berkeley Sockets
app/
  quiz.py       # lógica do quiz — aplica comandos ao placar
main.py         # ponto de entrada de cada nó
client.py       # cliente CLI para jogadores
run_cluster.sh  # sobe os 3 nós de uma vez
```

## Execução

### Opção A — terminal por terminal (recomendado para desenvolvimento)

Abra quatro terminais na pasta do projeto.

**Terminal 1 — Nó 1:**
```bash
python3 main.py 1
```

**Terminal 2 — Nó 2:**
```bash
python3 main.py 2
```

**Terminal 3 — Nó 3:**
```bash
python3 main.py 3
```

Cada nó escuta na porta `5000 + id` (5001, 5002, 5003).  
Em poucos instantes você verá nos logs qual nó se elegeu líder.

**Terminal 4 — cliente:**
```bash
python3 client.py --questions                                              # listar perguntas
python3 client.py --node 1 --player Alice --question 1 --answer AppendEntries
python3 client.py --node 1 --scoreboard                                    # ver placar
```

O cliente redireciona automaticamente para o líder caso o nó contatado seja seguidor.

---

### Opção B — script automático (tudo em background)

```bash
./run_cluster.sh          # sobe os 3 nós, logs em logs/
tail -f logs/node1.log logs/node2.log logs/node3.log
```

```bash
./run_cluster.sh --demo   # cenário de submissões concorrentes
./run_cluster.sh --kill   # encerra todos os nós
```

---

## Cenários de demonstração

### Cenário 1 — execução normal com dois clientes simultâneos

O servidor suporta múltiplos clientes ao mesmo tempo — cada conexão roda em
uma thread separada. Para demonstrar a concorrência manualmente, use **5 terminais**:

**Terminais 1, 2, 3 — nós do cluster:**
```bash
python3 main.py 1   # terminal 1
python3 main.py 2   # terminal 2
python3 main.py 3   # terminal 3
```

**Terminal 4 — cliente Alice** (dispare ao mesmo tempo que o terminal 5):
```bash
python3 client.py --node 1 --player Alice --question 1 --answer AppendEntries
```

**Terminal 5 — cliente Bob** (dispare ao mesmo tempo que o terminal 4):
```bash
python3 client.py --node 2 --player Bob --question 2 --answer 3
```

Nos logs dos 3 nós você verá as duas entradas commitadas **na mesma ordem** e
o placar final idêntico nos três. Ou via script:

```bash
./run_cluster.sh --demo
```

### Cenário 2 — falha do líder

```bash
# 1. Iniciar o cluster
./run_cluster.sh

# 2. Ver qual nó virou líder (procure "tornou-se LÍDER" nos logs)
grep "LÍDER" logs/node*.log

# 3. Matar o líder (ex.: nó 1)
kill $(cat logs/node1.pid)

# 4. Observar nos outros logs:
#    - election timeout
#    - RequestVote sendo enviado
#    - novo líder eleito com term incrementado
tail -f logs/node2.log logs/node3.log
```

### Cenário 3 — recuperação de nó

```bash
# Após o cenário 2, reiniciar o nó que caiu:
python main.py 1 > logs/node1.log 2>&1 &

# O nó volta como follower e recebe AppendEntries para sincronizar o log.
tail -f logs/node1.log
```

---

## Protocolo

### Mensagens Raft (JSON sobre TCP)

| Mensagem | De → Para | Quando |
|---|---|---|
| `RequestVote` | candidato → todos | ao iniciar eleição |
| `RequestVoteReply` | qualquer → candidato | resposta ao voto |
| `AppendEntries` | líder → seguidores | replicação + heartbeat |
| `AppendEntriesReply` | seguidor → líder | confirmação |
| `ClientRequest` | cliente → qualquer nó | submissão de resposta |
| `GetScoreboard` | cliente → qualquer nó | consulta do placar |

### Timers

| Timer | Valor |
|---|---|
| `election_timeout` | aleatório 150–300 ms |
| `heartbeat_interval` | fixo 50 ms |

### Persistência

Cada nó salva `currentTerm`, `votedFor` e `log[]` em:
```
data/raft_state_{node_id}.json
```
Ao reiniciar, o estado é restaurado automaticamente.

---

## Formato do log

```
HH:MM:SS.mmm [NÓ 1 | LEADER    | term=2] quórum atingido | commitIndex=3
HH:MM:SS.mmm [NÓ 2 | FOLLOWER  | term=2] AppendEntries de 1 | +1 entr. | log_len=3
HH:MM:SS.mmm [NÓ 3 | FOLLOWER  | term=2] APLICANDO | index=3 | cmd={...}
```
