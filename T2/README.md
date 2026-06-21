# T2 — Consenso via Raft (INE 5418 · UFSC · 2026/1)

Implementação do algoritmo **Raft** em Python puro, com um quiz distribuído como aplicação de demonstração.

- **Sem dependências externas** — usa apenas a stdlib do Python 3.10+
- **3 nós** se comunicam via Berkeley Sockets (TCP/JSON)
- O placar do quiz é a **state machine replicada** pelo Raft

---

## Como executar

### Passo 1 — subir o cluster (3 terminais)

```bash
python3 main.py 1   # terminal 1 → escuta na porta 5001
python3 main.py 2   # terminal 2 → escuta na porta 5002
python3 main.py 3   # terminal 3 → escuta na porta 5003
```

Em poucos instantes um nó se elege líder. Procure nos logs:
```
[NÓ 2 | LEADER    | term=1] tornou-se LÍDER
```

> Alternativa: `./run_cluster.sh` sobe os 3 nós em background com logs em `logs/`.

---

### Passo 2 — jogar (4º terminal)

**Ver as perguntas:**
```bash
python3 client.py --questions
```

Saída:
```
[1] Qual protocolo o Raft usa para replicar entradas?
      a) RequestVote
      b) AppendEntries
      c) HeartBeat
      d) Commit
...
```

**Enviar uma resposta:**
```bash
python3 client.py --node 1 --player Alice --question 1 --answer b
```

**Ver o placar:**
```bash
python3 client.py --node 1 --scoreboard
```

> O cliente redireciona automaticamente para o líder se o nó contatado for seguidor.

---

## Regra de pontuação

A validação da resposta e o cálculo dos pontos acontecem **dentro do `apply()` da state machine**, não no cliente. Isso é intencional: o Raft serializa as submissões concorrentes, e quem aparecer primeiro no log é tratado como "primeiro a responder".

| Situação | Pontos |
|---|---|
| Primeiro a acertar a questão | pontos cheios |
| Segundo ou mais a acertar | metade (arredondado para baixo) |
| Resposta errada | 0 pontos, nada é commitado |

---

## Estrutura do projeto

```
raft/
  node.py       # máquina de estados Raft: eleição, replicação, commit
  messages.py   # dataclasses das mensagens (RequestVote, AppendEntries…)
  transport.py  # servidor/cliente TCP com Berkeley Sockets
app/
  quiz.py       # state machine do quiz: valida respostas, mantém placar
main.py         # ponto de entrada de cada nó
client.py       # cliente CLI para jogadores
run_cluster.sh  # script para subir/derrubar o cluster
```

A separação entre `raft/` e `app/` é deliberada: o Raft não sabe nada sobre quiz, e o quiz não sabe nada sobre consenso — ele só implementa `apply(command)`.

---

## Cenários de demonstração

### Cenário 1 — execução normal com dois clientes simultâneos

Mostra que todos os nós commitam as entradas **na mesma ordem** e chegam ao **mesmo placar**.

```bash
# Terminal 4 — Alice responde a questão 1
python3 client.py --node 1 --player Alice --question 1 --answer b

# Terminal 5 — Bob responde a mesma questão ao mesmo tempo
python3 client.py --node 2 --player Bob --question 1 --answer b
```

Nos logs dos 3 nós você verá as duas entradas no log em uma ordem única e consistente. Quem aparecer no índice menor ganha pontos cheios; o outro ganha metade. O placar é idêntico nos 3 terminais.

---

### Cenário 2 — falha do líder

Mostra eleição automática e recuperação sem perda de dados.

```bash
# 1. Subir o cluster
./run_cluster.sh

# 2. Descobrir qual nó é o líder
grep "tornou-se LÍDER" logs/node*.log

# 3. Matar o líder (ex.: nó 1)
kill $(cat logs/node1.pid)

# 4. Acompanhar a eleição nos logs dos nós restantes
tail -f logs/node2.log logs/node3.log
```

Você verá:
- Election timeout disparando
- `RequestVote` sendo enviado
- Novo líder eleito com `term` incrementado
- Operações continuando normalmente

---

### Cenário 3 — recuperação de nó

Mostra que um nó que caiu volta com o log sincronizado.

```bash
# Após o cenário 2, reiniciar o nó 1
python3 main.py 1

# Acompanhar a sincronização
tail -f logs/node1.log
```

O nó volta como follower e recebe `AppendEntries` do líder atual para preencher as entradas que perdeu enquanto estava fora.

---

## Referência do protocolo

### Mensagens (JSON sobre TCP)

| Mensagem | De → Para | Quando |
|---|---|---|
| `RequestVote` | candidato → todos | ao iniciar eleição |
| `RequestVoteReply` | qualquer → candidato | resposta ao voto |
| `AppendEntries` | líder → seguidores | replicação de entradas + heartbeat |
| `AppendEntriesReply` | seguidor → líder | confirmação ou rejeição |
| `ClientRequest` | cliente → qualquer nó | submissão de resposta do quiz |
| `ClientResponse` | nó → cliente | resultado após commit |
| `GetScoreboard` | cliente → qualquer nó | consulta do placar (leitura local) |

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
O estado é restaurado automaticamente ao reiniciar — um nó que cai e volta sincroniza o log via `AppendEntries`.

---

## Formato dos logs

Cada linha segue o padrão:
```
HH:MM:SS.mmm [NÓ {id} | {ESTADO}   | term={n}] {evento}
```

Exemplos:
```
10:01:02.300 [NÓ 1 | LEADER    | term=2] client cmd enfileirado | index=4 | cmd={player: Alice, question_id: 1, answer: b}
10:01:02.310 [NÓ 2 | FOLLOWER  | term=2] AppendEntries de 1 | +1 entr. | log_len=4
10:01:02.320 [NÓ 3 | FOLLOWER  | term=2] AppendEntries de 1 | +1 entr. | log_len=4
10:01:02.330 [NÓ 1 | LEADER    | term=2] quórum atingido | commitIndex=4
10:01:02.330 [NÓ 1 | LEADER    | term=2] APLICANDO | index=4 | result={correct: true, points_awarded: 10, first: true}
```
