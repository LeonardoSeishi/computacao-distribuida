# Trabalho 2 — Computação Distribuída (INE 5418 · UFSC · 2026/1)
## Aplicação Distribuída baseada em Building Blocks

---

## Contexto acadêmico

Este projeto é o Trabalho 2 da disciplina INE 5418 — Computação Distribuída da Universidade Federal de Santa Catarina (UFSC), semestre 2026/1.

**Building block sorteado:** Consenso  
**Algoritmo escolhido:** Raft  
**Aplicação de exemplo:** Quiz acadêmico distribuído (estilo Kahoot simplificado)  
**Linguagem:** Python  
**Grupos:** até 3 participantes

> ⚠️ O foco do trabalho **não é a aplicação do quiz em si**, mas sim demonstrar o funcionamento do algoritmo Raft. O quiz existe apenas para contextualizar e gerar eventos concretos (submissões concorrentes, placar replicado) que permitam observar o Raft em ação.

---

## Requisitos do professor (critérios de avaliação)

### Desenvolvimento
- [ ] Implementar uma aplicação distribuída funcional
- [ ] Utilizar **pelo menos três processos independentes**
- [ ] Implementar pelo menos um building block principal (consenso via Raft)
- [ ] Utilizar **comunicação real entre processos via Berkeley Sockets**

### Demonstração (vídeo — máximo 10 minutos)
- [ ] Apresentar **logs ou saídas** que permitam observar o funcionamento do sistema
- [ ] Demonstrar um **cenário normal de execução**
- [ ] Demonstrar um **cenário com concorrência, falha, atraso ou comportamento especial** relacionado ao tema

### Seminário (apresentação em aula — 10 a 12 minutos)
- [ ] Discutir as principais decisões de implementação
- [ ] Explicar o algoritmo e a API disponibilizada pelo building block
- [ ] Ilustrar o uso do building block com a aplicação de exemplo

### Conteúdo mínimo da apresentação
- Problema estudado
- Aplicação implementada
- Building block utilizado
- Arquitetura da solução
- Algoritmo implementado
- Cenário normal de execução
- Cenário com concorrência, falha, atraso ou comportamento especial
- Principais dificuldades
- Limitações e conclusões

### Entregáveis (via Moodle)
- [ ] Código-fonte
- [ ] Instruções claras de compilação e execução
- [ ] Slides da apresentação
- [ ] Link para vídeo da demonstração gravada (máx. 10 min)

> ⚠️ Os nomes de todos os participantes devem constar nos artefatos entregues. Participantes não referenciados não serão considerados membros do grupo.

---

## Recomendações do professor para a demo

Para o tema de consenso, o professor espera que seja possível observar nos logs:
- Que todos os processos entregam as mensagens (comandos) na **mesma ordem**
- Que o sistema se recupera corretamente após falha
- Que a concorrência é tratada de forma consistente

---

## Sobre o algoritmo Raft

O Raft é um algoritmo de **consenso distribuído** projetado para ser compreensível. Ele resolve o problema de fazer múltiplos processos concordarem em uma sequência de comandos (log replicado), mesmo na presença de falhas parciais.

### Por que Raft e não Paxos
- Paper original ("In Search of an Understandable Consensus Algorithm") foi escrito com foco em clareza — ideal para explicar no seminário
- Completamente especificado (Paxos deixa muitos detalhes implícitos)
- Cenários de falha naturais e dramáticos para a demo (queda do líder, eleição, recuperação)
- Usado em produção: etcd (Kubernetes), CockroachDB, TiKV

### Papéis dos nós
Cada nó está sempre em um de três estados:
- **Líder:** único nó que aceita requisições de clientes e coordena replicação
- **Seguidor:** replica o log do líder e responde a heartbeats
- **Candidato:** estado transitório durante eleição

### Termos
O tempo é dividido em **termos** (inteiros monotonicamente crescentes). Cada termo começa com uma eleição. Se um candidato vencer, ele lidera pelo resto do termo.

### As quatro mensagens do protocolo
| Mensagem | Quem envia | Para quem | Quando |
|---|---|---|---|
| `RequestVote` | candidato | todos os nós | ao iniciar eleição |
| `RequestVoteReply` | qualquer nó | candidato | resposta ao vote request |
| `AppendEntries` | líder | todos os seguidores | replicação de entradas + heartbeat (entries vazio) |
| `AppendEntriesReply` | seguidor | líder | confirmação ou rejeição |

### Estado que cada nó mantém

**Persistido em disco** (deve sobreviver a crash e reinício):
```
currentTerm   # último termo visto pelo nó
votedFor      # candidato em quem votou no termo atual (ou None)
log[]         # lista de entradas: [{term, index, command}]
```

**Volátil em memória — todos os nós:**
```
commitIndex   # maior índice conhecido como commitado
lastApplied   # maior índice aplicado à máquina de estados
state         # 'follower' | 'candidate' | 'leader'
```

**Volátil em memória — somente o líder:**
```
nextIndex[]   # próximo índice a enviar para cada seguidor
matchIndex[]  # maior índice confirmado em cada seguidor
```

### Eleição de líder
1. Seguidor não recebe heartbeat dentro do `election_timeout` (aleatório entre 150–300ms)
2. Incrementa `currentTerm`, vira candidato, vota em si mesmo
3. Envia `RequestVote` para todos
4. Se receber votos da maioria (`n//2 + 1`): vira líder, começa a enviar heartbeats
5. O timeout **aleatório** evita empates persistentes entre candidatos

### Replicação de log
1. Cliente envia comando ao líder
2. Líder adiciona entrada ao seu log (não commitada ainda)
3. Líder envia `AppendEntries` para todos os seguidores em paralelo
4. Quando a **maioria** confirmar, líder commita e aplica à máquina de estados
5. Líder responde ao cliente e notifica commit nos próximos heartbeats

### Garantias do Raft
- **Safety:** nunca dois líderes no mesmo termo; nenhuma entrada commitada é perdida
- **Liveness:** desde que a maioria dos nós funcione, o sistema progride
- **Tolerância a falhas:** suporta até `(n-1)//2` falhas simultâneas (com 3 nós: tolera 1 falha)

---

## Arquitetura do sistema

### Visão geral
```
[Cliente A] ──┐
              ├──► [Nó 1 — líder  :5001] ◄──► [Nó 2 — seguidor :5002]
[Cliente B] ──┘              │
                             └──────────────► [Nó 3 — seguidor :5003]
```

- Todos os processos rodam na **mesma máquina**, em portas distintas
- Clientes se conectam **sempre ao líder**
- Seguidores não aceitam requisições de clientes diretamente

### Estrutura interna de cada nó
```
┌─────────────────────────────────────────┐
│         camada de aplicação (quiz)       │
│  recebe resposta · pontua · exibe placar │
│                submit(cmd) ↕ commit(cmd) │
├─────────────────────────────────────────┤
│           building block — Raft          │
│  ┌─────────────┐ ┌──────────────────┐   │
│  │  eleição    │ │ replicação de log│   │
│  │ RequestVote │ │ AppendEntries    │   │
│  │ heartbeat   │ │ commitIndex      │   │
│  └─────────────┘ └──────────────────┘   │
│  ┌─────────────────────────────────┐    │
│  │  comunicação — Berkeley Sockets │    │
│  │  TCP · mensagens em JSON        │    │
│  └─────────────────────────────────┘    │
├─────────────────────────────────────────┤
│  estado persistido em disco             │
│  currentTerm · votedFor · log[]         │
└─────────────────────────────────────────┘
```

### Estrutura de arquivos sugerida
```
projeto/
├── raft/
│   ├── __init__.py
│   ├── node.py          # máquina de estados Raft (eleição, log, commit)
│   ├── messages.py      # dataclasses: RequestVote, AppendEntries, etc.
│   └── transport.py     # servidor/cliente TCP com Berkeley Sockets
├── app/
│   ├── __init__.py
│   └── quiz.py          # lógica do quiz — fina, só chama raft.submit()
├── main.py              # ponto de entrada: recebe node_id e lista de peers
├── run_cluster.sh       # script para subir os 3 nós de uma vez
└── README.md            # instruções de execução (exigido pelo professor)
```

> A separação entre `raft/` e `app/` é importante para o seminário — demonstra fisicamente que o building block é independente da aplicação.

---

## Aplicação de exemplo: quiz acadêmico

### Funcionamento
- Um processo separado atua como **servidor de perguntas** (pode ser o próprio líder ou um processo externo)
- Alunos (clientes) enviam respostas via linha de comando
- Cada resposta correta gera um comando `{"player": "A", "points": 10}` submetido ao Raft
- Após o commit, todos os nós aplicam o comando à máquina de estados (placar)
- O placar deve ser **idêntico em todos os nós** ao final — isso é o que demonstra o consenso

### Exemplo de log esperado na demo
```
[NÓ 1 | líder  | term=2] AppendEntries → nó2, nó3 | entry: {player:A, pts:10}
[NÓ 2 | seguidor | term=2] AppendEntries recebido | index=3 | ACK
[NÓ 3 | seguidor | term=2] AppendEntries recebido | index=3 | ACK
[NÓ 1 | líder  | term=2] quórum atingido | commitIndex=3 | aplicando...
[NÓ 1 | líder  | term=2] PLACAR: {A:10, B:0}
```

---

## Cenários de demonstração (exigido pelo professor)

### Cenário 1 — execução normal
- Subir os 3 nós
- Dois clientes enviam respostas simultaneamente
- Mostrar nos logs que todos os nós commitam as entradas **na mesma ordem**
- Exibir o placar final idêntico nos 3 terminais

### Cenário 2 — falha do líder (principal cenário de falha)
- Durante uma submissão, matar o processo líder com `Ctrl+C` ou `kill`
- Mostrar nos logs dos seguidores:
  - Timeout do election timer
  - `RequestVote` sendo enviado
  - Novo líder eleito com termo incrementado
  - Operações continuando sem perda de dados
- Este cenário evidencia **tolerância a falhas** e **eleição de líder**

### Cenário 3 — recuperação de nó
- Reiniciar o nó que caiu
- Ele volta como seguidor
- Líder percebe o nó de volta e envia `AppendEntries` para sincronizar o log
- Mostrar que o nó recuperado termina com o log idêntico aos demais

---

## Observações de implementação

### Berkeley Sockets
- Usar `socket` da stdlib do Python (`import socket`)
- TCP (não UDP) para garantir entrega
- Cada nó escuta em uma porta fixa; conhece as portas dos demais (configuração estática)
- Tratar reconexão: nós podem cair e voltar

### Serialização das mensagens
- JSON é suficiente para o trabalho
- Cada mensagem deve ter um campo `type` para identificação
- Exemplo:
```json
{
  "type": "AppendEntries",
  "term": 2,
  "leaderId": 1,
  "prevLogIndex": 2,
  "prevLogTerm": 1,
  "entries": [{"term": 2, "index": 3, "command": {"player": "A", "points": 10}}],
  "leaderCommit": 2
}
```

### Timers
- `election_timeout`: aleatório entre 150–300ms — reinicia a cada heartbeat recebido
- `heartbeat_interval`: fixo em 50ms — líder envia mesmo sem novas entradas (entries vazio)
- Usar `threading.Timer` ou um loop com `time.sleep` em thread separada

### Persistência
- Salvar `currentTerm`, `votedFor` e `log[]` em arquivo JSON local por nó
- Carregar ao iniciar — permite reiniciar um nó após crash

### Logs de saída (importante para a demo)
Cada linha de log deve mostrar claramente:
```
[NÓ {id} | {estado} | term={n}] {evento}
```
O professor avalia se é possível **observar o funcionamento** pelo log. Capriche nessa parte.

---

## Como iniciar no Claude Code

Ao abrir o Claude Code no VS Code com este arquivo presente, use o prompt:

```
Leia o PROJETO.md e me ajude a implementar o módulo Raft em Python,
começando pelo arquivo raft/node.py com a máquina de estados principal.
Siga a estrutura de arquivos definida no documento.
```

---

## Referências

- Paper original do Raft: https://raft.github.io/raft.pdf
- Visualização interativa: http://thesecretlivesofdata.com/raft/
- Site oficial com recursos: https://raft.github.io
