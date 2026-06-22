"""
raft/node.py — Máquina de estados Raft
  • Eleição de líder (RequestVote / RequestVoteReply)
  • Replicação de logs  (AppendEntries / AppendEntriesReply)
  • Persistência de currentTerm, votedFor e log[]
  • API para cliente:
  start(), stop(), submit(command) -> (success, result)
  
Concorrência: todas as mudanças de estado são protegidas por self._lock (RLock).
Timers e comunicação via rede são feitas fora do lock para evitar bloqueios.
"""

import json
import logging
import os
import random
import threading
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .messages import (
    AppendEntries, AppendEntriesReply, LogEntry,
    RequestVote, RequestVoteReply, parse_message,
)
from .transport import Transport

logger = logging.getLogger(__name__)

ELECTION_TIMEOUT_MIN = 0.150   # 150 ms
ELECTION_TIMEOUT_MAX = 0.300   # 300 ms
HEARTBEAT_INTERVAL   = 0.050   # 50 ms

# Retorno quando um nodo sai antes de comprometer uam entrada
_LOST_LEADERSHIP = object()


class RaftNode:
    def __init__(
        self,
        node_id: int,
        peers: Dict[int, Tuple[str, int]], 
        host: str,
        port: int,
        state_machine: Callable[[Any], Any], 
        data_dir: str = './data',
    ):
        self.node_id = node_id
        self.peers = peers
        self.host = host
        self.port = port
        self.state_machine = state_machine
        self.data_dir = data_dir

        # Estado persistente
        self.current_term: int = 0
        self.voted_for: Optional[int] = None
        self.log: List[LogEntry] = []       

        # Estado volátil
        self.commit_index: int = 0          # Maior índice comprometido
        self.last_applied: int = 0          # Maior indice aplicado à máquina de estados
        self.state: str = 'follower'       
        self.current_leader: Optional[int] = None

        # Estado volátil para candidatos
        self.votes_received: Set[int] = set()

        # Estado volátil para líderes (mensagens de AppendEntries)
        self.next_index: Dict[int, int] = {}   
        self.match_index: Dict[int, int] = {}  

        # Submissão de clientes
        # Eventos para sinalizar quando uma entrada é comprometida ou liderança perdida.
        self._pending: Dict[int, threading.Event] = {}
        self._pending_results: Dict[int, Any] = {}

        self._lock = threading.RLock()
        self._election_timer: Optional[threading.Timer] = None
        self._heartbeat_timer: Optional[threading.Timer] = None

        self._transport = Transport(host, port, self._on_message)
        self._load_persistent_state()

    # 
    # API
    #

    def start(self):
        self._transport.start()
        self._reset_election_timer()
        self._log('started as follower')

    def stop(self):
        with self._lock:
            self._cancel_election_timer()
            self._cancel_heartbeat_timer()
        self._transport.stop()

    def submit(self, command: Any) -> Tuple[bool, Any]:
        """
        Enviar comando para o log do líder.

        Retorna (True, result) quando a entrada é comprometida.
        Retorna (False, leader_id_or_None) se o nó não é o líder
        ou se a liderança foi perdida antes do comprometimento (LOST_LEADERSHIP)
        """
        with self._lock:
            if self.state != 'leader':
                return False, self.current_leader

            index = len(self.log) + 1
            entry = LogEntry(term=self.current_term, index=index, command=command)
            self.log.append(entry)
            self._save_persistent_state()
            self.match_index[self.node_id] = index
            self.next_index[self.node_id] = index + 1

            event = threading.Event()
            self._pending[index] = event
            self._log(f'client cmd enfileirado | index={index} | cmd={command}')

        # Replicação fora do lock para não bloquear o cliente
        self._send_append_entries_to_all()

        # Bloqueado até comprometer ou timeout
        committed = event.wait(timeout=5.0)
        if not committed:
            self._pending.pop(index, None)
            return False, None

        result = self._pending_results.pop(index, None)
        if result is _LOST_LEADERSHIP:
            return False, self.current_leader
        return True, result

    @property
    def is_leader(self) -> bool:
        return self.state == 'leader'

    @property
    def leader_id(self) -> Optional[int]:
        return self.current_leader

    # 
    # Persistência
    #

    def _persistence_path(self) -> str:
        return os.path.join(self.data_dir, f'raft_state_{self.node_id}.json')

    def _load_persistent_state(self):
        path = self._persistence_path()
        if not os.path.exists(path):
            return
        try:
            with open(path) as f:
                d = json.load(f)
            self.current_term = d.get('currentTerm', 0)
            self.voted_for = d.get('votedFor')
            self.log = [LogEntry.from_dict(e) for e in d.get('log', [])]
            self._log(
                f'estado carregado do disco | term={self.current_term} '
                f'log_len={len(self.log)}'
            )
        except Exception as e:
            logger.error(f'Erro ao carregar estado persistido: {e}')

    def _save_persistent_state(self):
        """Escreve estado persistente de forma atômica."""
        os.makedirs(self.data_dir, exist_ok=True)
        path = self._persistence_path()
        tmp = path + '.tmp'
        try:
            with open(tmp, 'w') as f:
                json.dump({
                    'currentTerm': self.current_term,
                    'votedFor': self.voted_for,
                    'log': [e.to_dict() for e in self.log],
                }, f)
            os.replace(tmp, path)
        except Exception as e:
            logger.error(f'Erro ao salvar estado persistido: {e}')

    # 
    # Timers
    # 

    def _reset_election_timer(self):
        """Cancela o timer atual. Sem necessidade de lock."""
        if self._election_timer:
            self._election_timer.cancel()
        timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        self._election_timer = threading.Timer(timeout, self._start_election)
        self._election_timer.daemon = True
        self._election_timer.start()

    def _cancel_election_timer(self):
        if self._election_timer:
            self._election_timer.cancel()
            self._election_timer = None

    def _start_heartbeat_timer(self):
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()
        self._heartbeat_timer = threading.Timer(HEARTBEAT_INTERVAL, self._heartbeat_tick)
        self._heartbeat_timer.daemon = True
        self._heartbeat_timer.start()

    def _cancel_heartbeat_timer(self):
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()
            self._heartbeat_timer = None

    # 
    # Eleição de líder
    # 

    def _start_election(self):
        with self._lock:
            if self.state == 'leader':
                return

            self.state = 'candidate'
            self.current_term += 1
            self.voted_for = self.node_id
            self.votes_received = {self.node_id}
            self.current_leader = None
            self._save_persistent_state()

            last_log_index = len(self.log)
            last_log_term = self.log[-1].term if self.log else 0
            msg = RequestVote(
                term=self.current_term,
                candidateId=self.node_id,
                lastLogIndex=last_log_index,
                lastLogTerm=last_log_term,
            )
            peers_snapshot = dict(self.peers)
            self._log(
                f'eleição iniciada | pedindo votos de {list(peers_snapshot.keys())}'
            )

        # Envio fora do lock. Se o resultado é inconclusivo, reinicia o timer
        for peer_id, (h, p) in peers_snapshot.items():
            threading.Thread(
                target=self._transport.send,
                args=(h, p, msg.to_dict()),
                daemon=True,
            ).start()

        self._reset_election_timer()

    # 
    # Heartbeats e replicação de logs
    # 

    def _heartbeat_tick(self):
        with self._lock:
            if self.state != 'leader':
                return
        self._send_append_entries_to_all()
        self._start_heartbeat_timer()

    def _send_append_entries_to_all(self):
        with self._lock:
            if self.state != 'leader':
                return
            peers_snapshot = dict(self.peers)

        for peer_id, (h, p) in peers_snapshot.items():
            threading.Thread(
                target=self._send_append_entries_to_peer,
                args=(peer_id, h, p),
                daemon=True,
            ).start()

    def _send_append_entries_to_peer(self, peer_id: int, host: str, port: int):
        with self._lock:
            if self.state != 'leader':
                return
            next_idx = self.next_index.get(peer_id, len(self.log) + 1)
            prev_log_index = next_idx - 1
            if prev_log_index > 0 and prev_log_index <= len(self.log):
                prev_log_term = self.log[prev_log_index - 1].term
            else:
                prev_log_term = 0
            entries = list(self.log[next_idx - 1:])
            msg = AppendEntries(
                term=self.current_term,
                leaderId=self.node_id,
                prevLogIndex=prev_log_index,
                prevLogTerm=prev_log_term,
                entries=entries,
                leaderCommit=self.commit_index,
            )

        self._transport.send(host, port, msg.to_dict())

    # 
    # Disparo de mensagens
    # 

    def _on_message(self, raw: Dict, reply_fn):
        msg = parse_message(raw)

        if isinstance(msg, RequestVote):
            self._handle_request_vote(msg)
        elif isinstance(msg, RequestVoteReply):
            self._handle_request_vote_reply(msg)
        elif isinstance(msg, AppendEntries):
            self._handle_append_entries(msg)
        elif isinstance(msg, AppendEntriesReply):
            self._handle_append_entries_reply(msg)
        elif isinstance(raw, dict) and raw.get('type') == 'ClientRequest':
            self._handle_client_request(raw.get('command'), reply_fn)
        elif isinstance(raw, dict) and raw.get('type') == 'GetScoreboard':
            self._handle_get_scoreboard(reply_fn)
        else:
            logger.warning(f'Mensagem desconhecida: {raw.get("type")}')

    # 
    # RPC 
    # 

    def _handle_request_vote(self, msg: RequestVote):
        vote_granted = False
        with self._lock:
            if msg.term > self.current_term:
                self._become_follower(msg.term)

            if msg.term >= self.current_term:
                can_vote = (self.voted_for is None or
                            self.voted_for == msg.candidateId)
                last_log_index = len(self.log)
                last_log_term = self.log[-1].term if self.log else 0
                # Raft §5.4.1: candidate's log must be at least as up-to-date.
                candidate_ok = (
                    msg.lastLogTerm > last_log_term or
                    (msg.lastLogTerm == last_log_term and
                     msg.lastLogIndex >= last_log_index)
                )
                if can_vote and candidate_ok:
                    vote_granted = True
                    self.voted_for = msg.candidateId
                    self._save_persistent_state()
                    self._reset_election_timer()

            reply = RequestVoteReply(
                term=self.current_term,
                voteGranted=vote_granted,
                voterId=self.node_id,
            )
            candidate_addr = self.peers.get(msg.candidateId)
            self._log(
                f'RequestVote de {msg.candidateId} term={msg.term} '
                f'| voto={vote_granted}'
            )

        if candidate_addr:
            self._transport.send(candidate_addr[0], candidate_addr[1], reply.to_dict())

    def _handle_request_vote_reply(self, msg: RequestVoteReply):
        with self._lock:
            if msg.term > self.current_term:
                self._become_follower(msg.term)
                return

            if self.state != 'candidate' or msg.term != self.current_term:
                return

            if msg.voteGranted:
                self.votes_received.add(msg.voterId)
                self._log(
                    f'voto recebido de {msg.voterId} '
                    f'| total={len(self.votes_received)}'
                )

            # Maioria absoluta ([(N+1)//2 + 1] de N) para permitir número par de nodos.
            quorum = (len(self.peers) + 1) // 2 + 1
            if len(self.votes_received) >= quorum:
                self._become_leader()

    def _handle_append_entries(self, msg: AppendEntries):
        reply: Optional[AppendEntriesReply] = None

        with self._lock:
            if msg.term > self.current_term:
                self._become_follower(msg.term)

            if msg.term < self.current_term:
                reply = AppendEntriesReply(
                    term=self.current_term, success=False,
                    followerId=self.node_id, matchIndex=0,
                )
            else:
                # Mensagem válida do líder
                self.current_leader = msg.leaderId
                if self.state != 'follower':
                    self._become_follower(msg.term)
                else:
                    self._reset_election_timer()

                # Consistência de log
                if msg.prevLogIndex > len(self.log):
                    # Contabilizar entradas faltantes 
                    reply = AppendEntriesReply(
                        term=self.current_term, success=False,
                        followerId=self.node_id, matchIndex=len(self.log),
                    )
                elif (msg.prevLogIndex > 0 and
                      self.log[msg.prevLogIndex - 1].term != msg.prevLogTerm):
                    # Conflito de terms: descarta a partir do índice com conflito
                    self.log = self.log[:msg.prevLogIndex - 1]
                    self._save_persistent_state()
                    reply = AppendEntriesReply(
                        term=self.current_term, success=False,
                        followerId=self.node_id, matchIndex=len(self.log),
                    )
                else:
                    # Inserir logs sobrescrevendo conflitos
                    for i, entry in enumerate(msg.entries):
                        log_idx = msg.prevLogIndex + i + 1
                        if log_idx <= len(self.log):
                            if self.log[log_idx - 1].term != entry.term:
                                self.log = self.log[:log_idx - 1]
                                self.log.append(entry)
                        else:
                            self.log.append(entry)

                    if msg.entries:
                        self._save_persistent_state()
                        self._log(
                            f'AppendEntries de {msg.leaderId} '
                            f'| +{len(msg.entries)} entr. '
                            f'| log_len={len(self.log)}'
                        )

                    # Avançar commitIndex
                    if msg.leaderCommit > self.commit_index:
                        self.commit_index = min(msg.leaderCommit, len(self.log))
                        self._apply_committed_entries()

                    reply = AppendEntriesReply(
                        term=self.current_term, success=True,
                        followerId=self.node_id, matchIndex=len(self.log),
                    )

            leader_addr = self.peers.get(msg.leaderId)

        if reply and leader_addr:
            self._transport.send(leader_addr[0], leader_addr[1], reply.to_dict())

    def _handle_append_entries_reply(self, msg: AppendEntriesReply):
        with self._lock:
            if msg.term > self.current_term:
                self._become_follower(msg.term)
                return

            if self.state != 'leader':
                return

            fid = msg.followerId
            if msg.success:
                self.match_index[fid] = max(
                    self.match_index.get(fid, 0), msg.matchIndex
                )
                self.next_index[fid] = self.match_index[fid] + 1
                self._advance_commit_index()
            else:
                # Back up nextIndex using the follower's hint.
                self.next_index[fid] = max(1, msg.matchIndex + 1)

    #
    # Requisições de clientes
    # 

    def _handle_client_request(self, command: Any, reply_fn):
        with self._lock:
            is_leader = self.state == 'leader'
            leader_id = self.current_leader

        if not is_leader:
            reply_fn({
                'type': 'ClientResponse',
                'success': False,
                'leaderId': leader_id,
                'error': 'not leader',
            })
            return

        # submit() é bloqueante, então é chamado fora do lock
        success, result = self.submit(command)

        if success:
            reply_fn({'type': 'ClientResponse', 'success': True, 'result': result})
        else:
            reply_fn({
                'type': 'ClientResponse',
                'success': False,
                'leaderId': self.current_leader,
                'error': 'submit falhou (liderança perdida ou timeout)',
            })

    def _handle_get_scoreboard(self, reply_fn):
        """Retorna o estado atual sem passar pelo consenso"""
        with self._lock:
            # Re-aplica todas as entradas comprometidas para reconstuir o
            # estado atual.
            scoreboard_fn = getattr(self, '_get_scoreboard_fn', None)
        if scoreboard_fn:
            reply_fn({'type': 'Scoreboard', 'data': scoreboard_fn()})
        else:
            reply_fn({'type': 'Scoreboard', 'data': {}})

    # 
    # Transição de estados (deve ser chamado com lock)
    # 

    def _become_follower(self, term: int):
        old_state = self.state
        self.state = 'follower'
        self.current_term = term
        self.voted_for = None
        self._save_persistent_state()
        self._cancel_heartbeat_timer()

        # Falha submissões pendentes para que clientes não permeneçam bloqueados indefinidamente
        for idx, event in list(self._pending.items()):
            self._pending_results[idx] = _LOST_LEADERSHIP
            event.set()
        self._pending.clear()

        if old_state != 'follower':
            self._log(f'tornou-se FOLLOWER | term={term}')

        self._reset_election_timer()

    def _become_leader(self):
        """Must be called with lock held."""
        self.state = 'leader'
        self.current_leader = self.node_id
        self._cancel_election_timer()

        last = len(self.log)
        for pid in self.peers:
            self.next_index[pid] = last + 1
            self.match_index[pid] = 0
        # Líder contabiliza a si mesmo para quórum
        self.match_index[self.node_id] = last

        self._log(f'tornou-se LÍDER | term={self.current_term}')
        self._start_heartbeat_timer()

    # 
    # Comprometer e aplicar (requer lock)
    # 

    def _advance_commit_index(self):
        """
        Líder: encontrar o maior N tal que N > commitIndex, 
        log[N].term == currentTerm e um quórum de matchIndex >= N.
        """
        total = len(self.peers) + 1
        quorum = total // 2 + 1

        for n in range(len(self.log), self.commit_index, -1):
            if self.log[n - 1].term != self.current_term:
                continue
            count = sum(1 for m in self.match_index.values() if m >= n)
            if count >= quorum:
                self.commit_index = n
                self._log(f'quórum atingido | commitIndex={self.commit_index}')
                self._apply_committed_entries()
                break

    def _apply_committed_entries(self):
        """Aplica todas as entradas até commitIndex."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            result = self.state_machine(entry.command)
            self._log(
                f'APLICANDO | index={self.last_applied} '
                f'| cmd={entry.command} | result={result}'
            )
            # Notifica se há clientes aguardando por esta entrada
            if entry.index in self._pending:
                self._pending_results[entry.index] = result
                self._pending[entry.index].set()
                del self._pending[entry.index]

    # 
    # Logging
    # 

    def _log(self, msg: str):
        state_label = self.state.upper().ljust(9)
        logger.info(
            f'[NÓ {self.node_id} | {state_label} | term={self.current_term}] {msg}'
        )
