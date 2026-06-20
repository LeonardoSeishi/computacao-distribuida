"""
raft/node.py — Raft consensus state machine.

Implements:
  • Leader election (RequestVote / RequestVoteReply)
  • Log replication (AppendEntries / AppendEntriesReply)
  • Persistence of currentTerm, votedFor, log[] to disk
  • Public API: start(), stop(), submit(command) -> (success, result)

Thread model: all state mutations happen under self._lock (RLock).
Timers and network sends happen outside the lock to avoid blocking.
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

# Sentinel: submit() returns this when the node stepped down before commit.
_LOST_LEADERSHIP = object()


class RaftNode:
    def __init__(
        self,
        node_id: int,
        peers: Dict[int, Tuple[str, int]],   # {peer_id: (host, port)}
        host: str,
        port: int,
        state_machine: Callable[[Any], Any],  # called on each committed entry
        data_dir: str = './data',
    ):
        self.node_id = node_id
        self.peers = peers
        self.host = host
        self.port = port
        self.state_machine = state_machine
        self.data_dir = data_dir

        # ── Persistent state (written to disk before responding to RPCs) ──
        self.current_term: int = 0
        self.voted_for: Optional[int] = None
        self.log: List[LogEntry] = []       # 0-based list; Raft indices are 1-based

        # ── Volatile state (all nodes) ──
        self.commit_index: int = 0          # highest log index known to be committed
        self.last_applied: int = 0          # highest log index applied to state machine
        self.state: str = 'follower'        # 'follower' | 'candidate' | 'leader'
        self.current_leader: Optional[int] = None

        # ── Volatile state (candidates) ──
        self.votes_received: Set[int] = set()

        # ── Volatile state (leaders only) ──
        self.next_index: Dict[int, int] = {}   # peer_id -> next log index to send
        self.match_index: Dict[int, int] = {}  # peer_id -> highest replicated index

        # ── Pending client submits ──
        # index -> threading.Event; event is set when entry is committed (or lost)
        self._pending: Dict[int, threading.Event] = {}
        self._pending_results: Dict[int, Any] = {}

        self._lock = threading.RLock()
        self._election_timer: Optional[threading.Timer] = None
        self._heartbeat_timer: Optional[threading.Timer] = None

        self._transport = Transport(host, port, self._on_message)
        self._load_persistent_state()

    # ════════════════════════════════════════════════════════════════════
    # Public API
    # ════════════════════════════════════════════════════════════════════

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
        Submit a command to the replicated log.

        Returns (True, result) when the entry is committed and applied.
        Returns (False, leader_id_or_None) if this node is not the leader,
        or if leadership is lost before the entry commits.
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

        # Kick replication immediately (outside lock).
        self._send_append_entries_to_all()

        # Block until committed or timeout.
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

    # ════════════════════════════════════════════════════════════════════
    # Persistence
    # ════════════════════════════════════════════════════════════════════

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
        """Atomically write persistent state. Called with lock held."""
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

    # ════════════════════════════════════════════════════════════════════
    # Timers
    # ════════════════════════════════════════════════════════════════════

    def _reset_election_timer(self):
        """Cancel the current timer and start a new one. Lock not required."""
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

    # ════════════════════════════════════════════════════════════════════
    # Election
    # ════════════════════════════════════════════════════════════════════

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

        # Send outside lock; reset timer in case this election is inconclusive.
        for peer_id, (h, p) in peers_snapshot.items():
            threading.Thread(
                target=self._transport.send,
                args=(h, p, msg.to_dict()),
                daemon=True,
            ).start()

        self._reset_election_timer()

    # ════════════════════════════════════════════════════════════════════
    # Heartbeat / replication
    # ════════════════════════════════════════════════════════════════════

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

    # ════════════════════════════════════════════════════════════════════
    # Message dispatcher
    # ════════════════════════════════════════════════════════════════════

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

    # ════════════════════════════════════════════════════════════════════
    # RPC handlers
    # ════════════════════════════════════════════════════════════════════

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

            # Majority = (total_nodes // 2) + 1; total = peers + self
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
                # Valid message from current leader.
                self.current_leader = msg.leaderId
                if self.state != 'follower':
                    self._become_follower(msg.term)
                else:
                    self._reset_election_timer()

                # Consistency check.
                if msg.prevLogIndex > len(self.log):
                    # Missing entries — signal how many we have so leader backs up.
                    reply = AppendEntriesReply(
                        term=self.current_term, success=False,
                        followerId=self.node_id, matchIndex=len(self.log),
                    )
                elif (msg.prevLogIndex > 0 and
                      self.log[msg.prevLogIndex - 1].term != msg.prevLogTerm):
                    # Term conflict — delete from prevLogIndex onward.
                    self.log = self.log[:msg.prevLogIndex - 1]
                    self._save_persistent_state()
                    reply = AppendEntriesReply(
                        term=self.current_term, success=False,
                        followerId=self.node_id, matchIndex=len(self.log),
                    )
                else:
                    # Append entries, overwriting conflicts.
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

                    # Advance commitIndex.
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

    # ════════════════════════════════════════════════════════════════════
    # Client request handlers (called from transport thread)
    # ════════════════════════════════════════════════════════════════════

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

        # submit() blocks until committed — do it outside the lock.
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
        """Return current state without going through consensus (stale read ok)."""
        with self._lock:
            # Re-apply all committed entries up to last_applied to reconstruct
            # state — but state_machine is already applied incrementally, so
            # we just ask the app layer. We expose this via a callback set by
            # the application after construction.
            scoreboard_fn = getattr(self, '_get_scoreboard_fn', None)
        if scoreboard_fn:
            reply_fn({'type': 'Scoreboard', 'data': scoreboard_fn()})
        else:
            reply_fn({'type': 'Scoreboard', 'data': {}})

    # ════════════════════════════════════════════════════════════════════
    # State transitions (must be called with lock held)
    # ════════════════════════════════════════════════════════════════════

    def _become_follower(self, term: int):
        old_state = self.state
        self.state = 'follower'
        self.current_term = term
        self.voted_for = None
        self._save_persistent_state()
        self._cancel_heartbeat_timer()

        # Fail any pending submits so clients don't block until timeout.
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
        # Leader counts itself in quorum calculations.
        self.match_index[self.node_id] = last

        self._log(f'tornou-se LÍDER | term={self.current_term}')
        self._start_heartbeat_timer()

    # ════════════════════════════════════════════════════════════════════
    # Commit & apply (must be called with lock held)
    # ════════════════════════════════════════════════════════════════════

    def _advance_commit_index(self):
        """
        Leader: find the highest N > commitIndex such that
          log[N].term == currentTerm AND a majority has matchIndex >= N.
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
        """Apply all entries up to commitIndex to the state machine."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            result = self.state_machine(entry.command)
            self._log(
                f'APLICANDO | index={self.last_applied} '
                f'| cmd={entry.command} | result={result}'
            )
            # Notify waiting submit() call, if any.
            if entry.index in self._pending:
                self._pending_results[entry.index] = result
                self._pending[entry.index].set()
                del self._pending[entry.index]

    # ════════════════════════════════════════════════════════════════════
    # Logging helper
    # ════════════════════════════════════════════════════════════════════

    def _log(self, msg: str):
        state_label = self.state.upper().ljust(9)
        logger.info(
            f'[NÓ {self.node_id} | {state_label} | term={self.current_term}] {msg}'
        )
