from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class LogEntry:
    term: int
    index: int
    command: Any

    def to_dict(self) -> Dict:
        return {'term': self.term, 'index': self.index, 'command': self.command}

    @classmethod
    def from_dict(cls, d: Dict) -> 'LogEntry':
        return cls(term=d['term'], index=d['index'], command=d['command'])


@dataclass
class RequestVote:
    term: int
    candidateId: int
    lastLogIndex: int
    lastLogTerm: int

    def to_dict(self) -> Dict:
        return {
            'type': 'RequestVote',
            'term': self.term,
            'candidateId': self.candidateId,
            'lastLogIndex': self.lastLogIndex,
            'lastLogTerm': self.lastLogTerm,
        }

    @classmethod
    def from_dict(cls, d: Dict) -> 'RequestVote':
        return cls(
            term=d['term'],
            candidateId=d['candidateId'],
            lastLogIndex=d['lastLogIndex'],
            lastLogTerm=d['lastLogTerm'],
        )


@dataclass
class RequestVoteReply:
    term: int
    voteGranted: bool
    voterId: int

    def to_dict(self) -> Dict:
        return {
            'type': 'RequestVoteReply',
            'term': self.term,
            'voteGranted': self.voteGranted,
            'voterId': self.voterId,
        }

    @classmethod
    def from_dict(cls, d: Dict) -> 'RequestVoteReply':
        return cls(term=d['term'], voteGranted=d['voteGranted'], voterId=d['voterId'])


@dataclass
class AppendEntries:
    term: int
    leaderId: int
    prevLogIndex: int
    prevLogTerm: int
    entries: List[LogEntry]
    leaderCommit: int

    def to_dict(self) -> Dict:
        return {
            'type': 'AppendEntries',
            'term': self.term,
            'leaderId': self.leaderId,
            'prevLogIndex': self.prevLogIndex,
            'prevLogTerm': self.prevLogTerm,
            'entries': [e.to_dict() for e in self.entries],
            'leaderCommit': self.leaderCommit,
        }

    @classmethod
    def from_dict(cls, d: Dict) -> 'AppendEntries':
        return cls(
            term=d['term'],
            leaderId=d['leaderId'],
            prevLogIndex=d['prevLogIndex'],
            prevLogTerm=d['prevLogTerm'],
            entries=[LogEntry.from_dict(e) for e in d['entries']],
            leaderCommit=d['leaderCommit'],
        )


@dataclass
class AppendEntriesReply:
    term: int
    success: bool
    followerId: int
    matchIndex: int

    def to_dict(self) -> Dict:
        return {
            'type': 'AppendEntriesReply',
            'term': self.term,
            'success': self.success,
            'followerId': self.followerId,
            'matchIndex': self.matchIndex,
        }

    @classmethod
    def from_dict(cls, d: Dict) -> 'AppendEntriesReply':
        return cls(
            term=d['term'],
            success=d['success'],
            followerId=d['followerId'],
            matchIndex=d['matchIndex'],
        )


def parse_message(d: Dict):
    """Deserialize a raw dict into the appropriate message dataclass."""
    t = d.get('type')
    if t == 'RequestVote':
        return RequestVote.from_dict(d)
    elif t == 'RequestVoteReply':
        return RequestVoteReply.from_dict(d)
    elif t == 'AppendEntries':
        return AppendEntries.from_dict(d)
    elif t == 'AppendEntriesReply':
        return AppendEntriesReply.from_dict(d)
    return d
