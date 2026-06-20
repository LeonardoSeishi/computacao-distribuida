"""
app/quiz.py — Quiz state machine applied on top of the Raft log.

The state is a simple scoreboard: {player_name: total_points}.
Every committed Raft entry carries a command dict:
  {"player": "<name>", "points": <int>}

The QuizApp.apply() method is passed as the state_machine callback to
RaftNode.  It is called exactly once per committed entry, in order, on
every node — guaranteeing that all nodes converge to the same scoreboard.
"""

import threading
from typing import Any, Dict


QUESTIONS = [
    {'id': 1, 'text': 'Qual protocolo o Raft usa para replicar entradas?',
     'answer': 'AppendEntries', 'points': 10},
    {'id': 2, 'text': 'Quantos nós toleram 1 falha no Raft?',
     'answer': '3', 'points': 10},
    {'id': 3, 'text': 'O que acontece quando o líder não recebe maioria?',
     'answer': 'eleição', 'points': 15},
    {'id': 4, 'text': 'Qual campo do log garante a ordem total?',
     'answer': 'index', 'points': 10},
    {'id': 5, 'text': 'Como se chama o estado transitório na eleição?',
     'answer': 'candidate', 'points': 10},
]


class QuizApp:
    """Replicated quiz scoreboard driven by Raft."""

    def __init__(self):
        self._scoreboard: Dict[str, int] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Raft state machine callback
    # ------------------------------------------------------------------

    def apply(self, command: Any) -> Dict[str, int]:
        """
        Called by RaftNode for each committed log entry.
        command must be {"player": str, "points": int}.
        Returns a snapshot of the scoreboard after applying the command.
        """
        if not isinstance(command, dict):
            return self.get_scoreboard()

        player = command.get('player')
        points = int(command.get('points', 0))

        if player:
            with self._lock:
                self._scoreboard[player] = self._scoreboard.get(player, 0) + points

        return self.get_scoreboard()

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def get_scoreboard(self) -> Dict[str, int]:
        with self._lock:
            return dict(sorted(self._scoreboard.items(),
                               key=lambda kv: kv[1], reverse=True))

    def check_answer(self, question_id: int, answer: str) -> int:
        """Return points if correct, 0 otherwise."""
        for q in QUESTIONS:
            if q['id'] == question_id:
                if answer.strip().lower() == q['answer'].strip().lower():
                    return q['points']
                return 0
        return 0

    @staticmethod
    def list_questions() -> list:
        return [{'id': q['id'], 'text': q['text']} for q in QUESTIONS]
