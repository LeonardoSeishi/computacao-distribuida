"""
app/quiz.py — Quiz state machine applied on top of the Raft log.

Command format (new):
  {"player": "<name>", "question_id": <int>, "answer": "<letter>"}

The Raft log is the source of truth: apply() validates the answer and
decides points.  First correct answer earns full points; subsequent
correct answers earn half (integer division).  All nodes apply the same
log in the same order, so all scoreboards converge identically.

Legacy format {"player": "<name>", "points": <int>} is still accepted
for --raw submissions.
"""

import threading
from typing import Any, Dict


QUESTIONS = [
    {
        'id': 1,
        'text': 'Qual protocolo o Raft usa para replicar entradas?',
        'options': {'a': 'RequestVote', 'b': 'AppendEntries', 'c': 'HeartBeat', 'd': 'Commit'},
        'answer': 'b',
        'points': 10,
    },
    {
        'id': 2,
        'text': 'Quantos nós toleram 1 falha no Raft?',
        'options': {'a': '2', 'b': '4', 'c': '3', 'd': '5'},
        'answer': 'c',
        'points': 10,
    },
    {
        'id': 3,
        'text': 'O que acontece quando o líder não recebe maioria?',
        'options': {'a': 'commit', 'b': 'rollback', 'c': 'snapshot', 'd': 'eleição'},
        'answer': 'd',
        'points': 15,
    },
    {
        'id': 4,
        'text': 'Qual campo do log garante a ordem total?',
        'options': {'a': 'term', 'b': 'leader', 'c': 'index', 'd': 'timestamp'},
        'answer': 'c',
        'points': 10,
    },
    {
        'id': 5,
        'text': 'Como se chama o estado transitório na eleição?',
        'options': {'a': 'follower', 'b': 'candidate', 'c': 'leader', 'd': 'observer'},
        'answer': 'b',
        'points': 10,
    },
    {
        'id': 6,
        'text': 'Qual será o dia da prova de Computação Distribuída?',
        'options': {'a': '01/07', 'b': '23/06', 'c': '24/06', 'd': '30/06'},
        'answer': 'd',
        'points': 10,
    },
]


class QuizApp:
    """Replicated quiz scoreboard driven by Raft."""

    def __init__(self):
        self._scoreboard: Dict[str, int] = {}
        # question_id -> name of the first player who answered correctly
        self._answered: Dict[int, str] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Raft state machine callback
    # ------------------------------------------------------------------

    def apply(self, command: Any) -> Dict:
        """
        Called by RaftNode for each committed log entry, in log order.

        New format:  {"player": str, "question_id": int, "answer": str}
        Legacy format: {"player": str, "points": int}

        Returns a result dict:
          {"scoreboard": {...}, "correct": bool, "points_awarded": int, "first": bool}
        """
        if not isinstance(command, dict):
            return {'scoreboard': self.get_scoreboard(), 'correct': False,
                    'points_awarded': 0, 'first': False}

        # ── Legacy --raw path ────────────────────────────────────────────
        if 'question_id' not in command:
            player = command.get('player')
            points = int(command.get('points', 0))
            if player:
                with self._lock:
                    self._scoreboard[player] = self._scoreboard.get(player, 0) + points
            return {'scoreboard': self.get_scoreboard(), 'correct': True,
                    'points_awarded': points, 'first': False}

        # ── New quiz path ────────────────────────────────────────────────
        player = command.get('player')
        question_id = int(command.get('question_id', 0))
        answer = str(command.get('answer', '')).strip().lower()

        question = next((q for q in QUESTIONS if q['id'] == question_id), None)
        if not question or not player:
            return {'scoreboard': self.get_scoreboard(), 'correct': False,
                    'points_awarded': 0, 'first': False}

        correct = answer == question['answer']
        if not correct:
            return {'scoreboard': self.get_scoreboard(), 'correct': False,
                    'points_awarded': 0, 'first': False}

        with self._lock:
            first = question_id not in self._answered
            if first:
                self._answered[question_id] = player
                points = question['points']
            else:
                points = question['points'] // 2

            self._scoreboard[player] = self._scoreboard.get(player, 0) + points

        return {
            'scoreboard': self.get_scoreboard(),
            'correct': True,
            'points_awarded': points,
            'first': first,
        }

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def get_scoreboard(self) -> Dict[str, int]:
        with self._lock:
            return dict(sorted(self._scoreboard.items(),
                               key=lambda kv: kv[1], reverse=True))

    @staticmethod
    def list_questions() -> list:
        return [{'id': q['id'], 'text': q['text'], 'options': q['options']}
                for q in QUESTIONS]
