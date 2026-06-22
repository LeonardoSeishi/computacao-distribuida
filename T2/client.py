#!/usr/bin/env python3
"""
client.py — Jogador CLI.

Como executar:
  python client.py --node 1 --player Alice --question 1 --answer b
  python client.py --node 1 --questions          # listar perguntas
  python client.py --node 1 --scoreboard         # mostrar placar atual
  python client.py --node 1 --raw '{"player":"Bob","points":10}'  # enviar comando diretamente via JSON

Respostas são enviadas indicando a alternativa (a, b, c , d).  O cálculo de pontuação ocorre na máquina de estados Raft,
onde a ordem de logs determina quem respondeu primeiro.

Se o cliente contatado não é o líder, ele tenta novamente
redirecionando para o líder indicado na resposta (até 3 redirecionamentos).

"""

import argparse
import json
import socket
import sys

BASE_PORT = 5000
HOST = '127.0.0.1'
TIMEOUT = 10.0


def send_request(host: str, port: int, msg: dict, timeout: float = TIMEOUT) -> dict | None:
    try:
        data = json.dumps(msg).encode()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((host, port))
            s.sendall(data)
            s.shutdown(socket.SHUT_WR)
            reply = b''
            while True:
                chunk = s.recv(65536)
                if not chunk:
                    break
                reply += chunk
        return json.loads(reply.decode()) if reply else None
    except ConnectionRefusedError:
        print(f'  ✗ Nó {port - BASE_PORT} não está acessível ({host}:{port})')
        return None
    except Exception as e:
        print(f'  ✗ Erro: {e}')
        return None


def submit_command(node_id: int, command: dict, max_redirects: int = 3) -> dict | None:
    """Submit a command, following leader redirects automatically."""
    port = BASE_PORT + node_id
    for attempt in range(max_redirects + 1):
        print(f'  -> conectando ao nó {node_id} ({HOST}:{port})...')
        resp = send_request(HOST, port, {'type': 'ClientRequest', 'command': command})
        if resp is None:
            return None
        if resp.get('success'):
            return resp
        leader_id = resp.get('leaderId')
        if leader_id is not None:
            print(f'  -> nó {node_id} não é o líder | redirecionando para nó {leader_id}...')
            node_id = leader_id
            port = BASE_PORT + leader_id
        else:
            print(f'  - Falha: {resp.get("error", "desconhecido")}')
            return resp
    print('  ✗ Não foi possível encontrar o líder após redirecionamentos')
    return None


def get_scoreboard(node_id: int) -> dict | None:
    port = BASE_PORT + node_id
    return send_request(HOST, port, {'type': 'GetScoreboard'})


def parse_args():
    p = argparse.ArgumentParser(description='Quiz client para o cluster Raft')
    p.add_argument('--node', type=int, default=1,
                   help='Nó inicial (default: 1)')
    p.add_argument('--player', type=str, help='Nome do jogador')
    p.add_argument('--question', type=int, help='ID da pergunta (1-5)')
    p.add_argument('--answer', type=str, help='Resposta para a pergunta')
    p.add_argument('--questions', action='store_true',
                   help='Listar as perguntas do quiz')
    p.add_argument('--scoreboard', action='store_true',
                   help='Exibir o placar atual')
    p.add_argument('--raw', type=str,
                   help='Enviar um comando JSON bruto, ex: \'{"player":"X","points":10}\'')
    return p.parse_args()


def main():
    args = parse_args()

    # Listar perguntas
    if args.questions:
        from app.quiz import QuizApp
        print('\nPerguntas do Quiz:')
        for q in QuizApp.list_questions():
            print(f"  [{q['id']}] {q['text']}")
            for letter, text in q['options'].items():
                print(f"        {letter}) {text}")
        print()
        return

    # Mostrar placar
    if args.scoreboard:
        resp = get_scoreboard(args.node)
        if resp and 'data' in resp:
            data = resp['data']
            print('\nPlacar atual:')
            if data:
                for rank, (player, pts) in enumerate(data.items(), 1):
                    print(f'  {rank}. {player}: {pts} pts')
            else:
                print('  (vazio)')
        else:
            print('  Não foi possível obter o placar.')
        print()
        return

    # Enviar comando diretamente via JSON
    if args.raw:
        try:
            command = json.loads(args.raw)
        except json.JSONDecodeError as e:
            print(f'JSON inválido: {e}')
            sys.exit(1)
        resp = submit_command(args.node, command)
        if resp and resp.get('success'):
            print(f'\nPlacar: {resp["result"]}')
        return

    # Resposta do Quiz
    if not all([args.player, args.question, args.answer]):
        print('Use --player, --question e --answer a|b|c|d  (ou --questions / --scoreboard / --raw)')
        sys.exit(1)

    command = {
        'player': args.player,
        'question_id': args.question,
        'answer': args.answer.strip().lower(),
    }
    resp = submit_command(args.node, command)

    if not resp:
        return

    if not resp.get('success'):
        print(f'Falha: {resp.get("error")}')
        return

    result = resp.get('result', {})
    correct = result.get('correct', False)
    points = result.get('points_awarded', 0)
    first = result.get('first', False)
    scoreboard = result.get('scoreboard', {})

    if not correct:
        print(f'\n Resposta incorreta para a questão {args.question}.')
    elif first:
        print(f'\n Primeiro a acertar! +{points} pontos para {args.player}')
    else:
        print(f'\n Correto, mas não foi o primeiro. +{points} pontos para {args.player} (metade)')

    print(f'Placar: {scoreboard}')


if __name__ == '__main__':
    main()
