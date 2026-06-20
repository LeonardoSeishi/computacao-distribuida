#!/usr/bin/env python3
"""
main.py — Entry point for a single Raft node.

Usage:
  python main.py <node_id> [--nodes N] [--host HOST] [--base-port PORT] [--data-dir DIR]

Examples:
  python main.py 1               # node 1 of a 3-node cluster on localhost
  python main.py 2 --nodes 5    # node 2 of a 5-node cluster
"""

import argparse
import logging
import signal
import sys
import time

from app.quiz import QuizApp
from raft.node import RaftNode


def setup_logging(node_id: int):
    fmt = '%(asctime)s.%(msecs)03d %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt='%H:%M:%S',
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    # Suppress verbose debug from lower layers.
    logging.getLogger('raft.transport').setLevel(logging.WARNING)


def parse_args():
    p = argparse.ArgumentParser(description='Raft node + quiz app')
    p.add_argument('node_id', type=int, help='Unique node ID (1-based)')
    p.add_argument('--nodes', type=int, default=3,
                   help='Total number of nodes in the cluster (default: 3)')
    p.add_argument('--host', default='127.0.0.1',
                   help='Bind host (default: 127.0.0.1)')
    p.add_argument('--base-port', type=int, default=5000,
                   help='Base port; node N listens on base_port + N (default: 5000)')
    p.add_argument('--data-dir', default='./data',
                   help='Directory for persistent state (default: ./data)')
    return p.parse_args()


def main():
    args = parse_args()
    node_id = args.node_id
    host = args.host
    port = args.base_port + node_id

    setup_logging(node_id)

    # Build peer map: every node except ourselves.
    peers = {
        i: (host, args.base_port + i)
        for i in range(1, args.nodes + 1)
        if i != node_id
    }

    quiz = QuizApp()

    node = RaftNode(
        node_id=node_id,
        peers=peers,
        host=host,
        port=port,
        state_machine=quiz.apply,
        data_dir=args.data_dir,
    )

    # Wire the scoreboard query into the node so GetScoreboard RPCs work.
    node._get_scoreboard_fn = quiz.get_scoreboard

    node.start()

    print(f'[NÓ {node_id}] rodando em {host}:{port} | peers: {peers}')
    print(f'[NÓ {node_id}] perguntas do quiz: python client.py --questions')
    print(f'[NÓ {node_id}] Ctrl+C para encerrar')

    def shutdown(sig, frame):
        print(f'\n[NÓ {node_id}] encerrando...')
        node.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    while True:
        time.sleep(1)


if __name__ == '__main__':
    main()
