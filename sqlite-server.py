#!/usr/bin/env python3
"""
Usage: sqlite-server PORT
"""

import base64
import socket
import sqlite3
import sys


def send_data(peer_file, data):
    print(base64.b64encode(data).decode('ascii'), file=peer_file)

def recv_data(peer_file):
    blob = next(peer_file)
    return base64.b64decode(blob).decode('utf-8')


def execute_query(peer_recv, peer_send, cursor):
    peer_send.flush()
    query = recv_data(peer_recv)
    try:
        cursor.execute(query)
    except sqlite3.Error as err:
        print('ERROR', file=peer_send)
        send_data(peer_send, (str(err) or 'Unknown error').encode('utf-8'))
        return

    rowcount = cursor.rowcount
    metadata = cursor.description

    if metadata is not None:
        print('METADATA', file=peer_send)
        print(len(metadata), file=peer_send)
        for col in metadata:
            send_data(peer_send, col[0].encode('utf-8'))
            send_data(peer_send, str(col[1] or 'DYNAMIC').encode('utf-8'))

        for row in cursor:
            print('ROW', file=peer_send)
            for col in row:
                col_bytes = col
                if col is None:
                    col_bytes = '<null>'
                elif isinstance(col, str):
                    col_bytes = col.encode('utf-8')
                elif isinstance(col, (int, float)):
                    col_bytes = str(col).encode('utf-8')
                else:
                    # Render binary data as base64 to avoid messing
                    # up the other end's terminal
                    col_bytes = base64.b64encode(col).encode('utf-8')

                send_data(peer_send, col_bytes)

        print('END', file=peer_send)

    else:
        print('AFFECTED', file=peer_send)
        print(rowcount, file=peer_send)


def prepare_query(peer_recv, peer_send, cursor):
    peer_send.flush()
    metadata_query = recv_data(peer_recv)
    query = 'SELECT * FROM (' + metadata_query + ') LIMIT 0'
    try:
        cursor.execute(query)
    except sqlite3.Error as err:
        print('ERROR', file=peer_send)
        send_data(peer_send, (str(err) or 'Unknown error').encode('utf-8'))
        return

    print('METADATA', file=peer_send)
    print(len(cursor.description), file=peer_send)
    for col in cursor.description:
        send_data(peer_send, col[0].encode('utf-8'))
        send_data(peer_send, str(col[1] or 'DYNAMIC').encode('utf-8'))


def main(port):
    db = sqlite3.connect('db', isolation_level=None)
    cursor = db.cursor()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('', port))
    server.listen(1)
    (peer, peer_info) = server.accept()
    peer_recv = peer.makefile(buffering=1, encoding='ascii', newline='\n')
    peer_send = peer.makefile(mode='w', buffering=1, encoding='ascii', newline='\n')
    while True:
        peer_send.flush()

        try:
            command = next(peer_recv).strip()
        except StopIteration:
            break

        if command == 'EXECUTE':
            print('OK', file=peer_send)
            execute_query(peer_recv, peer_send, cursor)
        elif command == 'PREPARE':
            print('OK', file=peer_send)
            prepare_query(peer_recv, peer_send, cursor)
        else:
            print('ERROR', file=peer_send)
            send_data(peer_send, b'Illegal command')

    db.close()


if __name__ == '__main__':
    port = None
    try:
        port = int(sys.argv[1])
    except (IndexError, ValueError):
        print(__doc__)

    if port is not None:
        main(port)
