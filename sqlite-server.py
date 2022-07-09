#!/usr/bin/env python3
"""
Usage: sqlite-server HOST PORT
"""

import base64
import socket
import sqlite3
import sys


def send_data(server_file, data):
    print(base64.b64encode(data).decode('ascii'), file=server_file)

def recv_data(server_file):
    blob = next(server_file)
    return base64.b64decode(blob).decode('utf-8')


def execute_query(server_recv, server_send, cursor):
    server_send.flush()
    query = recv_data(server_recv)
    try:
        cursor.execute(query)
    except sqlite3.Error as err:
        print('ERROR', file=server_send)
        send_data(server_send, (str(err) or 'Unknown error').encode('utf-8'))
        return

    rowcount = cursor.rowcount
    metadata = cursor.description

    if metadata is not None:
        print('METADATA', file=server_send)
        print(len(metadata), file=server_send)
        for col in metadata:
            send_data(server_send, col[0].encode('utf-8'))
            send_data(server_send, str(col[1] or 'DYNAMIC').encode('utf-8'))

        page_row = 0
        for row in cursor:
            if page_row == 3:
                print('PAGE', file=server_send)
                server_send.flush()

                command = next(server_recv).strip()
                if command == 'MORE':
                    page_row = 0
                elif command == 'ABORT':
                    break
                else:
                    raise ValueError('Expected MORE or ABORT in response to PAGE')

            page_row += 1
            print('ROW', file=server_send)
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

                send_data(server_send, col_bytes)

        print('END', file=server_send)

    else:
        print('AFFECTED', file=server_send)
        print(rowcount, file=server_send)


def prepare_query(server_recv, server_send, cursor):
    server_send.flush()
    metadata_query = recv_data(server_recv)
    query = 'SELECT * FROM (' + metadata_query + ') LIMIT 0'
    try:
        cursor.execute(query)
    except sqlite3.Error as err:
        print('ERROR', file=server_send)
        send_data(server_send, (str(err) or 'Unknown error').encode('utf-8'))
        return

    print('METADATA', file=server_send)
    print(len(cursor.description), file=server_send)
    for col in cursor.description:
        send_data(server_send, col[0].encode('utf-8'))
        send_data(server_send, str(col[1] or 'DYNAMIC').encode('utf-8'))


def main(host, port):
    db = sqlite3.connect('db', isolation_level=None)
    cursor = db.cursor()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.connect((host, port))
    server_recv = server.makefile(buffering=1, encoding='ascii', newline='\n')
    server_send = server.makefile(mode='w', buffering=1, encoding='ascii', newline='\n')

    print('HELLO', file=server_send)
    send_data(server_send, b'sqlite')

    while True:
        server_send.flush()

        try:
            command = next(server_recv).strip()
        except StopIteration:
            break

        try:
            if command == 'EXECUTE':
                execute_query(server_recv, server_send, cursor)
            elif command == 'PREPARE':
                prepare_query(server_recv, server_send, cursor)
            else:
                break
        except Exception as err:
            print('Error:', err)
            break

    server.close()
    db.close()


if __name__ == '__main__':
    host = None
    port = None
    try:
        host = sys.argv[1]
        port = int(sys.argv[2])
    except (IndexError, ValueError):
        print(__doc__)

    if host is not None and port is not None:
        main(host, port)
