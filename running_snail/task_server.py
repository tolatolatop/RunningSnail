import socket
import sys
import threading
from random import randint


class TaskServer(threading.Thread):

    def __init__(self):
        super(TaskServer, self).__init__()
        self.setDaemon(True)
        self.address = ('127.0.0.1', 2999)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(self.address)
        s.listen(4)
        self.socket = s
        self.task_map = {}

    def run(self):
        while True:
            try:
                c, _ = self.socket.accept()
                t = threading.Thread(target=self.handle, args=(c, ))
                t.start()
            except socket.error as e:
                break

    def stop(self):
        self.socket.close()
        self.join()

    def handle(self, client):
        cache = client.recv(1080)
        while True:
            try:
                sp = cache.find(b'\n')
                if sp != -1:
                    cmd, cache = cache[:sp], cache[sp + 1:]
                    self.action(cmd, client)
                cache += client.recv(1080)
            except socket.error:
                break

    def action(self, cmd, client):
        if b'post' in cmd:
            task_id = str(randint(0, 999999))
            self.task_map[task_id] = randint(0, 4)
            client.send(task_id.encode() + b'\n')

        if b'query' in cmd:
            msg = ''
            for t, v in self.task_map.items():
                if v > -3:
                    msg += '%s\t%s\n' % (t, v)
                    self.task_map[t] -= 1
            client.send(msg.encode() + b'\n')


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == 'server':
        ts = TaskServer()
        ts.start()
        ts.join()
    elif cmd == 'post':
        cmd = sys.stdin.read().encode()
        c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        c.connect(('127.0.0.1', 2999))
        c.send(cmd + b'\n')
        sys.stdout.write(c.recv(100).decode())
    elif cmd == 'query':
        c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        c.connect(('127.0.0.1', 2999))
        cmd = b'query'
        c.send(cmd + b'\n')
        sys.stdout.write(c.recv(100).decode())


