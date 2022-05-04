import re
import time
import unittest
import threading
import socket
from random import randint

from running_snail.pipeline import *
from running_snail.action import load_pipeline_yaml


class TaskServer(threading.Thread):

    def __init__(self):
        super(TaskServer, self).__init__()
        self.setDaemon(True)
        self.address = ('127.0.0.1', 2999)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(self.address)
        s.listen()
        self.socket = s
        self.task_map = {}

    def run(self):
        cache = b''
        c, ip = self.socket.accept()
        while True:
            try:
                cache += c.recv(1080)
                sp = cache.find(b'\n')
                if sp != -1:
                    cmd, cache = cache[:sp], cache[sp + 1:]
                    self.action(cmd, c)
            except socket.error:
                self.socket.close()

    def action(self, cmd, client):
        if b'post' in cmd:
            task_id = str(randint(0, 999999))
            self.task_map[task_id] = randint(0, 4)
            client.send(task_id.encode() + b'\n')

        if b'query' in cmd:
            for t, v in self.task_map.items():
                if v > -3:
                    msg = '%s\t%s\n' % (t, v)
                    self.task_map[t] -= 1
                    client.send(msg.encode())
            client.send(b'\n')


class TestPipeline(unittest.TestCase):

    def setUp(self) -> None:
        self.test_data = pathlib.Path(__file__).parent

    def test_task_server(self):
        ts = TaskServer()
        ts.start()
        c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        c.connect(ts.address)
        c.send(b'post\n')
        c.recv(100)
        self.assertEqual(1, len(ts.task_map))
        msg = '\n'
        for i in range(10):
            c.send(b'query\n')
            msg = c.recv(100)
        self.assertEqual(b'\n', msg)
        c.close()

    def test_pipeline_run(self):
        pipeline_yaml = load_pipeline_yaml(self.test_data / 'test_pipeline.yaml')
        pipeline = PipeLine(pipeline_yaml)

        pipeline.run()


if __name__ == '__main__':
    unittest.main()