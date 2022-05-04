import os
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
        s.listen(4)
        self.socket = s
        self.task_map = {}
        self.stop = threading.Event()
        self.stop.clear()

    def run(self):
        while not self.stop.is_set():
            c, _ = self.socket.accept()
            t = threading.Thread(target=self.handle, args=(c, ))
            t.start()
        self.socket.close()

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


class TestPipeline(unittest.TestCase):

    def setUp(self) -> None:
        self.test_data = pathlib.Path(__file__).parent / 'data'

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
        ts.stop.set()
        c.close()

    def test_pipeline_run(self):
        ts = TaskServer()
        ts.start()

        pwd = os.getcwd()
        os.chdir(self.test_data.parent)
        pipeline_yaml = load_pipeline_yaml(self.test_data / 'test_pipeline.yaml')
        pipeline = PipeLine(pipeline_yaml)

        pipeline.run()
        os.chdir(pwd)
        ts.stop.set()


if __name__ == '__main__':
    unittest.main()
