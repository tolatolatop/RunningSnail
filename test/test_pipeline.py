import os
import unittest
import socket

from running_snail.pipeline import *
from running_snail.action import load_pipeline_yaml
from running_snail.task_server import TaskServer


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
        ts.stop()
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
        ts.stop()


if __name__ == '__main__':
    unittest.main()
