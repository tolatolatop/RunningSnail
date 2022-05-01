import pathlib
import unittest
from running_snail.action import *


class TestAction(unittest.TestCase):

    def setUp(self) -> None:
        self.test_data = pathlib.Path('./data')

    def test_model(self):
        pipeline_yaml = load_pipeline_yaml(self.test_data / 'pipeline.yaml')
        print(pipeline_yaml)

    def test_named(self):
        pos = generate_task_id('base')
        self.assertEqual('base_0000', next(pos))

    def test_push_task(self):
        cmd = ['findstr', '123']
        task_id = push_task('.', cmd, '123455')
        self.assertEqual('123455', task_id)


if __name__ == '__main__':
    unittest.main()
