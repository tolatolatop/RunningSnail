import pathlib
import unittest
from running_snail.action import *


class TestAction(unittest.TestCase):

    def setUp(self) -> None:
        self.test_data = pathlib.Path('./data')

    def test_model(self):
        pipeline_yaml = load_pipeline_yaml(self.test_data / 'pipeline.yaml')
        print(pipeline_yaml)


if __name__ == '__main__':
    unittest.main()
