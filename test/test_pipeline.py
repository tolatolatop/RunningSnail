import unittest
from running_snail.pipeline import *
from running_snail.action import load_pipeline_yaml


class TestPipeline(unittest.TestCase):

    def setUp(self) -> None:
        self.test_data = pathlib.Path('./data')

    def test_model(self):
        pipeline_yaml = load_pipeline_yaml(self.test_data / 'pipeline.yaml')
        pipeline = PipeLine(pipeline_yaml)

    def test_pipeline_run(self):
        pipeline_yaml = load_pipeline_yaml(self.test_data / 'test_pipeline.yaml')
        pipeline = PipeLine(pipeline_yaml)

        pipeline.run()


if __name__ == '__main__':
    unittest.main()
