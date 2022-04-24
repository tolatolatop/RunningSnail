import unittest
from running_snail.model import *


class TestModel(unittest.TestCase):

    def test_model(self):
        vasp_jobs = VaspJobs(name="Structure Optimization")
        pipeline = Jobs(name='test_sub_job', task_list=[vasp_jobs])
        print(pipeline.json())

        potcar = PotcarEnv(value='path')
        self.assertEqual('path', potcar.value)


if __name__ == '__main__':
    unittest.main()
