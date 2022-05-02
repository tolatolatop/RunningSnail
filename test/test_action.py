import pathlib
import unittest
from running_snail.action import *


class TestAction(unittest.TestCase):

    def setUp(self) -> None:
        self.test_data = pathlib.Path('./data')

    def test_named(self):
        pos = generate_task_id('base')
        self.assertEqual('base_0000', next(pos))

    def test_push_task(self):
        cmd = ['findstr', '123']
        task_id = push_task('.', cmd, '123455')
        self.assertEqual('123455', task_id)

    def test_load_files(self):
        files = load_files('.', './data/*.yaml')
        print(files)

    def test_create_input_args_batch(self):
        input_args = {
            'POSCAR': ['POSCAR_001', 'POSCAR_002', 'POSCAR_003'],
            'INCAR': ['INCAR']
        }
        input_args_batch = create_input_args_batch(input_args)
        result = (
            (('POSCAR', 'POSCAR_001'), ('INCAR', 'INCAR')),
            (('POSCAR', 'POSCAR_002'), ('INCAR', 'INCAR')),
            (('POSCAR', 'POSCAR_003'), ('INCAR', 'INCAR'))
        )
        self.assertEqual(result, input_args_batch)


if __name__ == '__main__':
    unittest.main()
