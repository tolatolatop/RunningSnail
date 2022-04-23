from typing import List, Optional

from pydantic import BaseModel


class Task(BaseModel):
    name: str
    status: str = 'wait'


class Env(BaseModel):
    value: str


class PotcarEnv(Env):
    potcar_dir: str


class VaspTask(Task):
    incar: str
    poscar: str
    potcar: PotcarEnv


class Runner(Task):
    push_sh: str


class Jobs(Task):
    task_list: List[Task] = []


class FileCollector(BaseModel):
    name: str
    path: str


class VaspJobs(Jobs):
    task_list: List[VaspTask] = []
    input_collector_list: List[FileCollector] = []
    result_collector_list: List[FileCollector] = []


class PostJobs(Jobs):
    name: str
