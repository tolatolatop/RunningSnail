from typing import List, Optional

from pydantic import BaseModel


class Task(BaseModel):
    name: str
    status: str = 'wait'


class TaskStatus(BaseModel):
    path: str
    task_id: str
    type: str
    status: str


class Env(BaseModel):
    potcar_dir: str = '/data/potcar'
    workspace: str = '/data'


class VaspTask(Task):
    incar: str
    poscar: str
    potcar: str


class Runner(BaseModel):
    push_sh: str = 'push.sh'


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


class PipeLine(BaseModel):
    env: Env
    runner: Runner
    jobs: List[Jobs]
