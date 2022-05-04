from running_snail import log
import pathlib
import time
from collections import OrderedDict
from typing import List, Dict
import logging

from . import model
from . import action

jobs_map = {
    'vaspjobs': model.VaspJobs,
    'postjobs': model.PostJobs
}

collector_map = {
    'filecollector': model.FileCollector
}

logger = logging.getLogger(__name__)


class PipeLine(object):

    def __init__(self, data: dict):
        # TODO: pipeline和vasp_task可以解耦 当前功能单一没意义
        self._meta_data = data

        if 'env' in self._meta_data.keys():
            env = model.Env(**self._meta_data['env'])
        else:
            env = model.Env()

        if 'runner' in self._meta_data.keys():
            runner = model.Runner(**self._meta_data['runner'])
        else:
            runner = model.Runner()

        jobs = self.load_jobs()

        self._model = model.PipeLine(
            env=env,
            runner=runner,
            jobs=jobs
        )

        self.copy_mode = True
        self.file_collections = {}
        self.task_status: Dict[str, model.TaskStatus] = OrderedDict()
        self.push_script_code = action.get_push_script_code(self._model.runner.post_sh)
        self._task_status_file = pathlib.Path(self._model.env.workspace) / '.pipeline' / 'task_status.yaml'

    def init(self):
        """
        有副作用的初始化过程

        :return:
        """
        action.init_pipeline_env(self._model.env)
        action.init_pipeline_runner(self._model.runner)

        task_status = action.load_task_status(self._task_status_file)
        for task_data in task_status:
            task_status = model.TaskStatus(**task_data)
            self.task_status[task_status.path] = task_status

    def load_jobs(self):
        jobs = []
        if 'jobs' in self._meta_data.keys():
            for sub_jobs_data in self._meta_data['jobs']:
                sub_jobs_cls = jobs_map[sub_jobs_data['type']]
                sub_jobs = sub_jobs_cls(
                    name=sub_jobs_data['name'],
                )
                if 'input_collector_list' in sub_jobs_data:
                    for collector_data in sub_jobs_data['input_collector_list']:
                        collector = collector_map[collector_data['type']](**collector_data)
                        sub_jobs.input_collector_list.append(collector)

                if 'result_collector_list' in sub_jobs_data:
                    for collector_data in sub_jobs_data['result_collector_list']:
                        collector = collector_map[collector_data['type']](**collector_data)
                        sub_jobs.result_collector_list.append(collector)

                jobs.append(sub_jobs)
        return jobs

    def init_job(self):
        pass

    def load_file_collections(self, job):
        # TODO: 设计缺陷 这里file_collector的值是二义的 所以要做二义处理 需要重构
        file_collector_list = [c for c in job.input_collector_list if '@' not in c.path]
        collections = action.load_file_collections(file_collector_list)
        for name, files in collections.items():
            collect_name = '%s@%s' % (job.name, name)
            self.file_collections[collect_name] = files

        other_collector = [c for c in job.input_collector_list if '@' in c.path]
        for collector in other_collector:
            collect_name = collector.path
            if collect_name in self.file_collections.keys():
                # TODO: collections key有共识约束 需要重构
                collections[collector.name] = self.file_collections[collect_name]
            else:
                raise ValueError('未找到引用 %s' % collector.path)
        return collections

    def init_vasp_jobs(self, job: model.VaspJobs):
        collections = self.load_file_collections(job)
        input_batch = action.create_input_args_batch(collections)
        folder_name = job.name.replace(' ', '_').lower()
        task_id_list = action.generate_task_id(folder_name, len(input_batch))
        logger.info('the number of %s jobs is %d', folder_name, len(input_batch))
        return tuple(zip(task_id_list, input_batch))

    def run_vasp_jobs(self, vasp_jobs: tuple):
        task_list = {}
        for task_id, input_batch in vasp_jobs:
            # TODO: 添加针对任务的修改 修改方式是幂等
            task_dir = action.create_task_dir(self._model.env.workspace, task_id, input_batch, copy_mode=self.copy_mode)

            sub_id = self.push_task(task_dir, self._model.runner.post_cmd, self.push_script_code)
            task_list[task_dir] = sub_id
        return task_list

    def push_task(self, task_dir: str, *args, **kwargs):
        if task_dir in self.task_status.keys():
            return self.task_status[task_dir].task_id
        task_id = action.push_task(task_dir, *args, **kwargs)
        task_status = model.TaskStatus(
            path=task_dir,
            task_id=task_id,
            type='vasp',
            status='wait'
        )
        self.task_status[task_dir] = task_status

    @property
    def need_update_task_status(self):
        out = {}
        for task_dir, task_status in self.task_status.items():
            if task_status.status not in ('finished', 'unknown'):
                out[task_dir] = task_status
        logger.info('task list: %s', str(out))
        return out

    def update_vasp_status(self):
        query_task_status = action.query_task_status(self._model.runner.query_cmd)
        for task_dir, task_status in self.need_update_task_status.items():
            task_id = task_status.task_id
            if task_id in query_task_status.keys():
                task_status.status = query_task_status[task_id]
            else:
                task_status.status = 'unknown'

        write_data = [t.dict() for t in self.task_status.values()]
        action.write_task_status(self._task_status_file, write_data)
        logger.debug('current task status is %s', str(self.task_status))
        time.sleep(0.5)

    def wait_vasp_jobs_finished(self, task_list):
        finished_status = (
            'finished',
            'unknown'
        )

        has_unfinished_jobs = True
        while has_unfinished_jobs:
            has_unfinished_jobs = False
            self.update_vasp_status()
            for task_status in self.task_status.values():
                has_unfinished_jobs |= (task_status.status not in finished_status)

    def run(self):
        self.init()
        for job in self._model.jobs:
            if isinstance(job, model.VaspJobs):
                vasp_jobs = self.init_vasp_jobs(job)
                task_list = self.run_vasp_jobs(vasp_jobs)
                self.wait_vasp_jobs_finished(task_list)
            elif isinstance(job, model.PostJobs):
                self.init_job()
