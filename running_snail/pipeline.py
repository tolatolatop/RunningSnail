import pathlib

from . import model
from . import action

jobs_map = {
    'vaspjobs': model.VaspJobs,
    'postjobs': model.PostJobs
}

collector_map = {
    'filecollector': model.FileCollector
}


class PipeLine(object):

    def __init__(self, data: dict):
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

    def init(self):
        """
        有副作用的初始化过程

        :return:
        """
        action.init_pipeline_env(self._model.env)
        action.init_pipeline_runner(self._model.runner)

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

    def init_vasp_job(self):
        pass

    def run(self):
        pass
