import pathlib
import shutil
from itertools import count
import subprocess as sp

import yaml


def load_pipeline_yaml(path: pathlib.Path):
    if path.is_file():
        with path.open('r') as f:
            return yaml.load(f, Loader=yaml.SafeLoader)

    raise ValueError('No Found %s' % path)


def set_incar():
    """
    酱incar文件设置成特定值 或者默认值

    :return:
    """
    pass


def get_value():
    pass


def create_task_dir():
    """
    创建文件夹 复制文件

    :return:
    """
    pass


def prepare_potcar():
    """
    准备potcar文件

    :return:
    """
    pass


def push_task():
    """
    提交任务 返回任务id

    :return:
    """
    pass


def get_task_status():
    """
    获取任务状态

    :return:
    """
    pass


def generate_task_id(poscar_name):
    """
    生成唯一id structure_space_type_order

    :return:
    """
    return map(lambda x: '{}_{:04d}'.format(poscar_name, x), count())


def init_pipeline_env(env):
    """

    :param env:
    :return:
    """
    if not pathlib.Path(env.potcar_dir).is_dir():
        raise ValueError('POTCAR目录 %s 不存在', env.potcar_dir)

    workspace = pathlib.Path(env.workspace)
    if not workspace.is_dir():
        try:
            workspace.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise ValueError('无法创建工作目录 %s', str(workspace.absolute()))


def init_pipeline_runner(runner):
    """

    :param runner:
    :return:
    """
    push_sh = pathlib.Path(runner.push_sh)
    if not push_sh.exists():
        raise ValueError('找不到提交脚本 %s', str(push_sh.absolute()))
