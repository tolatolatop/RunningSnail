import os
import pathlib
import shutil
from itertools import product
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


def create_task_dir(workspace: str, task_id: str, input_files: list, copy_mode=True):
    """
    创建文件夹 复制文件

    :return:
    """
    workspace = pathlib.Path(workspace)
    workspace.mkdir(exist_ok=True, parents=True)
    task_dir = workspace / task_id

    task_dir.mkdir(exist_ok=True, parents=True)

    for file_name, src_file_path in input_files:
        src_file_path = pathlib.Path(src_file_path)
        dst_file_path = task_dir / file_name

        if copy_mode:
            copy_files(src_file_path, dst_file_path)
        else:
            link_files(src_file_path, dst_file_path)

    return str(task_dir.absolute())


def get_atom_set_in_poscar(poscar):
    return {}


def prepare_potcar(potcar_dir, atom_set, dst_path):
    """
    准备potcar文件

    :return:
    """
    potcar_dir = pathlib.Path(potcar_dir)
    potcar_list = ''
    for atom in atom_set:
        potcar_file = potcar_dir / atom / 'POTCAR'
        potcar_list += '"%s"' % str(potcar_file.absolute())

    cmd = 'cat %s > %s' % (potcar_list, dst_path)
    ret = os.system(cmd)
    if ret != 0:
        raise RuntimeError('获取POTCAR错误 %s' % cmd)


def push_task(workspace, cmd, push_script_code: str) -> str:
    """
    提交任务 返回任务id

    :return:
    """
    p = sp.Popen(cmd, shell=False, stdin=sp.PIPE, stderr=sp.STDOUT, stdout=sp.PIPE, cwd=workspace)
    stdout, stderr = p.communicate(input=push_script_code.encode())
    if p.returncode != 0:
        err_msg = 'run [ %s < %s ] error, return %d %s' % (
            ' '.join(cmd),
            push_script_code,
            p.returncode,
            stdout.decode(),
        )
        return err_msg
    return stdout.decode().strip()


def load_task_status(task_status_file: pathlib.Path):
    if task_status_file.is_file():
        with task_status_file.open('r') as f:
            return yaml.load(f, Loader=yaml.SafeLoader)
    return []


def query_task_status(cmd) -> dict:
    """
    返回状态

    :return:
    """
    p = sp.Popen(cmd, shell=False, stdin=sp.PIPE, stderr=sp.STDOUT, stdout=sp.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        err_msg = 'run [ %s ] error, return %d %s' % (
            ' '.join(cmd),
            p.returncode,
            stdout.decode(),
        )
        raise RuntimeError(err_msg)
    return {}


def write_task_status(task_status_file: pathlib.Path, task_status: list):
    if not task_status_file.parent.is_dir():
        task_status_file.parent.mkdir(exist_ok=True, parents=True)
    with task_status_file.open('w') as f:
        return yaml.dump(task_status, f, yaml.SafeDumper)


def get_task_status():
    """
    获取任务状态

    :return:
    """
    pass


def generate_task_id(poscar_name, count):
    """
    生成唯一id structure_space_type_order

    :return:
    """
    task_id_list = ['{}_{:04d}'.format(poscar_name, i) for i in range(count)]
    return task_id_list


def init_pipeline_env(env):
    """

    :param env:
    :return:
    """
    # TODO: 需要解耦
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
    # TODO: 需要解耦
    push_sh = pathlib.Path(runner.post_sh)
    if not push_sh.exists():
        raise ValueError('找不到提交脚本 %s', str(push_sh.absolute()))


def load_files(path, glob):
    path = pathlib.Path(path)
    return list(str(f.absolute()) for f in path.glob(glob))


def link_files(src: pathlib.Path, dst: pathlib.Path):
    dst.symlink_to(src)


def copy_files(src: pathlib.Path, dst: pathlib.Path):
    shutil.copyfile(src, dst)


def load_file_collections(file_collector_list):
    """

    :param file_collector_list:
    :return:
    """
    collections = {}
    for collector in file_collector_list:
        files = load_files('.', collector.path)
        collections[collector.name] = files
    return collections


def create_input_args_batch(input_args: dict):
    """
    任务组合

    :return:
    """
    input_args_batch = product(*input_args.values())
    return tuple(tuple(zip(input_args.keys(), arg)) for arg in input_args_batch)


def get_push_script_code(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()
