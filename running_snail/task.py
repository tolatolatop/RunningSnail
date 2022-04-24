class VaspTask(object):

    def __init__(self):
        """
        初始化信息

        """
        pass

    def init(self):
        """
        创建计算目录

        """
        pass

    def push(self):
        pass

    def update(self):
        pass

    def archive(self):
        pass


class StructureOptimization(VaspTask):
    pass


class StaticCalculation(VaspTask):
    pass


class OpticalCalculation(VaspTask):
    pass
