import enum

from sql_lineage2.sql_components.column import Column


class DepType(enum.Enum):
    SELECT = 1
    JOIN = 2
    WHERE = 3


class Dependencies(object):
    def __init__(self):
        self.dep_pool = {
            DepType.SELECT: set(),
            DepType.JOIN: set(),
            DepType.WHERE: set(),
        }

    def add_direct(self, fq_name: str, type: DepType = DepType.SELECT):
        self.dep_pool[type].add(fq_name)

    def inherit_from(self, another: Column, type: DepType = DepType.SELECT):
        self.dep_pool[type].update(another.dependencies.get_all())

    def get_all(self):
        retval = set()
        for v in self.dep_pool.values():
            retval.update(v)
        return retval
