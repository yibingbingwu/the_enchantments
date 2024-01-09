import enum


class DepType(enum.Enum):
    SELECT = 1
    JOIN = 2
    WHERE = 3


class TableColumn(object):
    """
    This is the bare core definition of a Column as seen from metadata store
    """

    def __init__(self, name: str, type: str):
        self.name = name
        self.type = type


class Column(object):
    @staticmethod
    def build_from(another, dep_type: DepType, known_as: str = None):
        new_inst = Column(is_physical=False, known_as=known_as, fq_name=another.known_as)
        if another.is_physical:
            new_inst.dependencies.add_direct(another.fq_name, dep_type)
        else:
            new_inst.dependencies.inherit_from(another, dep_type)
        return new_inst

    @staticmethod
    def from_table(ns: str, ds: str, tab: str, col: TableColumn):
        return Column(is_physical=True, known_as=col.name, fq_name='.'.join([ns, ds, tab, col.name]))

    @staticmethod
    def derived(params: list, dep_type: DepType, known_as: str = None):
        new_inst = Column(is_physical=False, known_as=known_as, fq_name=None)
        for p in params:
            new_inst.dependencies.inherit_from(p, dep_type)
        return new_inst

    def __init__(self, is_physical, known_as: str = None, fq_name: str = None):
        self.is_physical = is_physical
        self.fq_name = fq_name
        self.known_as = known_as or fq_name.split('.')[-1]
        self.dependencies = Dependencies()


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
