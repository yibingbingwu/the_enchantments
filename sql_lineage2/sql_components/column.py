from typing import Dict


class Column(object):
    @staticmethod
    def build_from(another):
        new_inst = Column(another.is_physical)
        return new_inst

    @staticmethod
    def from_table(col: Dict[str, str]):
        return Column(is_physical=True, known_as=col['column'])

    def __init__(self, is_physical, known_as: str = None, direct_inherit=None):
        self.known_as = known_as or direct_inherit.known_as
        self.is_physical = is_physical
        self.depend_list = [direct_inherit] if direct_inherit else []
