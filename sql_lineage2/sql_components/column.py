class Column(object):
    @staticmethod
    def build_from(another):
        new_inst = Column()
        return new_inst

    def __init__(self, is_physical, known_as: str = None, direct_inherit=None):
        self.known_as = known_as or direct_inherit.known_as
        self.is_physical = is_physical
        self.depend_list = [direct_inherit] if direct_inherit else []
