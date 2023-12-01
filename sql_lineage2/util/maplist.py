from typing import List, Union


class MapList(object):
    @staticmethod
    def to_list(seed: Union[dict, List[dict], None]) -> list:
        if not seed or type(seed) == list:
            return seed

        assert type(seed) == dict, "Unexpected data type here"

        retval = []
        for k, v in seed.items():
            retval.append(dict(k=v))
        return retval

    def __init__(self, seed: Union[dict, List[dict], None]):
        if seed:
            assert (type(seed) == list and type(seed[0]) == dict) or type(seed) == dict, "Wrong seed type here"
        self.seed = seed

    def get(self, k: str) -> list:
        if type(self.seed) == list:
            ret_list = []
            for i in self.seed:
                if k in i:
                    ret_list.append(i[k])
            return ret_list

        kv = self.seed.get(k, None)
        if kv and type(kv) != list:
            return [kv]
        return kv

    def __contains__(self, k: str) -> bool:
        if type(self.seed) == list:
            for i in self.seed:
                if k in i:
                    return True
        return k in self.seed
