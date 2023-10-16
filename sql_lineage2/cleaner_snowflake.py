import re
import sys


def _ignore(s:str, m: re.Match):
    pass


def _keep(s:str, m: re.Match):
    pass


PATTERNS = [
    (r"(comment\s*=\s*\"[^\"]+\")", _ignore),
    (r"(nth_value\([^\)]+\))\s*from\s*[first|last]", _keep),
]


def cleanup_txt(data: str):
    clean_txt = "NTH_VALUE(3, 4, 5)  FROM first"
    for p, act in PATTERNS:
        instances = re.finditer(p, clean_txt, flags=re.IGNORECASE)
        out_str = ""
        for i in instances:
            s_pt = i.regs[0][0]
            e_pt = i.regs[0][1]
            out_str += clean_txt[0:s_pt]
            out_str += clean_txt[e_pt:]
        clean_txt = out_str

    return clean_txt


if __name__ == '__main__':
    # with open(sys.argv[1], 'r') as fin:
    #     in_file = fin.read()

    output = cleanup_txt('in_file')
