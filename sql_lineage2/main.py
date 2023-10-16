import sys

import sqlfluff
from sqlfluff.api import APIParsingError

sys.setrecursionlimit(1024 * 1024)


def try_parse(qry_str: str):
    # my_bad_query = """create table test_schema.z as
    # SeLEct  a.*, 1, b.blah as fOO
    #                 from mySchema.myTable a
    #                 left join mySchema.yTable b on a.x=b.y
    #                 where blah!='abc'"""
    # try:
    #     parse_result = sqlfluff.parse(my_bad_query, dialect="bigquery")
    try:
        sqlfluff.parse(qry_str, dialect="snowflake")
        # print(parse_result)
    except APIParsingError as e:
        print("PARSING ERROR")
        raise e
        # sys.exit(2)
    except Exception as e:
        print("OTHER error:" + str(e))
        sys.exit(1)

    print('OK')


if __name__ == '__main__':
    with open(sys.argv[1], 'r') as fin:
        in_query = fin.read()
    try_parse(in_query)
