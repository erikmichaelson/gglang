import duckdb
import sys
from gg_types import *

# to make it easy one element can't wrap over a line.
# break characters are parens and spaces - if they're not wrapped in parens
# max depth we deal with here is 1
def lex(line: str) -> [str]:
    line = line.strip()
    ret = []
    depth = 0
    start = 0
    last_char = None
    for i,c in enumerate(line):
        if c == '(':
            if depth == 0:
                start = i
            depth += 1
        elif c == ')':
            depth -= 1
            assert depth >= 0, f"ERROR: unmatched ')' character # {i} : {c}"
            if depth == 0:
                ret.append(line[start:i+1])
        elif c == ' ':
            if depth == 0 and last_char not in (' ', '(', ')'):
                ret.append(line[start:i])
        else:
            if depth == 0 and last_char in (' ', ')'):
                start = i
        last_char = c
    # get the last word in there
    ret.append(line[start:])
    return ret

def parse(text: str) -> Plot:
    text = text.lower()
    lines = text.split('\n')
    ret = Plot()
    data_source = None
    data_alias = None
    for l in lines:
        #l = [l for l in l.strip().split(' ') if l != '']
        l = lex(l)
        print(l)
        if l == []:
            continue
        elif l[0] == 'data':
            data_source = l[1] # this will be a sql expression or raw data_source
            view_sql = ''
            assert len(l) == 3, "ERROR: need format 'data [SQL | PATH] [DATA_ALIAS]'"
            data_alias = l[2]
            if data_source[1][0] == '(':
                view_sql = f"create view {data_alias} as ({data_source});"
            else:
                view_sql = f"create view {data_alias} as (select * from {data_source});"
            print(view_sql)
            duckdb.sql(view_sql)

        elif l[0] == 'table':
            ret = Table()
            print(f'table init called, {ret.plot_type}')
        elif l[0] == 'dot':
            ret = Dot()
        elif l[0] == 'map':
            ret = Map()
        elif l[0] == 'row':
            assert ret.plot_type == 'TABLE', f'plot type = {ret.plot_type}'
            ret.row = l[1]
        elif l[0] == 'col':
            assert ret.plot_type == 'TABLE', f'plot type = {ret.plot_type}'
            ret.col = l[1]
        elif l[0] == 'geometry':
            assert ret.plot_type == 'MAP', f'plot type = {ret.plot_type}'
            ret.geometry = l[1]
        elif l[0] == 'limit':
            ret.limit = l[1]
        elif l[0] == 'value':
            assert ret.plot_type == 'TABLE', f'plot type = {ret.plot_type}'
            # for now all tables are pivot tables - split_agg is called in the sql() func
            ret.value = l[1]
        elif l[0] == 'x':
            assert ret.plot_type == 'DOT'
            ret.x = l[1]
        elif l[0] == 'y':
            assert ret.plot_type == 'DOT'
            ret.y = l[1]
        elif l[0] == 'param':
            duckdb.sql("create table params (name text, variable text, value float) ")
            assert ret.plot_type is not None
            ret.params.name = l[1]
            for i in range(2, len(l)):
                assert l[i] in ['x', 'y']
                ret.params.p.append(l[i])
    assert data_source is not None, "ERROR: no data source specified"
    ret.data_source = data_source
    if data_alias is not None:
        ret.data_alias = data_alias
    return ret

if __name__ == '__main__':
    p = parse(open(sys.argv[1]).read())
    print(p.sql())
