import duckdb
import sys
from gg_types import *

def parse(text: str) -> Plot:
    text = text.lower()
    lines = text.split('\n')
    ret = Plot()
    data_source = None
    data_alias = None
    for l in lines:
        l = [l for l in l.strip().split(' ') if l != '']
        if l == []:
            continue
        #print(l)
        if l[0] == 'data':
            i = 1
            if l[i][0] == '(':
                tmp = []
                while l[i][-1] != ')':
                    tmp.append(l[i])
                    i += 1
                tmp.append(l[i])
                i += 1
            data_source = l[i]
            if len(l) == 3:
                data_alias = l[2]
                view_sql = f"create view {data_alias} as (select * from '{data_source}');"
            else:
                assert len(l) == i + 1, "ERROR: need a data alias for custom SQL data"
                view_sql = f"create view {data_alias} as {' '.join(tmp)};"
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
            assert ret.plot_type is not None
            ret.params.name = l[1]
            for i in range(1, len(l)):
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
