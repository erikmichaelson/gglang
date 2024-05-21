import duckdb
import sys
from gg_types import *
import re

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
        if c == '[':
            if depth == 0:
                start = i
            depth += 1
        elif c == ']':
            depth -= 1
            assert depth >= 0, f"ERROR: unmatched ']' character # {i} : {c}"
            if depth == 0:
                ret.append(line[start:i+1])
                start = i + 1
        elif c == '(':
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
    if start != len(line):
        ret.append(line[start:])
    return ret

# parse messes with the database but the caller keeps a pointer to it
def parse(db:duckdb.DuckDBPyConnection, text: str) -> {int: Plot}:
    text = text.lower()
    lines = text.split('\n')
    ret = {}
    db.sql("create table params (name text, variable text, value float, def float, data_dependencies text[]); ")
    db.sql("create table data (name text, code text, plot_dependencies int[]); ")

    PLOT_ID = 0
    plt = Plot()
    param_source = None
    for l in lines:
        #l = [l for l in l.strip().split(' ') if l != '']
        l = lex(l)
        print(l)
        if l == []:
            continue
        #######  DATA TYPES  #######
        elif l[0] == 'stream':
            raise Exception("ERROR: streams don't exist anymore, everything is data")
        elif l[0] == 'data':
            data_source = l[1] # this will be a sql expression or raw data_source
            assert len(l) == 3, "ERROR: need format 'data [SQL | PATH] [DATA_ALIAS]'"
            data_alias = l[2]
            if data_source[0] == '(':
                # todo:  actually escape this live sql better... :/
                # todo2: handle defaults and swap out the variables before calling into sql
                db.sql(f"""insert into data values ('{data_alias}', '{data_source.replace("'", "''")}', null) """)
            else:
                view_sql = f"select * from {data_source}"
                db.sql(f"""insert into data values ('{data_alias}', '{view_sql.replace("'","''")}', null) """)

        #######  PLOTS  #######
        elif l[0] == 'table':
            if(plt.plot_type is not None):
                assert plt.data_name is not None, "ERROR: no data source specified for this plot"
                ret.update({PLOT_ID: plt})
                PLOT_ID += 1
            plt = Table()
            print(f'table init called, {plt.plot_type}')
        elif l[0] == 'dot':
            if(plt.plot_type is not None):
                assert plt.data_name is not None, "ERROR: no data source specified for this plot"
                ret.update({PLOT_ID: plt})
                PLOT_ID += 1
            plt = Dot()
        elif l[0] == 'map':
            if(plt.plot_type is not None):
                assert plt.data_name is not None, "ERROR: no data source specified for this plot"
                ret.update({PLOT_ID: plt})
                PLOT_ID += 1
            plt = Map()
        elif l[0] == 'text':
            if(plt.plot_type is not None):
                assert plt.data_name is not None, "ERROR: no data source specified for this plot"
                ret.update({PLOT_ID: plt})
                PLOT_ID += 1
            plt = Text()

        #######  ENCODINGS  #######
        elif l[0] == 'row':
            assert plt.plot_type == 'TABLE', f'plot type = {plt.plot_type}'
            try:
                param_source = l[1].split('.')[0]
            except:
                raise Exception("ERROR: you need to specify the data source name for each encoding")
            assert plt.data_name is None or plt.data_name == param_source, f"ERROR: all encodings in the same plot need to have same source {param_source} vs {plt.data_name}"
            plt.data_name = param_source
            db.sql(f"update data set plot_dependencies = list_append(plot_dependencies, {PLOT_ID}) where name = '{param_source}' ")
            plt.row = l[1]
        elif l[0] == 'col':
            assert plt.plot_type == 'TABLE', f'plot type = {plt.plot_type}'
            try:
                param_source = l[1].split('.')[0]
            except:
                raise Exception("ERROR: you need to specify the data source name for each encoding")
            assert plt.data_name is None or plt.data_name == param_source, f"ERROR: all encodings in the same plot need to have same source {param_source} vs {plt.data_name}"
            plt.data_name = param_source
            db.sql(f"update data set plot_dependencies = list_append(plot_dependencies, {PLOT_ID}) where name = '{param_source}' ")
            plt.col = l[1]
        elif l[0] == 'geometry':
            assert plt.plot_type == 'MAP', f'plot type = {plt.plot_type}'
            try:
                param_source = l[1].split('.')[0]
            except:
                raise Exception("ERROR: you need to specify the data source name for each encoding")
            assert plt.data_name is None or plt.data_name == param_source, f"ERROR: all encodings in the same plot need to have same source {param_source} vs {plt.data_name}"
            plt.data_name = param_source
            db.sql(f"update data set plot_dependencies = list_append(plot_dependencies, {PLOT_ID}) where name = '{param_source}' ")
            plt.geometry = l[1]
        elif l[0] == 'value':
            assert plt.plot_type in ['TABLE','TEXT'], f'plot type = {plt.plot_type}'
            # value can now either be a list or a single value
            # eval is hideously unsafe lol
            str_list = []
            if l[1][0] == '[' and l[1][-1] == ']':
                assert plt.plot_type == 'TABLE', f'only TABLE types can have list values (plot_type = {plt.plot_type})'
                l[1] = l[1][1:-1]
                str_list = [s.strip() for s in l[1].split(',')]
            else:
                str_list.append(l[1])
            l[1] = str_list
            print(l[1])
            for value in l[1]:
                print(value)
                try:
                    param_source = value.split('.')[0]
                except:
                    raise Exception("ERROR: you need to specify the data source name for each encoding")
                assert plt.data_name is None or plt.data_name == param_source, f"ERROR: all encodings in the same plot need to have same source {param_source} vs {plt.data_name}"
                plt.data_name = param_source
                fail = db.sql(f"""update data set plot_dependencies = list_append(plot_dependencies, {PLOT_ID}) where name = '{param_source}'
                                returning name, plot_dependencies""").fetchall()
                assert len(fail) == 1, f"ERROR: cannot set plot as a datasource dependent {fail}"
            plt.value = l[1]
        elif l[0] == 'x':
            assert plt.plot_type == 'DOT'
            try:
                param_source = l[1].split('.')[0]
            except:
                raise Exception("ERROR: you need to specify the data source name for each encoding")
            assert plt.data_name is None or plt.data_name == param_source, f"ERROR: all encodings in the same plot need to have same source {param_source} vs {plt.data_name}"
            plt.data_name = param_source
            db.sql(f"update data set plot_dependencies = list_append(plot_dependencies, {PLOT_ID}) where name = '{param_source}' ")
            plt.x = l[1]
        elif l[0] == 'y':
            assert plt.plot_type == 'DOT'
            try:
                param_source = l[1].split('.')[0]
            except:
                raise Exception("ERROR: you need to specify the data source name for each encoding")
            assert plt.data_name is None or plt.data_name == param_source, f"ERROR: all encodings in the same plot need to have same source {param_source} vs {plt.data_name}"
            plt.data_name = param_source
            db.sql(f"update data set plot_dependencies = list_append(plot_dependencies, {PLOT_ID}) where name = '{param_source}' ")
            plt.y = l[1]
        elif l[0] == 'color':
            fields = lex(l)
            if '$' in fields[1]:
                pass
            # what I'm running into in this example is whether an encoding can be directly
            # dependent on a parameter for a literal value (unnamed data stream).
            # I definitely think it should be able to, but that's a bit more difficult to
            # code. The other big difference is that for SQL encoding fields they're just
            # columns, not full tables, so you can't do the strategy of making a view and
            # then select *, you need to have an anonymous view for the whole plot's
            # data source. Maybe that should be it. Each plot gets an anonymous data source
            # as an aside, this makes the "compilation" easier since we'd explicitly track
            # which columns are used so we can ignore / compress later
            db.sql("update data set plot_dependencies = list_append(plot_dependencies, {PLOT_ID}) where name = '{param_source}' ")

        #######  OTHER  #######
        elif l[0] == 'param':
            assert plt.plot_type is not None
            plt.param = {'name':l[1] , 'variables': l[2:]}
            for i in range(2, len(l)):
                # we assume these are numerical ranges for now
                if l[i] in ['x', 'y']:
                    db.sql(f"insert into params (name, variable, def) values ('{l[1]}', 'min{l[i]}', {-sys.maxsize - 1});")
                    db.sql(f"insert into params (name, variable, def) values ('{l[1]}', 'max{l[i]}', {sys.maxsize});")
                elif l[i] == 'row':
                    db.sql(f"insert into params (name, variable, def) values ('{l[i]}', 'pk', NULL);")
                elif l[1] == 'col':
                    pass
                    #db.sql(f"insert into params (name, variable, def) values ('{l[i]}', '', {});")
                else:
                    raise Exception("Only x, y, rows and columns can be params")
        elif l[0] == 'limit':
            plt.limit = l[1]
        elif l[0] == 'ticks':
            # format [{number of x ticks}, {number of y ticks}]
            plt.ticks = l[1]

    ret.update({PLOT_ID: plt})
    PLOT_ID += 1
    # "bind" a.k.a. enforce referential integrity
    # TODO: fix this: do a left join instead of nested loop and use prepared statments
    exp_p  = db.sql("select regexp_extract_all(code, '.*\$\w+.*') from data where code ~ '.*\$\w+.*' ").fetchall()
    real_p = db.sql("select name from params;").fetchall()
    data_strings = db.sql("select name, code from data ").fetchall()
    print(data_strings)
    for alias, code in data_strings:
        ps = re.findall('\$(\w+)\.(\w+)', code)
        if ps is not None:
            print(ps)
            for p in ps:
                db.sql(f"update params set data_dependencies = list_append(data_dependencies, '{alias}') where name = '{p[0]}' and variable = '{p[1]}' ")
                # have to sub in defaults AND un-escape the code
                d = db.sql(f"select def from params where name = '{p[0]}' and variable = '{p[1]}' ").fetchone()
                # again, "anti-pattern" when I could just check my returns
                try:
                    code = code.replace(f'${p[0]}.{p[1]}', str(d[0])).replace("''", "'")
                except:
                    print("USER ERROR: data expression depends on a parameter variable that doesn't exist")
                    exit()
            print(f'bound (defaults filled code: create view {alias} as {code}')
        db.sql(f'create view {alias} as {code}')

    print(f"params expected: {exp_p}, params in existance: {real_p}")
    # the real problem here would be if there's a used param which isn't registered.
    # if there's just an extra param we can ignore it and warn
    if exp_p.sort() != real_p.sort():
        print("ERROR: expected and registered params aren't the same")

    return ret

if __name__ == '__main__':
    p = parse(open(sys.argv[1]).read())
    print(p.sql())
