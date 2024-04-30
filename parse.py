import duckdb

class Agg_Value():
    def __init__(self, agg_func, ref):
        self.agg_func = agg_func
        self.ref = ref

def split_agg(value):
    value = value.split('(')
    assert len(value) == 2
    assert value[-1][-1] == ')'
    value[-1] = value[-1][:-1]
    print(value)
    return Agg_Value(value[0].upper(), value[1])

class Plot():
    def __init__(self, plot_type=None, data_source=None):
        self.plot_type = plot_type
        self.data_source = data_source
    def sql(self) -> str:
        raise "Not implemented"

class Map():
    def __init__(self, geometry=None, color=None, tooltip=None, limit=None):
        self.plot_type   = 'MAP'
        self.data_source = None
        self.geometry    = geometry
        self.color       = color
        self.tooltip     = tooltip
        self.limit       = limit
    def sql(self):
        ret = f"copy (select ST_Transform({self.geometry}, 'epsg:26915','epsg:4326') as geom "
        if self.color:
            ret += f',{color} '
        limit = f' limit {self.limit} ' if self.limit else ''
        ret += f"from {self.data_source} {limit}) to 'test.geojson' with (format gdal, driver geojson);"
        return ret

class Table(Plot):
    def __init__(self, row=None, col=None, value=None):
        self.plot_type = 'TABLE'
        self.data_source = None
        self.row   = row
        self.col   = col
        self.value = value
    def sql(self):
        col_values = duckdb.query(f"select distinct {self.col} from '{self.data_source}'").fetchall()
        col_values = [c[0] for c in col_values]
        print(col_values)
        self.value = split_agg(self.value)
        ret = f'select {self.row},\n'
        for cv in col_values:
            if self.value.agg_func == 'COUNT':
                ret += f"sum(case when {self.col} = '{cv}' then 1 else 0 end) as '{cv}'"
            elif self.value.agg_func == 'SUM':
                #assert col_type in ['FLOAT', 'NUMERIC', 'INT']
                ret += f"sum(case when {self.col} = '{cv}' then {self.value.ref} else 0 end) as '{cv}'"
            elif self.value.agg_func == 'AVG':
                #assert col_type in ['FLOAT', 'NUMERIC', 'INT']
                ret += f"avg(case when {self.col} = '{cv}' then {self.value.ref} else 0 end) as '{cv}'"
            elif self.value.agg_func == 'MAX':
                ret += f"max(case when {self.col} = '{cv}' then {self.value.ref} else 0 end) as '{cv}'"
            elif self.value.agg_func == 'MIN':
                ret += f"min(case when {self.col} = '{cv}' then {self.value.ref} else 0 end) as '{cv}'"
            ret += ',\n' # duckdb is ok with trailing commas
        ret += f"from '{self.data_source}' group by {self.row}"
        return ret

class Dot(Plot):
    def __init__(self, x=None, y=None, color=None, size=None):
        self.plot_type = 'DOT'
        self.data_source = None
        self.x = x
        self.y = y
        self.color = color
        self.size  = size
    def sql(self):
        ret = 'select {self.x}, {self.y},'
        if color is not None:
            ret += ' {self.color}, '
        if size is not None:
            ret += ' {self.size} '
        ret += 'from {self.data_source}'
        return ret

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
        print(l)
        if l[0] == 'data':
            data_source = l[1]
            if len(l) == 3:
                data_alias = l[2]
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
    assert data_source is not None, "ERROR: no datasource specified"
    ret.data_source = data_source
    if data_alias is not None:
        ret.data_alias = data_alias
    return ret

if __name__ == '__main__':
    p = parse(open(sys.argv[1]).read())
    print(p.sql())
