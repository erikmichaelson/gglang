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
    def __init__(self, plot_type=None, data_name=None):
        self.plot_type = plot_type
        self.data_name = data_name
    def html(self, db) -> str:
        raise Exception("ERROR: called on abstract class")
    def sql(self, db) -> str:
        raise Exception("ERROR: called on abstract class")

class Map(Plot):
    def __init__(self, geometry=None, color=None, tooltip=None, limit=None):
        self.plot_type   = 'MAP'
        self.data_name  = None
        self.geometry    = geometry
        self.color       = color
        self.tooltip     = tooltip
        self.limit       = limit
    def html(self, db):
        # this just writes a geojson file - I have to read it
        db.sql('load spatial')
        db.sql(self.sql(db))
        geojson = open('test.geojson').read()
        ret = f'''
        <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
        integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
        crossorigin=""/>
        <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
        integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
        crossorigin=""></script>

        <div id="map" style="height:500"></div>'''
        ret += '''
        <script>
           var map = L.map('map').setView([37.8, -96], 4);
           var tiles = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
               maxZoom: 19,
               attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
           }).addTo(map);
        '''
        ret += f'''
           const data = {geojson};
           console.log(data);
           L.geoJson(data).addTo(map);
        </script>
        '''
        return ret
    def sql(self, db):
        ret = f"copy (select ST_Transform({self.geometry}, 'epsg:26915','epsg:4326') as geom "
        if self.color:
            ret += f',{color} '
        limit = f' limit {self.limit} ' if self.limit else ''
        ret += f"from '{self.data_name}' {limit}) to 'test.geojson' with (format gdal, driver geojson);"
        return ret

class Table(Plot):
    def __init__(self, row=None, col=None, value=None):
        self.plot_type = 'TABLE'
        self.data_name = None
        self.row    = row
        self.col    = col
        self.value  = value
    def html(self, db, _id):
        table_object = f'<div id="{_id}"><div class="query">{self.sql(db)}</div>'
        table_object += db.sql(sql).pl()._repr_html_()
        table_object += '<div class="res-container"></div></div>'
        return table_object
    def sql(self, db):
        col_values = db.query(f"select distinct {self.col} from '{self.data_name}'").fetchall()
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
        ret += f"from '{self.data_name}' group by {self.row}"
        return ret

class Dot(Plot):
    def __init__(self, x=None, y=None, color=None, size=None):
        self.plot_type = 'DOT'
        self.data_name = None
        self.height = 600
        self.width = 600
        self.x = x
        self.y = y
        self.color = color
        self.size  = size
        self.param = None
    def invert_selection(self, db, variable, pixel:float) -> float:
        # so wasteful lol. but premature optimization...
        if variable == 'x':
            minx, maxx = db.sql(f'select min({self.x}::float), max({self.x}::float) from {self.data_name}').fetchall()[0]
            print(f'[inverter] var: {variable}, input: {pixel}, output: {pixel * ((maxx - minx) / self.width) + (((maxx - minx) / self.width) * minx)}')
            return pixel * ((maxx - minx) / self.width) + (((maxx - minx) / self.width) * minx)
        elif variable == 'y':
            miny, maxy = db.sql(f'select min({self.y}::float), max({self.y}::float) from {self.data_name}').fetchall()[0]
            return ((self.height - pixel) * ((maxy - miny) / self.height)) + (((maxy - miny) / self.height) * miny)
        raise Exception("DOT can only invert x and y encodings")
    def html(self, db, _id):
        sql = self.sql(db)
        predicated = False
        if 'where' in sql:
            predicated = True
        dot_plot = f'<div id="{_id}"><div class="query">{sql}</div><svg height="{self.height}" width="{self.width}" viewport="0 0 {self.height} {self.width}"'
        if self.param is not None:
            dot_plot += f'onmousemove="move_listener(event)" onmouseup="up_listener(event.target, \'{self.param}\')"'
        dot_plot += '>'
        # if it errors out after this type cast we just let it. The user has to supply columns w/ correct data types
        res = db.sql(f'select min({self.x}::float), max({self.x}::float), min({self.y}::float), max({self.y}::float) from {self.data_name}').fetchall()[0]
        #print(res)
        minx, maxx, miny, maxy = res
        for d in db.sql(sql).fetchall():
            dot_plot += f'<circle cx="{((d[0] - minx) * 600) / (maxx - minx)}" cy="{600 - (((d[1] - miny) * 600) / (maxy - miny))}" r="3" color="black"/>\n'
        dot_plot += '</svg></div>'
        dot_plot += '<h1 class="sel_counter">-</div>'
        print(len(dot_plot))
        return dot_plot
    def sql(self, db):
        ret = f'select {self.x}, {self.y},'
        if self.color is not None:
            ret += f' {self.color}, '
        if self.size is not None:
            ret += f' {self.size} '
        ret += f"from {self.data_name} "
        return ret

class Line(Plot):
    def __init__(self):
        pass
    def html(self, db, _id):
        sql = self.sql(db)
        predicated = False
        line_plot = f'<div id="{_id}"><div class="query">{sql}</div>'
        if 'where' in sql:
            predicated = True
        if plot.params.name:
            if not predicated:
                line_plot += 'where'
        line_plot += '</div>'
        return line_plot
    def sql(self, db):
        pass

class Text(Plot):
    def __init__(self):
        self.plot_type = 'TEXT'
        self.x = None
        self.y = None
        self.value = None
        self.data_name = None
    def sql(self, db):
        ret = f'select {self.value} '
        assert self.value is not None, "VALUE needed for Text plot type"
        if(self.x):
            ret += f',{self.x}'
        if(self.y):
            ret += f',{self.y}'
        ret += f' from {self.data_name}'
        return ret
    def html(self, db, _id):
        sql = self.sql(db)
        res = db.sql(sql).fetchall()
        print(f'format of count(*) response:{res[0][0]}')
        ret = f'<div id="{_id}">'
        if(len(res[0]) == 1):
            for value in res:
                ret += f'<h3 class="text">{res[0][0]}</h3>'
        elif(len(res[0]) == 2):
            ret = '<svg>'
            raise Exception("Single variable TEXT not implemented")
        elif(len(res[0]) == 3):
            ret = '<svg>'
            raise Exception("Double variable TEXT not implemented")
        ret += '</div>'
        return ret
