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
    def __init__(self, plot_type=None, data_source=None, data_alias=None):
        self.plot_type = plot_type
        self.data_source = data_source
        self.data_alias = data_alias
    def html(self, db) -> str:
        raise Exception("ERROR: called on abstract class")
    def sql(self, db) -> str:
        raise Exception("ERROR: called on abstract class")

class Map(Plot):
    def __init__(self, geometry=None, color=None, tooltip=None, limit=None):
        self.plot_type   = 'MAP'
        self.data_source = None
        self.data_alias  = None
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
        ret += f"from '{self.data_alias}' {limit}) to 'test.geojson' with (format gdal, driver geojson);"
        return ret

class Table(Plot):
    def __init__(self, row=None, col=None, value=None):
        self.plot_type = 'TABLE'
        self.data_source = None
        self.data_alias = None
        self.row    = row
        self.col    = col
        self.value  = value
    def html(self, db):
        table_object = f'<div><div class="query">{self.sql(db)}</div>'
        table_object += db.sql(sql, db).pl()._repr_html_()
        table_object += '<div class="res-container"></div></div>'
        return table_object
    def sql(self, db):
        col_values = db.query(f"select distinct {self.col} from '{self.data_alias}'").fetchall()
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
        ret += f"from '{self.data_alias}' group by {self.row}"
        return ret

class Dot(Plot):
    def __init__(self, x=None, y=None, color=None, size=None):
        self.plot_type = 'DOT'
        self.data_source = None
        self.data_alias = None
        self.x = x
        self.y = y
        self.color = color
        self.size  = size
    def html(self, db):
        sql = self.sql(db)
        predicated = False
        if 'where' in sql:
            predicated = True
        dot_plot = f'<div><div class="query">{sql}</div><svg height="600" width="600" viewport="0 0 600 600"'
        #if self.params['name']:
        dot_plot += 'onmousemove="move_listener(event)" onmouseup="up_listener()"'
        dot_plot += '>'
        res = db.sql(f'select min({self.x}), max({self.x}), min({self.y}), max({self.y}) from {self.data_alias}', db).fetchall()[0]
        print(res)
        minx, maxx, miny, maxy = res
        for d in db.sql(sql, db).fetchall():
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
        ret += f"from '{self.data_alias}' "
        return ret

class Line(Plot):
    def __init__(self):
        pass
    def html(self):
        sql = self.sql(db)
        predicated = False
        line_plot = f'<div><div class="query">{sql}</div>'
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
        self.data_source = None
        self.data_alias = None
    def html(self, db):
        sql = self.sql(db)
        res = db.sql(sql).fetchall()
        ret = ''
        if(len(res[0]) == 1):
            for value in res:
                ret += f'<h3 class="text">{res[0]}</h3>'
        elif(len(res[0]) == 2):
            ret = '<svg>'
            raise Exception("Single variable TEXT not implemented")
        elif(len(res[0]) == 3):
            ret = '<svg>'
            raise Exception("Double variable TEXT not implemented")
        return ret

    def sql(self, db):
        ret = f'select {self.value} '
        assert self.value is not None, "VALUE needed for Text plot type"
        if(self.x):
            ret += f',{self.x}'
        if(self.y):
            ret += f',{self.y}'
        ret += f' from {self.data_alias}'
        return ret
