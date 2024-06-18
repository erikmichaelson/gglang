import duckdb
import json

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
    def __eq__(self, other):
        return self.__dict__ == other.__dict__

class Table(Plot):
    def __init__(self, row=None, col=None, value=None, data_name=None):
        self.plot_type = 'TABLE'
        self.row    = row
        self.col    = col
        self.value  = value
        self.data_name = data_name
    def html(self, db, _id):
        table_object = f'<div id="{_id}"><div class="query">{self.sql(db)}</div>'
        table_object += db.sql(self.sql(db)).pl()._repr_html_()
        table_object += '<div class="res-container"></div></div>'
        return table_object
    def invert_selection(self, db, variable, row) -> str:
        # we need to invert for both pivoted and non-pivoted tables
        if self.row is None and self.col is None:
            # we assume the javascript does the heavy lifting of extracting the PK
            # this is basically a noop
            return row
        raise Exception("pivot invert not implemented yet")
    def sql(self, db):
        ret = ''
        # need to determine if the table should be a pivot or a raw info
        # non-pivot criteria
        if self.row is None and self.col is None:
            ret = 'select '
            data = ''
            # automatically hiding a primary key in every table for invertibility is unfortunately 
            # not immediately viable bc you'd be regenerating row_number() unless you ALSO built
            # the cardinalehedron. LATER. For now we yell at the user when they try to invert a raw
            # table if a unique field isn't in the table
            #
            #data = self.value[0].split('.')[0]
            #pk = db.sql(f"select primary key from information_schema.columns where table_name = '{data}' ").fetchall()[0]
            ## if you do not have a PK one will be supplied for you
            #if pk is None:
            #    ret += 'row_number() over (),'
            for v in self.value:
                #print(v)
                v = v.split('.')
                data = v[0]
                ret += f'{v[1]},'
            ret += f' from {data} '
            ret += 'limit 10' # this will be swapped out with pagination logic
        else:
            col_values = db.query(f"select distinct {self.col} from '{self.data_name}'").fetchall()
            col_values = [c[0] for c in col_values]
            #print(col_values)
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
    def __eq__(self, other):
        return self.__dict__ == other.__dict__

class Dot(Plot):
    def __init__(self, x=None, y=None, data_name=None, color=None, size=None, param=None):
        self.plot_type = 'DOT'
        self.data_name = data_name
        self.height = 600
        self.width = 600
        self.ticks = [5,5]
        self.x = x
        self.y = y
        self.color = color
        self.size  = size
        self.param = param
    def invert_selection(self, db, variable:str, pixel:float) -> float:
        # so wasteful lol. but premature optimization...
        if variable == 'x':
            offset = 50 if self.ticks[0] != 0 else 0
            minx, maxx = db.sql(f'select min({self.x}::float), max({self.x}::float) from {self.data_name}').fetchall()[0]
            #print(f'[inverter] var: {variable}, input: {pixel}, output: {pixel * ((maxx - minx) / self.width) + (((maxx - minx) / self.width) * minx)}')
            return (pixel + offset) * ((maxx - minx) / self.width) + minx
        elif variable == 'y':
            offset = 15 if self.ticks[0] != 0 else 0
            miny, maxy = db.sql(f'select min({self.y}::float), max({self.y}::float) from {self.data_name}').fetchall()[0]
            return ((pixel + offset) * ((maxy - miny) / self.height)) + miny
        raise Exception("DOT can only invert x and y encodings")
    def html(self, db, _id):
        sql = self.sql(db)
        viewport = (self.width, self.height)
        if self.ticks is not None:
            viewport = (viewport[0] + 50, viewport[1] + 15)
        dot_plot = f'<div id="{_id}"><div class="query">{sql}</div><svg height="{viewport[1]}" width="{viewport[0]}" viewport="0 0 {viewport[1]} {viewport[0]}"'
        if self.param is not None:
            x = 1 if 'x' in self.param['variables'] else 0
            y = 1 if 'y' in self.param['variables'] else 0
            dot_plot += f'''onmousemove="interval_move_listener(event,{x},{y})" onmouseup="interval_up_listener(event.target,'{self.param['name']}', {x}, {y})"'''
        dot_plot += '>'
        # if it errors out after this type cast we just let it. The user has to supply columns w/ correct data types
        res = db.sql(f'select min({self.x}::float), max({self.x}::float), min({self.y}::float), max({self.y}::float) from {self.data_name}').fetchall()[0]
        minx, maxx, miny, maxy = res
        #print(minx, maxx, miny, maxy)
        if self.color:
            for d in db.sql(sql).fetchall():
                dot_plot += f'<circle transform="translate({50}, {-15})" cx="{((d[0] - minx) * self.height) / (maxx - minx)}" cy="{600 - (((d[1] - miny)*600) / (maxy - miny))}" '
                dot_plot += f'r="2" '
                dot_plot += f'color="{d[2]}"/>\n'
        else:
            for d in db.sql(sql).fetchall():
                dot_plot += f'<circle transform="translate({50}, {-15})" cx="{((d[0] - minx) * self.height) / (maxx - minx)}" cy="{600 - (((d[1] - miny) * 600) / (maxy - miny))}"'
                dot_plot += f'r="2"/>'
        if self.ticks is not None:
            for xt in range(0, self.width + 50,  int(self.width / self.ticks[0])):
                dot_plot += f'<text class="tickMark" x="{xt}" y="{self.height + 15}">{(xt * ((maxx - minx) / self.width)) + minx}</text>'
            for yt in range(0, self.height + 50, int(self.height / self.ticks[1])):
                dot_plot += f'<text class="tickMark" x="0" y="{self.height - yt}">{(yt * ((maxy - miny) / self.height)) + miny}</text>'
        dot_plot += '</svg></div>'
        return dot_plot
    def sql(self, db):
        ret = f'select {self.x}, {self.y},'
        # color can either be a hexcode or a SQL column expression that returns hex
        # eventually it can be a categorical column
        if self.color is not None:
            ret += f' {self.color}'
        if self.size is not None:
            ret += f' {self.size} '
        ret += f"from {self.data_name} "
        return ret
    def __eq__(self, other):
        return self.__dict__ == other.__dict__

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
    def __eq__(self, other):
        return self.__dict__ == other.__dict__

class Text(Plot):
    def __init__(self, x=None, y=None, value=None,data_name=None):
        self.plot_type = 'TEXT'
        self.x = x
        self.y = y
        self.value = value
        self.data_name = data_name
    def sql(self, db):
        ret = f'select {self.value[0]} '
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
        #print(f'format of count(*) response:{res[0][0]}')
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
    def __eq__(self, other):
        return self.__dict__ == other.__dict__
