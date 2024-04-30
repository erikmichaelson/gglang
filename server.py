from flask import Flask, request
import typing
import duckdb
import parse
import sys

app = Flask(__name__)

HTML =  '''
<script>
window.onload = (() => {
    var data = document.getElementsByTagName("td");
    for(const d of data) {
        d.addEventListener("click", (d) => {return invert_table(d.srcElement);} );
    }
});

var selected;
function invert_table(clicked_element) {
    change_selected(clicked_element);
    if(!selected) {
        return;
    }
    // get the first element - this is the row-pivoted "groupby" value
    row_value = clicked_element.parentElement.children[0].innerHTML.replaceAll('"','')
    // *should* assert this is a <th> and the thing was written right
    col_value = clicked_element.parentElement.parentElement.children[0].innerHTML

    og_query = clicked_element.parentElement.parentElement.parentElement.parentElement.parentElement.firstChild.innerHTML

    col_query = og_query.split(',')[clicked_element.cellIndex]
    if(clicked_element.cellIndex == 0) {
        col_query = col_query.substr(7) // removes 'select\\n'
    }
    var re = /group by (\w+)/
    const rows = re.exec(og_query)[1]
    // first "match" is always the full thing
    re = /(sum|avg|min|max)\(case when (\w+) = (['|\w]+) then (\w+) else 0 end\)/
    res = re.exec(col_query) 
    re = /from ([\.|'|/|\w]+)/
    data_source = re.exec(og_query)[1]
    const query = 'select * from '+data_source+' where '+res[2]+' = '+res[3]+' and '+rows+" = '"+row_value+"'"
    console.log(query)
    fetch('/query', {
        method: "POST",
        headers: { "Content-Type" : "application/json" },
        body: JSON.stringify(query),
    }).then(function(response) {
        return response.text();
    }).then(function(HTML) {
        if(selected) {
            document.getElementsByClassName("res-container")[0].innerHTML = HTML;
        }
    });

    return query
}

function change_selected(element) {
    console.log("change_selected called");
    if(selected) {
        selected.className = null;
        if(selected == element) {
            document.getElementsByClassName("res-container")[0].innerHTML = null;
            selected = null;
            return;
        }
    }
    selected = element;
    selected.className = "highlighted";
}
</script>
<style>
.query {
    visibility:hidden;
    height: 0;
}
.res-container {
    overflow: scroll;
    width: 1400px;
}
.highlighted {
    background: pink;
}
</style>'''


@app.route('/query', methods=['POST'])
def query():
    return duckdb.sql(request.get_json()).pl()._repr_html_()

@app.route('/')
def index():
    global HTML
    return HTML

def table_from_sql(sql:str) -> str:
    table_object = f'<div><div class="query">{sql}</div>'
    table_object += duckdb.sql(sql).pl()._repr_html_()
    table_object += '<div class="res-container"></div>  </div>'
    return table_object

def map_from_sql(sql:str) -> str:
     # this just writes a geojson file - I have to read it
     duckdb.sql('load spatial')
     duckdb.sql(sql)
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

if __name__ == '__main__':
    plot = parse.parse(open(sys.argv[1]).read())
    sql = plot.sql()
    print(sql)
    #HTML += table_from_sql(sql)
    HTML += map_from_sql(sql)
    app.run()
