from flask import Flask, request
import typing
import duckdb
import parse
import sys
from gg_types import *

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

var working_selector;
var i_x, i_y, f_x, f_y;

// take a streaming click event on a selector and draw a rectangle
function move_listener(event) {
    if(event.buttons) {
        f_x = event.x;
        f_y = event.y;
        console.log('(' + i_x + ',' + i_y +') -> (' + f_x + ',' + f_y +')');
        if(!working_selector) {
            i_x = event.x;
            i_y = event.y;
            var hlrect = event.target.getElementsByClassName('selector')[0];
            if(!hlrect) {
                hlrect = document.createElementNS('http://www.w3.org/2000/svg','rect');
            }
            hlrect.setAttribute('fill', 'blue');
            hlrect.setAttribute('opacity', 0.7);
            working_selector = hlrect;
            event.target.appendChild(hlrect);
        }
        working_selector.setAttribute('x', Math.min(f_x, i_x));
        working_selector.setAttribute('y', Math.min(f_y, i_y));
        working_selector.setAttribute('width',  Math.abs(f_x - i_x));
        working_selector.setAttribute('height', Math.abs(f_y - i_y));
        working_selector.setAttribute('class', 'selector');
    }
}

function up_listener(param_name) {
    working_selector = null;
    var query = "update params set x0="+i_x+",x1="+f_x+",y0="+i_y+",y1="+f_y+" where name = " + param_name +";"
    fetch('/query',
        method : 'POST',
        headers: {'message-type' : 'application/json'},
        body : json.stringify(param: param_name, v_vs: {first_x: x0, second_x: x1, first_y: y0, second_y: y1} )
    }).then((num) =>
        document.getElementsByClassName('sel_counter')[0].innerHTML = num;)
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
.hlbox {
    opacity: 0.5;
    background: blue;
}
</style>'''

plots = []
conn = duckdb.connect(':memory:')

@app.route('/param_update_plots', methods=['POST'])
def param_update_plots():
    req = request.get_json()
    print('param named',req['param'], '; v_vs', req['v_vs']);
    streams = conn.sql(f"""select data_depedencies from params where param_name = '{req["param"]}' """).fetchall()
    for s in streams:
        print(s)
        swapped_data_def = s[1].replace(f'${req["param"]}', f'${p_value}')
        dependencies = conn.sql("select plot_dependencies from data where data_alias = '{s}' ").fetchall()
        conn.sql(f"create or replace view {data_alias} as ({swapped_data_def}) ")
        global plots
        for p in dependencies:
            if p.id == '':
                p.update()

@app.route('/query', methods=['POST'])
def query():
    return conn.sql(request.get_json()).pl()._repr_html_()

@app.route('/')
def index():
    global HTML
    return HTML

if __name__ == '__main__':
    # the database stores all of the parsed params and streams needed at runtime
    plots = parse.parse(conn, open(sys.argv[1]).read())
    print("params still visibile back in server main:", conn.sql("select distinct name from params;").fetchall())
    print("data still visibile back in server main:", conn.sql("select distinct name from data;").fetchall())
    print(len(plots), 'plots displaying')
    for p in plots.values():
        sql = p.sql(conn)
        print(sql)
        HTML += p.html(conn)
    app.run()
