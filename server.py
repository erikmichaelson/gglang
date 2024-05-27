from flask import Flask, request, redirect
import typing
import duckdb
import parse
import sys
from gg_types import *
import json

app = Flask("__name__")

HTML =  '''
<script>
window.onload = (() => {
    var data = document.getElementsByTagName("td");
    for(const d of data) {
        d.addEventListener("click", (d) => {return table_click_listener(d.srcElement, "param", 0, 0);} );
    }
});

var selected;
function table_click_listener(clicked_element, param_name, row, col) {
    const pid = clicked_element.parentElement.parentElement.parentElement.parentElement.parentElement;
    if(selected) {
        selected.className = null;
        if(selected == clicked_element) {
            document.getElementsByClassName("res-container")[0].innerHTML = null;
            selected = null;
            // TODO: send HTTP to flask to clear the selector
            fetch('/param_update_plots', {
                method: "POST",
                headers: { "Content-type" : "application/json" },
                body: JSON.stringify({plot_id: pid, name: param_name, v_vs:{pk: None}})
            })
            return;
        }
    }
    selected = clicked_element;
    // if we're only tracking rows then we highlight the whole row
    // and accept clicks on the whole row to null the selection
    if (!col) { selected = selected.parentElement; }
    selected.className = "highlighted";
    /*if(!selected) {
        return;
    } pretty sure this is dead code*/
    og_query = clicked_element.parentElement.parentElement.parentElement.parentElement.parentElement.firstChild.innerHTML

    // get the first element - this is the row-pivoted "groupby" value
    row_value = clicked_element.parentElement.children[0].innerHTML.replaceAll('"','')
    // we use col being set in the param as a proxy for being a pivot table
    // same as in gg_types. This is janky and will be fixed
    if(col) {
        // *should* assert this is a <th> and the thing was written right
        col_value = clicked_element.parentElement.parentElement.children[0].innerHTML

        col_query = og_query.split(',')[clicked_element.cellIndex]
        if(clicked_element.cellIndex == 0) {
            col_query = col_query.substr(7) // removes 'select\\n'
        }
        var re = /group by (\w+)/
        const rows = re.exec(og_query)[1]
        // first "match" is always the full thing
        re = /(sum|avg|min|max)\(case when (\w+) = (['|\w]+) then (\w+) else 0 end\)/
        res = re.exec(col_query) 
    }
    re = /from ([\.|'|/|\w]+)/
    data_source = re.exec(og_query)[1]
    //const query = 'select * from '+data_source+' where '+res[2]+' = '+res[3]+' and '+rows+" = '"+row_value+"'"
    //console.log(query)
    var vardict = {}
    if(row_param) {
        
    }
    fetch('/param_update_plots', {
        method: "POST",
        headers: { "Content-type" : "application/json" },
        body: JSON.stringify({plot_id: pid, name:param_name, v_vs: vardict}),
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
}

var working_selector;
var i_x, i_y, f_x, f_y;

// take a click event on a selector and draw a rectangle
function interval_move_listener(event, x, y) {
    if(event.buttons) {
        if(!working_selector) {
            var hlrect = event.target.getElementsByClassName('selector')[0];
            if(!hlrect) {
                hlrect = document.createElementNS('http://www.w3.org/2000/svg','rect');
            }
            hlrect.setAttribute('fill', 'blue');
            hlrect.setAttribute('opacity', 0.7);
            hlrect.setAttribute('class', 'selector');
            working_selector = hlrect;
            event.target.appendChild(hlrect);
        }
        if(x) {
            if(!i_x) {
                i_x = event.offsetX;
            }
            if(!y) {
                working_selector.setAttribute('height', working_selector.parentElement.height.animVal.value);
            }
            f_x = event.offsetX;
            working_selector.setAttribute('x', Math.min(f_x, i_x));
            working_selector.setAttribute('width',  Math.abs(f_x - i_x));
        }
        if(y) {
            if(!i_y) {
                i_y = event.offsetY;
            }
            if(!x) {
                working_selector.setAttribute('width', working_selector.parentElement.width.animVal.value);
            }
            f_y = event.offsetY;
            working_selector.setAttribute('y', Math.min(f_y, i_y));
            working_selector.setAttribute('height', Math.abs(f_y - i_y));
        }
    }
}

function interval_up_listener(target, param_name, x, y) {
    var vardict = {};
    if(x) { vardict['minx'] = Math.min(i_x,f_x); vardict['maxx'] = Math.max(i_x,f_x); }
    if(y) {
        var height = working_selector.parentElement.height.animVal.value;
        vardict['miny'] = (height - Math.max(i_y,f_y));
        vardict['maxy'] = (height - Math.min(i_y,f_y));
    }
    fetch('/param_update_plots', {
        method : 'POST',
        headers: {'Content-type' : 'application/json'},
        body : JSON.stringify({plot_id:working_selector.parentElement.parentElement.id, param: param_name, v_vs: vardict} )
    }).then((res) =>{
        return res.json();
    }).then((pairs) => {
        // we expect res to be a map of {id:html} pairs 
        pairs.map((p) => {
            document.getElementById(p.plot_id).outerHTML = p.html;
        });
    })
    working_selector = null;
    i_x = null;
    f_x = null;
    i_y = null;
    f_y = null;
}
</script>
<style>
svg circle {
    user-select: none;
    mouse-events: none;
}
svg text{
    user-select: None;
}
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

body {
    position: absolute;
    width: 100%;
    height: 100%;
    margin: 0;
    padding: 0;
}
.editor
.viewer {
    margin: 0;
    padding: 0;
    width:  59vw;
    height: 100%;
    display: block;
    float:  right;
}
.editor {
    float: left;
    background: lightgrey;
    width: 40vw;
}
textarea {
    background: inherit;
    width:100%;
    height:95%;
}
.editor > form {
    width: 100%;
    height: 100%;
    background: inherit;
}
</style>
'''

code = ''
plots = {}
conn = duckdb.connect(':memory:')

@app.route('/read_code', methods=['POST'])
def read_code():
    # reset, scorch earth on the DB (change this later)
    # change: don't make a new DB, but remove all params and data
    conn.sql('delete from data;')
    conn.sql('delete from params;')
    global code
    code = request.form.get('code')
    print("code in read_code",code)
    plots = parse.parse(conn, code)
    return redirect("/")
    

@app.route('/param_update_plots', methods=['POST'])
def param_update_plots() -> [{}]:
    req = request.get_json()
    print('param named',req['param'], '; v_vs', req['v_vs']);
    streams = conn.sql(f"""select distinct data_dependencies from params where name = '{req["param"]}' """).fetchall()
    print(streams)
    streams = set([j for i in [s[0] for s in streams] for j in i])
    streams = [f"'{i}'" for i in streams]
    print(streams)
    try:
        streams = conn.sql(f"""select name, code from data where name in ({",".join(streams)})""").fetchall()
    except:
        print(f"errored trying to query 'select name, code from data where name in ({','.join(streams)})' ")
    global plots
    # plot id, new html pairs
    ret = []
    for s in streams:
        print(s)
        swapped_data_def = s[1]
        for vv in req['v_vs']:
            #print(vv, req['v_vs'][vv], f'${req["param"]}.{vv}')
            translated_value = plots[int(req['plot_id'])].invert_selection(conn, vv[-1], req["v_vs"][vv])
            print(req["param"]+'.'+vv +':'+str(translated_value))
            swapped_data_def = swapped_data_def.replace(f'${req["param"]}.{vv}', f'{translated_value}')
        print(swapped_data_def)
        conn.sql(f"create or replace view {s[0]} as ({swapped_data_def}) ")
        dependencies = conn.sql(f"select plot_dependencies from data where name = '{s[0]}' ").fetchall()[0][0]
        print('plot dependencies', dependencies)
        for p in plots:
            for d in dependencies:
                if p == d:
                    print(f'updating plot #{p}')
                    ret.append({'plot_id':p , 'html':plots[p].html(conn, str(p))})
    print("returning:", len(ret) ,ret)
    return json.dumps(ret)

@app.route('/query', methods=['POST'])
def query():
    return conn.sql(request.get_json()).pl()._repr_html_()

def format_plots(plots):
    # the database stores all of the parsed params and streams needed at runtime
    print("params still visibile back in server main:", conn.sql("select distinct name, data_dependencies from params;").fetchall())
    print("data still visibile back in server main:", conn.sql("select distinct name, plot_dependencies from data;").fetchall())
    print(len(plots), 'plots displaying')
    ret = ''
    for p in plots:
        sql = plots[p].sql(conn)
        print(sql)
        ret += plots[p].html(conn, p)
    ret += '</div></body>' # close out the viewer div
    return ret

@app.route('/', methods=['GET','POST'])
def index():
    ret = HTML
    ret += f"""
        <body>
          <div class="editor">
            <form action="/read_code" method="POST">
              <textarea name="code">{code}</textarea>
              <input type="submit" value="Render >>" style="font-size: 20pt;"/>
            </form>
          </div>
          <div class="viewer">
        """
    ret += format_plots(plots)
    return ret

if __name__ == '__main__':
    conn.sql("create table params (name text, variable text, value float, def float, data_dependencies text[]); ")
    conn.sql("create table data (name text, code text, plot_dependencies int[]); ")
    print("tables: ",conn.sql("select table_name from information_schema.tables").fetchall())
    if(code == '' and len(sys.argv) == 2):
        code = open(sys.argv[1]).read()
        plots = parse.parse(conn, code)
    else:
        plots = {}
    from waitress import serve
    serve(app, host='localhost', port='5000')
