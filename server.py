from flask import Flask
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

function invert_table(clicked_element) {
    console.log("clicked_element called")
    // get the first element - this is the row-pivoted "groupby" value
    row_value = clicked_element.parentElement.children[0].innerHTML
    // *should* assert this is a <th> and the thing was written right
    col_value = clicked_element.parentElement.parentElement.children[0].innerHTML

    og_query = clicked_element.parentElement.parentElement.parentElement.parentElement.parentElement.firstChild.innerHTML

    col_query = og_query.split(',')[clicked_element.cellIndex]
    if(clicked_element.cellIndex == 0) {
        col_query = col_query.substr(7) // removes 'select\\n'
    }
    var re = /group by (\w+)\\n/
    const rows = re.exec(og_query)
    // first "match" is always the full thing
    re = /(sum|avg|min|max)\(case when (\w+) = (['|\w]+) then (\w+) else 0 end\)/
    res = re.exec(col_query) 
    re = /from ([\.|'|/|\w]+)/
    data_source = re.exec(og_query)[1]
    console.log('select * from '+data_source+' where '+res[2]+' = '+res[3]+' and '+rows+' = '+row_value)
    return 'select * from {data_source} where {res[2]} = {res[3]} and {rows} = {row_value}'
}
</script>
<style>
.query {
    visibility:hidden;
    height: 0;
}
</style>'''


@app.route('/')
def index():
    global HTML
    return HTML

def recreate(sql:str) -> str:
    table_object = f'<div><div class="query">{sql}</div>'
    table_object += duckdb.sql(sql).pl()._repr_html_()
    table_object += '</div>'
    return table_object

if __name__ == '__main__':
    plot = parse.parse(open(sys.argv[1]).read())
    sql = plot.sql()
    print(sql)
    HTML += recreate(sql)
    app.run()
