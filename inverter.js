document.onLoad() {
    var data = document.getElementsByTagName("td");
    for(const d of data) {
        d.addEventListener("click", invert_table(d));
    }
}

function invert_table(clicked_element) {
    // get the first element - this is the row-pivoted "groupby" value
    row_value = clicked_element.parentElement.children[0].innerHTML()
    // *should* assert this is a <th> and the thing was written right
    col_value = clicked_element.parentElement.parentElement.children[0].innerHTML()

    og_query = clicked_element.parentElement.parentElement.parentElement.firstChild().innerHTML()

    col_query = og_query.split(',')[clicked_element.cellIndex]
    if(clicked_element.cellIndex == 0) {
        col_query = col_query.substr(7) // removes 'select\n'
    }
    var re = /group by (\w+)\n/
    const rows = re.exec(og_query)
    // first "match" is always the full thing
    re = /(sum|avg|min|max)\(case when (\w+) = (['|\w]+) then (\w+) else 0 end\)/
    res = re.exec(column_agg_sql) 
    return 'select * from {data_source} where {res[2]} = {res[3]} and {rows} = {row_value}'
}
