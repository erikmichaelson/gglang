from flask import Flask
import typing
import duckdb
import parse
import sys

app = Flask(__name__)

global HTML
#def read_spec(code:str) -> Spec

@app.route('/')
def index():
    global HTML
    return HTML

if __name__ == '__main__':
    plot = parse.parse(open(sys.argv[1]).read())
    sql = plot.sql()
    print(sql)
    res = duckdb.sql(sql)
    print(res)
    HTML = res.df().to_html()
