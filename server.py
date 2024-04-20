from flask import Flask
import typing
import duckdb

app = Flask(__name__)

global HTML
#def read_spec(code:str) -> Spec
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
        

def pivot_table_SQL(row, col, value, data_source) -> str:
    #assert spec.plot == 'TABLE'
    col_values = duckdb.query(f"select distinct {col} from '{data_source}'").fetchall()
    col_values = [c[0] for c in col_values]
    print(col_values)
    value = split_agg(value)
    ret = f'select {row},\n'
    for cv in col_values:
        if value.agg_func == 'COUNT':
            ret += f"sum(case when {col} = '{cv}' then 1 else 0 end) as '{cv}'"
        elif value.agg_func == 'SUM':
            #assert col_type in ['FLOAT', 'NUMERIC', 'INT']
            ret += f"sum(case when {col} = '{cv}' then {value.ref} else 0 end) as '{cv}'"
        elif value.agg_func == 'AVG':
            #assert col_type in ['FLOAT', 'NUMERIC', 'INT']
            ret += f"avg(case when {col} = '{cv}' then {value.ref} else 0 end) as '{cv}'"
        elif value.agg_func == 'MAX':
            ret += f"max(case when {col} = '{cv}' then {value.ref} else 0 end) as '{cv}'"
        elif value.agg_func == 'MIN':
            ret += f"min(case when {col} = '{cv}' then {value.ref} else 0 end) as '{cv}'"
        ret += ',\n' # duckdb is ok with trailing commas
    ret += f"from '{data_source}' group by {row}"
    return ret

def dot_SQL(x, y, data_source, color:str = None, size:str = None) -> str:
    ret = 'select {x}, {y},'
    if color is not None:
        ret += ' {color}, '
    if size is not None:
        ret += ' {size} '
    ret += 'from {data_source}'
    return ret

@app.route('/')
def index():
    global HTML
    return HTML

if __name__ == '__main__':
    sql = pivot_table_SQL('state_code', 'loan_type', 'avg(loan_amount)'
                          ,'../data/hmda/2023_combined_mlar.parquet')
    print(sql)
    res = duckdb.sql(sql)
    print(res)
    HTML = res.df().to_html()
    app.run()
