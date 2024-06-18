import unittest
from parse import parse
import gg_types as gg
import duckdb

class parser_tests(unittest.TestCase):
    def test_selector(self):
        d = gg.Dot(x='econ.landvalue',y='econ.buildvalue', data_name='econ',
            param={'name':'amt', 'variables':['x','y']})
        txt = gg.Text(value=['price_range.pct_in_sel'], data_name='price_range')
        tab = gg.Table(value=['si.cityname','si.schooldist','si.yearbuilt'], data_name='si')
        db = duckdb.connect(':memory:')
        db.sql("create table params (name text, variable text, value float, def float, data_dependencies text[]); ")
        db.sql("create table data (name text, code text, plot_dependencies int[]); ")
        plots = parse(db, open('selector.gg').read())
        exp = {0:d, 1:txt, 2:tab}
        for p in plots:
            assert plots[p] == exp[p], f"expected {exp[p].__dict__}, got {plots[p].__dict__}"
            #self.assertEqual(plots[p], exp[p])
        #self.assertEqual(db."select * from ",)
    def test_dot(self):
        d = gg.Dot(x='tracts.aland',y='tracts.AWATER',data_name='tracts')
        db = duckdb.connect(':memory:')
        db.sql("create table params (name text, variable text, value float, def float, data_dependencies text[]); ")
        db.sql("create table data (name text, code text, plot_dependencies int[]); ")
        assert parse(db, open('dot.gg').read())[0] == d


if __name__ == '__main__':
    unittest.main()
