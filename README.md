G(rammar) of G(raphics) Language

Inspired by Vega and Mosaic, this project is a a barebones data visualization language that aims to be a thin wrapper around SQL.

GGLANG is its own langauge, accepting "marks" of `dot`, `text` and `table` and coordinating interaction between them. What starts as a basic scatter plot + table can be filtered down by dragging a selection box over the scatter plot:
![unselected](https://github.com/user-attachments/assets/a7926106-a964-4ea2-9430-a32c147aa171)

![selected](https://github.com/user-attachments/assets/227dc1d1-a485-4ef7-a02e-c16a8f7c820d)

DuckDB in the backend makes this able to read data from almost anywhere... no GUI import required!
