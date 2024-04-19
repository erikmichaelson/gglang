import sys

def tokenize(string):
    pass

# so good news is I'm pretty sure Vega-Lite is just a wrapper around
# vega. The tree should be basically some decorations on top of the 
def transpile(string):
    ret = ''
    tokens = string.split(' ')
    # jerry-rigged stack stucture on a list cuz I'm lazy
    # stack[-1] is the top. Append to push, slice [:-1] to pop
    stack = []
    for t in tokens:
        print(t, '\tstack:', stack)
        if t == 'DATA':
            assert len(stack) == 0 or stack[-1] in ['LAYER','VCONCAT','HCONCAT'],\
                    'DATA is a top-level keyword, and can only be nested below views'
            stack.append(t)
            ret += '"data" : {\n'
        elif t in ['BAR','RECT','POINT','CIRCLE']:
            stack.push(t)
            ret = '"mark":"{t}"\n'
        elif t in ['ENCODING']:
            assert len(stack) == 0 or stack[-1] in ['LAYER','VCONCAT','HCONCAT'],\
                    'ENCODING is a top-level keyword, and can only be nested below views'
            ret = '"encoding":'
        elif t in ['X','Y','COLOR','TOOLTIP','OPACITY']:
            assert stack[-1] == 'ENCODING', f'{t} is an ENCODING level keyword'
            stack.append(t)
            ret += ''
        elif t in ['TRANSFORM']:
            ret += '"transform":'
        elif t == ['CONDITION']:
            assert stack[-1] in ['COLOR']
        # TODO: handle data more elegantly / powerfully than this
        elif stack[-1] == 'DATA' and '.' in t:
            try: 
                l = t.split('.')[1]
                if l in ['csv','parquet']:
                    ret += t + '\n}\n'
                    stack = stack[:-1]
            except:
                print(t)

    return ret
    

if __name__ == "__main__":
    print(transpile(open(sys.argv[1]).read()))
