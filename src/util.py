import collections


def printf(val):
    if isinstance(val, collections.Iterable):
        for i in val:
            print(i, end='\n')
    else:
        print(val)


def parseToList(row):
    ar = []
    f = False
    t = []
    for x in row:
        if x == '"':
            f = not f
        elif f or x != ',':
            t.append(x)
        else:
            ar.append(''.join(t).strip())
            t.clear()
    r = ''.join(t).strip()
    # if len(r)>0:
    ar.append(r)
    return ar


def parseToTuple(row):
    return tuple(parseToList(row))
