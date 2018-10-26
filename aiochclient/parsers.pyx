ESC_CHR_MAPPING = {
    b"b": b"\b",
    b"N": b"\N",  # NULL
    b"f": b"\f",
    b"r": b"\r",
    b"n": b"\n",
    b"t": b"\t",
    b"0": b" ",
    b"'": b"'",
    b"\\": b"\\",
}

DQ = "'"
CM = ","


def decode(val):
    return _decode(val).decode()


cdef _decode(bytes val):
    """
    Converting bytes from clickhouse with
    backslash-escaped special characters
    to pythonic string format
    """
    n = val.find(b"\\")
    if n < 0:
        return val
    n += 1
    d = val[:n]
    b = val[n:]
    while b:
        d = d[:-1] + ESC_CHR_MAPPING.get(b[0:1], b[0:1])
        b = b[1:]
        n = b.find(b"\\")
        if n < 0:
            d = d + b
            break
        n += 1
        d = d + b[:n]
        b = b[n:]
    return d


def seq_parser(raw):
    """
    Generator for parsing tuples and arrays.
    Returns elements one by one
    """
    cur = []
    blocked = False
    if not raw:
        return None
    for sym in raw:
        if sym == CM and not blocked:
            yield "".join(cur)
            cur = []
        elif sym == DQ:
            blocked = not blocked
            cur.append(sym)
        else:
            cur.append(sym)
    yield "".join(cur)
