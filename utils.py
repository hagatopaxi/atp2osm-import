def limit_offset(count, step):
    """
    Generator that returns tuple of (start, end) for each step in a range for a database limit offset values.
    For example, limit_offset(14, 5) will yield (0, 5), (5, 10), (10, 14).
    """
    for i in range(0, count, step):
        yield (i, min(i + step, count))