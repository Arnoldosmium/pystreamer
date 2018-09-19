def streamify(stream_or_object):

    # iterators w/ py2 support
    if hasattr(stream_or_object, "__next__") or hasattr(stream_or_object, "next"):
        return stream_or_object

    # iterables
    elif hasattr(stream_or_object, "__iter__") or hasattr(stream_or_object, "__getitem__"):
        return iter(stream_or_object)

    else:
        return iter([stream_or_object])


def identity(x):
    return x
