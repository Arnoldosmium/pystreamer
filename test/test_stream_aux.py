from streamer import streams, Stream
import io


def test_cartesian():
    assert streams.cartesian_product_stream(range(10), range(5)) \
        .collect_as_set() == {(x, y) for x in range(10) for y in range(5)}

    assert streams.cartesian_product_stream(iter([]), Stream(range(10))) \
        .collect_as_set() == set()

    assert streams.cartesian_power(3, list(range(4))) \
        .collect_as_set() == {(x, y, z) for x in range(4) for y in range(4) for z in range(4)}

    assert streams.cartesian_product_stream(range(10), streams.constant_of(None, 3)) \
        .group_by(lambda x: x) \
        .map_values(len) \
        .collect_dict() == {(i, None): 3 for i in range(10)}


def test_simple_generators():
    assert streams.constant_of(1, 100).collect_as_list() == [1] * 100
    assert streams.generate(lambda: 1).limit(10).collect_as_list() == [1] * 10
    assert streams.iterate(1, lambda x: x * 2).limit(10).collect_as_list() == [1 << x for x in range(10)]


def test_text_splits():
    assert streams.lines_of("\n".join(str(x) for x in range(10))) \
        .map(int) \
        .collect_as_list() == list(range(10))

    assert streams.lines_of(io.StringIO("ff\ngg")).collect_as_set() == {"ff\n", "gg"}

    assert streams.split("ff\ngg", "\n").collect_as_set() == {"ff", "gg"}
    assert streams.split("ff\ngg\n\nff\nhh\n\n", "\n\n").collect_as_set() == {"ff\ngg", "ff\nhh", ""}

    buf = io.StringIO()
    buf.write("a" * 100)
    buf.write("m" * 2)      # test empty string in the middle
    buf.write("a" * io.DEFAULT_BUFFER_SIZE)     # test cross buffer string concat
    buf.write("m")
    buf.write("a" * (io.DEFAULT_BUFFER_SIZE - 104))
    buf.write("m")      # test right on the edge splitter
    buf.write("aaa")      # test last buffer read with string leftovers
    buf.seek(0)
    assert streams.split(buf, "m").collect_as_list() == [
        "a" * 100, "", "a" * io.DEFAULT_BUFFER_SIZE, "a" * (io.DEFAULT_BUFFER_SIZE - 104), "aaa"]