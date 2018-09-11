from pydragon import Stream
from pydragon.util import identity


def test_basic():
    s1 = Stream(range(10)) \
            .concat(range(20)) \
            .exclude(lambda x: x % 4) \
            .map(lambda x: x - 1) \
            .collect(list)
    assert s1 == [-1, 3, 7, -1, 3, 7, 11, 15]

    s2 = Stream(["this is a test", "what a test"]) \
        .flat_map(str.split) \
        .collect(set)
    assert s2 == set("this is a test what a test".split())

    s3set = set()
    s3 = Stream("this is a test".split()) \
        .flat_map(identity) \
        .foreach(s3set.add)
    assert s3set == set("thisisatest")

    s4 = Stream(range(10)) \
        .reduce(lambda prev, this: prev * 2 + this)
    assert s4 == 1013

    s5 = Stream(range(10)) \
        .reduce(lambda prev, this: (prev[0] if this % 2 else prev[0] + [this], prev[1] if this % 4 else prev[1] + [this]), ([], []))
    assert s5 == (list(range(0, 10, 2)), list(range(0, 10, 4)))


def test_skip():
    s1 = Stream(range(10)) \
            .skip(5) \
            .collect(list)
    assert s1 == list(range(5, 10))

    s2 = Stream(range(20)) \
        .skip(5) \
        .skip(10) \
        .collect(list)
    assert s2 == list(range(15, 20))

    s3 = Stream(range(20)) \
        .filter(lambda x: x % 2) \
        .skip(15) \
        .collect(list)
    assert s3 == []


def test_limit():
    s1 = Stream(range(10)) \
        .limit(5) \
        .collect(list)
    assert s1 == list(range(5))

    s2 = Stream(range(10)) \
        .limit(5) \
        .limit(10) \
        .collect(list)
    assert s2 == list(range(5))

    s3 = Stream(range(10)) \
        .limit(10) \
        .limit(5) \
        .collect(list)
    assert s3 == list(range(5))


def test_skip_limit():
    s1 = Stream(range(20)) \
        .skip(10) \
        .limit(5) \
        .collect(list)
    assert s1 == list(range(10, 15))

    s2 = Stream(range(20)) \
        .limit(5) \
        .skip(10) \
        .collect(list)
    assert s2 == []