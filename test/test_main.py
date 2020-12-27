from streamer import Stream
from streamer.operator import RepeatApply
from .util import identity


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
    Stream("this is a test".split()) \
        .flat_map(identity) \
        .foreach(s3set.add)
    assert s3set == set("thisisatest")

    s4 = Stream(range(10)) \
        .reduce(lambda prev, this: prev * 2 + this)
    assert s4 == 1013

    s5 = Stream(range(10)) \
        .reduce(lambda prev, this: (
            prev[0] if this % 2 else prev[0] + [this],
            prev[1] if this % 4 else prev[1] + [this]), ([], []))
    assert s5 == (list(range(0, 10, 2)), list(range(0, 10, 4)))

    s6dict = {}
    Stream(list("abcd")) \
        .foreach_index(lambda i, ch: s6dict.__setitem__(i, ch))
    s6dict2 = Stream(list("abcd")) \
        .map_with_index(lambda i, ch: (i, ch)) \
        .collect_dict()
    assert s6dict == dict(enumerate("abcd")) == s6dict2


def test_string_is_streamed():
    assert Stream("a string").collect(list) == list("a string")


def test_distinct():
    s1 = Stream(range(10)) \
        .concat(range(20)) \
        .distinct() \
        .collect(list)
    assert s1 == list(range(20))

    assert Stream("abcba").distinct().collect("".join) == "abc"


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


def test_conditional_cutoff_and_skip():
    random_str = "dlakfjaskdjflwerijaskljakflsjcioaofjalkxcjar"

    s1 = Stream("random_string:%s" % random_str) \
        .skip_util(lambda ch: ch == ":") \
        .skip(1) \
        .collect("".join)
    assert s1 == random_str

    s2 = Stream("random_string:%s" % random_str) \
        .skip_util(lambda ch: ch == ":") \
        .skip(1) \
        .cutoff_if(lambda ch: ch == "r") \
        .collect("".join)
    assert s2 == random_str[:random_str.find("r")]

    s3 = Stream(RepeatApply(1, lambda x: x + 1)) \
        .takewhile(lambda x: x % 10) \
        .collect_as_list()
    assert s3 == list(range(1, 10))


def test_static_of_operation():
    explicit = ["this", "is", "an", "explicit", "list"]
    assert Stream.of_list(*explicit) \
        .collect(list) \
        == explicit


def test_count_collection():
    assert Stream(range(1, 101)) \
        .flat_map(range) \
        .count() == 5050


def test_stream_boolean_tests():
    example = list(range(10))
    assert Stream(example).map(str).all_match(lambda s: len(s) == 1)
    assert Stream(example).any_match(lambda x: x > 8)
    assert Stream(example).none_match(lambda x: x < 0)


def test_find_a_element():
    assert Stream("abcd").find_first() == 'a'
    assert Stream(range(10)).filter(lambda x: x < 0).find_any() is None
