from streamer import Stream
from streamer.operator import RepeatApply
from streamer.collector import Collector
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

    assert Stream(range(10)) \
        .map(str) \
        .reduce_right(lambda prev, this: prev + this, "") == "9876543210"


def test_string_is_streamed():
    assert Stream("a string").collect(list) == list("a string")


def test_distinct():
    s1 = Stream(range(10)) \
        .concat(range(20)) \
        .distinct() \
        .collect(list)
    assert s1 == list(range(20))

    assert Stream("abcba").distinct().collect("".join) == "abc"
    assert Stream("ddabbbcaca").distinct(more_than=3).collect("".join) == "ba"
    assert Stream(range(9, 1, -1)) \
        .distinct(key=lambda x: x % 2) \
        .collect_as_set() == {9, 8}


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

    sample_map = {int(x): i for i, x in enumerate('4738')}
    assert Stream(range(10)) \
        .map(lambda x: sample_map.get(x)) \
        .not_none() \
        .collect_as_set() == set(sample_map.values())

    assert Stream(range(10)) \
        .without(*sample_map.keys()) \
        .collect_as_set() == set(range(10)) - set(sample_map.keys())


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


def test_static_of_method():
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

    ref = []
    assert Stream(range(10)) \
        .peek(ref.append) \
        .collect_as_list() == list(range(10))
    assert ref[0] == 0


def test_sort():
    assert Stream(range(10)) \
        .sorted(lambda x: (x % 2, x)) \
        .collect_as_list() == list(range(0, 10, 2)) + list(range(1, 10, 2))

    assert Stream('abcd') \
        .sorted() \
        .sorted(reverse=True) \
        .collect("".join) == "dcba"

    assert Stream.of_list(4, 7, 3, 8) \
        .sorted() \
        .collect_as_list() == [3, 4, 7, 8]


def test_intersperse():
    sample_string = "this is a test"
    assert Stream(sample_string.split()) \
        .intersperse(" ") \
        .collect("".join) == sample_string

    assert Stream([]).intersperse("any").collect("".join) == ""
    assert Stream("1").intersperse("great").collect("".join) == "1"

    assert Stream([None] * 2).intersperse(None).collect_as_list() == [None] * 3


def test_cross_function():
    assert Stream(range(10)) \
        .cross([1, 2]) \
        .collect_as_list() == Stream(range(10)) \
        .cross_result_of(lambda x: [1, 2]) \
        .collect_as_list() == [(x, y) for x in range(10) for y in [1, 2]]


def test_pair_up():
    assert Stream(range(10)).map_pairs(lambda pair: pair[0] - pair[1]).collect_as_set() == {-1}

    s = set()
    Stream(range(10)).for_pairs(s.add)
    assert s == {(x, x + 1) for x in range(9)}

    assert Stream([]).map_pairs(lambda p: p).collect_as_set() == set()
    assert Stream('1').map_pairs(lambda p: p).collect_as_set() == set()

    assert Stream([None] * 3).map_pairs(lambda p: p[0] is None and p[1] is None).collect_as_list() == [True, True]


def test_zip():
    assert Stream(range(10)) \
        .zip_with(range(2), range(5)) \
        .collect_as_list() == list(zip(range(10), range(2), range(5)))

    assert Stream(range(10)) \
        .zip_with(range(2), range(5), fill_none=True) \
        .collect_as_list() == [(i, i if i < 2 else None, i if i < 5 else None) for i in range(10)]

    assert Stream([None for _ in range(5)]) \
        .zip_with(range(2), fill_none=True) \
        .collect_as_list() == [(None, i if i < 2 else None) for i in range(5)]

    assert Stream(range(10)) \
        .zip_with([]) \
        .collect_as_set() == set()

    s = Stream(range(10))
    assert s.zip_with() == s


def test_combiner():
    assert Stream(range(10)) \
        .collapse_to_first(lambda x, y: (x * y) % 3 == 0) \
        .collect_as_list() == [0, 2, 5, 8]
    assert Stream(range(10)) \
        .collapse_and_combine(lambda x, y: (x * y) % 3 == 0, lambda x, y: x + y) \
        .collect_as_list() == [1, 9, 18, 17]
    assert Stream(range(10)) \
        .collapse_and_collect(lambda x, y: (x * y) % 3 == 0, Collector.of(list)) \
        .collect_as_list() == [[0, 1], [2, 3, 4], [5, 6, 7], [8, 9]]

    assert Stream([]).collapse_to_first(lambda *_: True).collect_as_set() == set()
    assert Stream("1").collapse_to_first(lambda *_: True).collect_as_list() == ['1']
    assert Stream(range(10)).collapse_to_first(lambda *_: False).collect_as_list() == list(range(10))

    assert Stream(range(10)) \
        .map(lambda x: None if x % 3 else x) \
        .collapse_to_first(lambda _, y: y is None) \
        .collect_as_list() == [x for x in range(10) if x % 3 == 0]

    assert Stream(range(10)) \
        .adjacent_groups(lambda x, y: (x - 5) * (y - 5) > 0) \
        .collect_as_list() == [[0, 1, 2, 3, 4], [5], [6, 7, 8, 9]]

    assert Stream(range(10)) \
        .adjacent_key_groups(lambda x: x < 5) \
        .collect_dict() == {
            True: [0, 1, 2, 3, 4],
            False: [5, 6, 7, 8, 9]}


def test_grouper():
    assert Stream(range(10)) \
        .group_to_map(lambda x: x % 2) == {0: list(range(0, 10, 2)), 1: list(range(1, 10, 2))}
    assert Stream([None] * 3) \
        .group_to_map(lambda x: x is None) == {True: [None] * 3}
    assert Stream([]).group_to_map(lambda x: x) == {}
