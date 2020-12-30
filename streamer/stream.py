# -*- coding: utf-8 -*-

"""
streamer.stream
---

The main module with Stream, DictStream implementations
"""

from itertools import chain, islice, dropwhile, takewhile, starmap
from functools import reduce
from typing import Callable, Union, List, Set, Iterator, Iterable, TypeVar, Generic, Dict, Tuple, Any
from .util import to_iterator
from .operator import Deduplicator, Inserter, PairUp, Zipper, Collapser, Grouper
from .collector import Collector, CountCollector

T = TypeVar('T')
R = TypeVar('R')
ElementOrIter = Union[Iterable[T], Iterator[T], T]

K = TypeVar('K')
V = TypeVar('V')


class Stream(Generic[T]):
    """
    The Stream class is the chainable wrapper class around any generators / iterators
    Generators and iterators are merged together, and the sequence is preserved - the same at creation time.
    No evaluation will actually happen unless a terminal / near-terminal operation is executed.
    """

    @staticmethod
    def _prepare_stream(*generators_or_iterables: ElementOrIter) -> Iterator[T]:
        if len(generators_or_iterables) == 1:
            return to_iterator(generators_or_iterables[0])
        else:
            return chain.from_iterable(map(to_iterator, generators_or_iterables))

    @staticmethod
    def of_list(*elements: T):
        return Stream(elements)

    def __init__(self, *generators_or_iterables: ElementOrIter):
        """
        Initialize Stream objects
        :param generators_or_iterables: any numbers of generators or iterables
        """
        self.__stream = Stream._prepare_stream(*generators_or_iterables)

    def __next__(self) -> T:
        return next(self.__stream)

    # to iterator
    def __iter__(self) -> Iterator[T]:
        return self.__stream

    ###
    # Common operations
    ###

    def add(self, *generators_or_iterables: ElementOrIter):
        """
        Append elements in iterators to the end of current stream
        :param generators_or_iterables: any numbers of generators or iterables
        :return: New Stream instance wrapping the union stream
        """
        return Stream(self.__stream, *generators_or_iterables)

    def chain(self, *generators_or_iterables: ElementOrIter):
        """
        (alias of `add`) Append elements in iterators to the end of current stream
        :param generators_or_iterables: any numbers of generators or iterables
        :return: New Stream instance wrapping the union stream
        """
        return self.add(*generators_or_iterables)

    def concat(self, *generators_or_iterables: ElementOrIter):
        """
        (alias of `add`) Append elements in iterators to the end of current stream
        :param generators_or_iterables: any numbers of generators or iterables
        :return: New Stream instance wrapping the union stream
        """
        return self.add(*generators_or_iterables)

    def enumerate(self):
        """
        Similar to builtin enumerate function
        :return: A Stream with original stream enumerated
        """
        return Stream(enumerate(self.__stream))

    def map(self, func: Callable[[T], R]):
        """
        Equivalent to builtin map function
        :param func: function each element will be passed to for transformation
        :return: New Stream instance wrapping the mapped stream
        """
        return Stream(map(func, self.__stream))

    def map_with_index(self, func: Callable[[int, T], R]):
        """
        Iterate through all elements with its index supplied
        :param func: ([int index, element] -> any) function each element and its index will be passed to
        :return: New Stream instance wrapping the mapped stream
        """
        return Stream(func(i, elem) for i, elem in enumerate(self.__stream))

    def flat_map(self, func: Callable[[T], R]):
        """
        Flattening (once) if any element is a collection or iterator
        :param func: function each current element will be passed to
        :return: New Stream instance wrapping the flat_mapped stream
        """
        return Stream(chain.from_iterable(map(to_iterator, map(func, self.__stream))))

    def filter(self, func: Callable[[T], bool]):
        """
        Pick elements that pass the test
        :param func: (element -> boolean) function each current element will be tested against
        :return: New Stream instance wrapping the filtered stream
        """
        return Stream(filter(func, self.__stream))

    def exclude(self, func: Callable[[T], bool]):
        """
        Filter out elements that pass the test
        :param func: (element -> boolean) function each current element will be tested against
        :return: New Stream instance wrapping the filtered stream
        """
        return Stream(elem for elem in self.__stream if not func(elem))

    def minus(self, func: Callable[[T], bool]):
        """
        (alias of `exclude`) Filter out elements that pass the test
        :param func: (element -> boolean) function each current element will be tested against
        :return: New Stream instance wrapping the filtered stream
        """
        return self.exclude(func)

    def not_none(self):
        """
        Filter out elements that are `None`s
        :return: New stream without None
        """
        return self.exclude(lambda x: x is None)

    def without(self, *exclusion: T):
        """
        Filter out elements that are in exclusion list
        :param exclusion: items to exclude
        :return: New stream with exclusion filtered
        """
        if len(exclusion) == 0:
            return self

        all_exclusions = set(exclusion)
        return Stream(elem for elem in self.__stream if elem not in all_exclusions)

    def peek(self, func: Callable[[T], None], raise_on_error: bool = False):
        """
        Run function on the first element without altering or consuming it.
        :param func: function to run on first element
        :param raise_on_error: flag to enforce raising no element error
        :return: Effectively same stream
        """
        maybe_elem = self.find_first()
        if maybe_elem is None:
            if raise_on_error:
                raise ValueError("The stream is empty, cannot apply `peek` to it")
            return self
        else:
            func(maybe_elem)
        return Stream((maybe_elem,), self)

    ###
    # Terminal operations
    ###

    def collect(self, collector: Union[Callable[[Iterator[T]], R], Collector[T, Any, R]]) -> R:
        """
        [Terminal operation] passes the stream to collector
        :param collector: (Stream -> any) function iterates through the stream
        :return: the result after piping stream to collector function
        """
        if isinstance(collector, Collector):
            return collector.collect(self)
        elif callable(collector):
            return Collector.of(collector).collect(self)
        else:
            raise ValueError("Collect seems to be neither a collector nor a callable function.")

    def collect_as_list(self) -> List[T]:
        """
        [Terminal operation] convert to a list
        :return: List
        """
        return list(self)

    def collect_as_set(self) -> Set[T]:
        """
        [Terminal operation] convert to a set
        :return: Set
        """
        return set(self)

    def collect_dict(self, dict_collector: Callable[[Iterator[T]], Dict] = dict):
        """
        [Terminal operation] form a map/dict with the 1st element from each stream candidate as keys and rest as value
        :param dict_collector: (Stream<I extends map.entry> -> Dict<K, V>) function iterates through the stream
        :return: the result after piping stream to dict collector function
        """
        return dict_collector(self.wrap_as_dict_stream())

    def build_dict(self, dict_collector: Callable[[Iterator[T]], Dict] = dict):
        """
        (alias of collect_dict) [Terminal operation] passes the stream to dict collector
        :param dict_collector: (stream<I extends item> -> D extends dict) function iterates through the item stream
        :return: the result after piping stream to dict collector function
        """
        return self.collect_dict(dict_collector)

    def collect_as_map(self, map_collector: Callable[[Iterator[T]], Dict] = dict):
        """
        (alias of collect_dict) [Terminal operation] passes the stream to dict collector
        :param map_collector: (stream<I extends item> -> D extends dict) function iterates through the item stream
        :return: the result after piping stream to dict collector function
        """
        return self.collect_dict(map_collector)

    def reduce(self, reducer: Callable[[R, T], R], initial_value: Union[R, None] = None):
        """
        [Terminal operation] equivalent to functool reduce. Passes the stream to reducer
        :param reducer: (T extends any, element -> T) function takes each item and produce an (aggregated) value
        :param initial_value: (T) optional value, served as the starting value.
        :return: the result after reducing the stream
        TODO: short circuitable reduction
        """
        if initial_value is not None:
            return reduce(reducer, self.__stream, initial_value)
        else:
            return reduce(reducer, self.__stream)

    def reduce_right(self, reducer: Callable[[R, T], R], initial_value: Union[R, None] = None) -> R:
        """
        [Terminal operation] equivalent to functool reduce. Passes the stream in reverse order to reducer
        :param reducer: (R result, T element -> T) function takes each item and produce an (aggregated) value
        :param initial_value: (R) optional value, served as the starting value.
        :return: R - the result after reducing the stream
        """
        reversed_seq = list(self.__stream)[::-1]
        if initial_value is not None:
            return reduce(reducer, reversed_seq, initial_value)
        else:
            return reduce(reducer, reversed_seq)

    def foreach(self, func: Callable[[T], None]) -> None:
        """
        [Terminal operation] passes the stream to func
        :param func: (element -> void) function runs on each element
        """
        for element in self:
            func(element)

    def foreach_index(self, func: Callable[[int, T], None]) -> None:
        """
        [Terminal operation] passes the stream to func
        :param func: (element -> void) function runs on each element
        """
        for i, element in enumerate(self):
            func(i, element)

    def any_match(self, match: Callable[[T], bool]) -> bool:
        """
        [Terminal operation] test if any element in the stream passes the predicate test
        :param match: (element -> bool) function tests each element
        :return: test result
        """
        for item in self.__stream:
            if match(item):
                return True
        return False

    def has_all(self, *candidate: T) -> bool:
        """
        [Terminal operation] test if all candidates exist in the stream
        :param candidate: candidates to check
        :return: test result
        """
        candidates = set(candidate)
        for item in self.__stream:
            if item in candidates:
                candidates.remove(item)
            if len(candidates) == 0:
                return True
        return False

    def has_any(self, *candidate: T) -> bool:
        """
        [Terminal operation] test if at least one of the candidates exist in the stream
        :param candidate: candidates to check
        :return: test result
        """
        candidates = set(candidate)
        return self.any_match(lambda item: item in candidates)

    def all_match(self, match: Callable[[T], bool]) -> bool:
        """
        [Terminal operation] test if all elements in the stream pass the predicate test
        :param match: (element -> bool) function tests each element
        :return: test result
        """
        for item in self.__stream:
            if not match(item):
                return False
        return True

    def none_match(self, match: Callable[[T], bool]) -> bool:
        """
        [Terminal operation] test if none of elements in the stream pass the predicate test
        :param match: (element -> bool) function tests each element
        :return: test result
        """
        return not self.any_match(match)

    def count(self) -> int:
        """
        [Terminal operation] count total number of elements
        :return: total number
        """
        return self.collect(CountCollector())

    def sorted(self, key: Union[Callable[[T], Any], None] = None, reverse: bool = False):
        """
        [Near terminal operation] effectively collects all element for comparison and sorting for a sorted stream
        :param key: optional comparison method
        :param reverse: if the order of sort should be reversed (decreasing order)
        :return: A sorted stream
        TODO: stream sort condition marker
        TODO: parallel implementation
        """
        return Stream(sorted(self, key=key, reverse=reverse))

    def for_pairs(self, func: Callable[[Tuple[T, T]], None]) -> None:
        """
        [Terminal operation] apply function to every adjacent pair
        :param func: function applying on all adjacent pairs
        """
        for pair in PairUp(self.__stream):
            func(pair)

    ###
    # Element extraction method
    ###

    def find_any(self) -> Union[T, None]:
        """
        [Terminal operation] get any one element in the stream
        :return: Optional[T] - maybe element
        """
        try:
            return next(self)
        except StopIteration:
            return None

    def find_first(self) -> Union[T, None]:
        """
        [Terminal operation] get first element in the stream
        The same as find_any in single thread stream
        :return: Optional[T] - maybe element
        """
        return self.find_any()

    ###
    # Element removal Operations
    ###

    def distinct(self, *, more_than: int = 1, key: Union[Callable[[T], Any], None] = None):
        """
        Gives stream with distinct elements
        :param more_than - only show elements that appear at least this number of times; at least 1
        :param key - apply this function to the element for de-duplicating; \
            only first of the elements have same key will be preserved.
        :return: stream with distinct elements
        TODO: stream distinct condition marker
        """
        return Stream(Deduplicator(self.__stream, more_than=more_than, key=key))

    def limit(self, num: int):
        """
        Limit the stream to certain amount of elements
        :param num: number of elements stream limit to
        :return: the limited stream
        """
        if num > 0:
            return Stream(islice(self.__stream, num))
        return self

    def takewhile(self, func: Callable[[T], bool]):
        """
        Limit the stream to the first element on which test fails; equivalent to itertools takewhile
        :param func: (element -> boolean) function each current element will be tested against
        :return: the limited stream
        """
        return Stream(takewhile(func, self.__stream))

    def cutoff_if(self, func: Callable[[T], bool]):
        """
        Limit the stream to the first element on which test succeeds
        :param func: (element -> boolean) function each current element will be tested against
        :return: the limited stream
        """
        return self.takewhile(lambda x: not func(x))

    def skip(self, num: int):
        """
        Skip the first several elements in the stream
        :param num: number of elements stream to skip
        :return: the skipped stream
        """
        if num > 0:
            return Stream(islice(self.__stream, num, None))
        return self

    def dropwhile(self, func: Callable[[T], bool]):
        """
        Skip the stream util the first element on which test fails; equivalent to itertools dropwhile
        :param func: (element -> boolean) function each current element will be tested against
        :return: the skipped stream
        """
        return Stream(dropwhile(func, self.__stream))

    def skip_util(self, func: Callable[[T], bool]):
        """
        Skip the stream util the first element on which test succeeds
        :param func: (element -> boolean) function each current element will be tested against
        :return: the skipped stream
        """
        return self.dropwhile(lambda x: not func(x))

    ###
    # Advanced operations
    ###

    def max(self, key: Union[Callable[[T], Any], None] = None):
        """
        [Terminal operation] grab the max value in the stream
        :param key: (element -> C extends comparable) optional evaluator to compare elements
        :return: the max element in the stream
        """
        if key is None:
            return max(self)
        return max(self, key=key)

    def min(self, key: Union[Callable[[T], Any], None] = None):
        """
        [Terminal operation] grab the min value in the stream
        :param key: (element -> C extends comparable) optional evaluator to compare elements
        :return: the min element in the stream
        """
        if key is None:
            return min(self)
        return min(self, key=key)

    def intersperse(self, delimiter: T):
        """
        Creates a new stream in which the delimiter is inserted in between every adjacent elements.
        :param delimiter: same kind of existing elements
        :return: a new stream
        """
        return Stream(Inserter(self, delimiter))

    def cross(self, elements: Iterable[R]):
        """
        Creates a new stream contain every pair of values and the cross elements.
        :param elements: for cross product
        :return: a new stream
        """
        element_list = list(elements)   # Cannot assume `elements` is a distinct set
        return Stream((item, elem) for item in self.__stream for elem in element_list)

    def cross_result_of(self, generate: Callable[[T], Iterator[R]]):
        """
        Creates a new stream contain every pair of values and the results of generate function.
        :param generate: generator function
        :return: a new stream
        """
        return Stream((item, value) for item in self.__stream for value in generate(item))

    def map_pairs(self, func: Callable[[Tuple[T, T]], R]):
        """
        Maps to new item on every adjacent pair
        :param func: mapping function applying on all adjacent pairs
        """
        return Stream(func(pair) for pair in PairUp(self.__stream))

    def map_to_entry(self, to_entry: Callable[[T], Tuple[K, V]]):
        """
        Creates a dict stream, entries of which are created by applying `to_entry` to existing element.
        :param to_entry: function to create K, V tuple pair from existing elements
        :return: DictStream
        """
        return DictStream(wrap=map(to_entry, self.__stream))

    def map_to_key_value(self, to_key: Callable[[T], K], to_val: Callable[[T], V]):
        """
        Creates a dict stream, entries of which are key-value pairs by applying `to_key` `to_val` to existing element,
        respectively.
        :param to_key: function to create key from existing elements
        :param to_val: function to create val from existing elements
        :return: DictStream
        """
        return DictStream(wrap=((to_key(elem), to_val(elem)) for elem in self.__stream))

    def zip_with(self, *streams: Iterator[R], fill_none: bool = False):
        """
        Creates a new stream, the i-th element of which is a tuple of i-th elements of this plus all other streams.
        :param streams: Other streams to get zipped
        :param fill_none: True - stop fast when one of the stream depletes;
            False - fill in None for depleted streams until all streams deplete.
        :return: Stream with zipped tuples
        """
        if len(streams) == 0:
            return self
        return Stream(Zipper(self.__stream, *streams, stop_fast=not fill_none))

    def collapse_to_first(self, collapsible: Callable[[T, T], bool]):
        """
        Creates a stream with only the leading elements of all consecutively collapsible element chains
        :param collapsible: determine if a pair of adjacent elements is collapsible.
        :return: stream with collapsed result
        """
        return Stream(Collapser(self.__stream, collapsible, collector=Collector.of(lambda l: l[0])))

    def collapse_and_combine(self, collapsible: Callable[[T, T], bool], combiner: Callable[[T, T], T]):
        """
        Creates a stream with all consecutively collapsible element chains combined with `combiner`
        :param collapsible: determine if a pair of adjacent elements is collapsible.
        :param combiner: apply this rule to the final chain of collapsible elements
        :return: stream with collapsed result
        """
        return Stream(Collapser(self.__stream, collapsible, combiner=combiner))

    def collapse_and_collect(self, collapsible: Callable[[T, T], bool], collector: Collector):
        """
        Creates a stream with all consecutively collapsible element chains collected `collector`
        :param collapsible: determine if a pair of adjacent elements is collapsible.
        :param collector: apply this collector to the final chain of collapsible elements
        :return: stream with collapsed result
        """
        return Stream(Collapser(self.__stream, collapsible, collector=collector))

    def adjacent_groups(self, in_same_group: Callable[[T, T], bool]):
        """
        Creates a stream of list with adjacent `in_same_group` elements groupped in
        :param in_same_group: determine if an adjacent pair still in the same group.
        :return: stream with adjacent element groups
        """
        return Stream(Collapser(self.__stream, in_same_group, collector=Collector.of(list)))

    def adjacent_key_groups(self, group_key_of: Callable[[T], R]):
        """
        Creates a stream of tuples of group key / adjacent elements
        :param group_key_of: get the group key of an element
        :return: stream with group tuples
        """
        return Stream(Collapser(
            self.__stream,
            collapsible=lambda x, y: group_key_of(x) == group_key_of(y),
            collector=Collector.of(lambda l: (group_key_of(l[0]), list(l)))))

    def group_by(self, key: Callable[[T], K]):
        """
        Creates a stream of map entries, keys of which are the result by applying `key` on all elements, and values of
        which are the list of elements that result in the same key.
        :param key: key generating function
        :return: stream of map entries
        """
        return DictStream(wrap=Grouper(self.map_to_key_value(key, lambda item: item)))

    def group_to_map(
            self, key: Callable[[T], K], *, map_collector: Callable[[Iterator[Tuple]], Dict] = dict) -> Dict[K, List[T]]:
        """
        Creates a map, keys of which are the result by applying `key` on all elements, and values of which are
        the list of elements that result in the same key.
        :param key: key generating function
        :param map_collector: transforming entry stream to a dict
        :return: dict
        """
        return self.group_by(key).collect_as_map(map_collector=map_collector)

    def stream_transform(self, stream_func: Callable[[Iterator[T]], Iterator[R]]):
        """
        Run arbitrary stream transformation
        :param stream_func: (stream -> stream) stream transformation function
        :return: A Stream with processed stream
        """
        return Stream(stream_func(self.__stream))

    def wrap_as_dict_stream(self):
        """
        Try box up current stream as a dict stream. Use at your own risk if the stream content is not in a format of
        two-element tuple.
        :return: DictStream
        """
        if isinstance(self, DictStream):
            return self
        return DictStream(wrap=((elem[0], elem[1] if len(elem) == 2 else elem[1:]) for elem in self.__stream))


class DictStream(Stream[Tuple[K, V]]):

    def __init__(self, *list_of_dicts: Dict[K, V], wrap: Union[Iterator[Tuple[K, V]], None] = None):
        """
        The MapStream / DictStream class is the chainable wrapper class around any generators / iterators of dict item
        like elements
        :param list_of_dicts: a list of dict-like elements
        :param wrap: <OR> wrap a stream with DictStream class
        """
        if wrap is None:
            super(DictStream, self).__init__(*(
                ((key, dct[key]) for key in dct)
                for dct in list_of_dicts
            ))
        else:
            if len(list_of_dicts):
                raise ValueError("No other iterator source should be provided when wrapping an iterator")
            super(DictStream, self).__init__(wrap)

    def map_items(self, func: Callable[[K, V], R]):
        """
        Pass key and item respectively to the map function
        :param func: (key, value -> any) item map function
        :return: A Stream wrapping resulting stream
        """
        return self.stream_transform(lambda stream: starmap(func, stream))

    def map_key_values(self, key_map: Callable[[K], T], value_map: Callable[[V], R]):
        """
        Pass key and item respectively to the map function
        :param key_map: (key -> key_like) key map function
        :param value_map: (value -> any) value map function
        :return: A Stream wrapping resulting stream
        """
        return DictStream(wrap=((key_map(k), value_map(v)) for k, v in self))

    def map_keys(self, func: Callable[[K], R]):
        """
        Apply the map function only to the keys
        :param func: (key -> key_like) key map function
        :return: A DictStream wrapping transformed item
        """
        return DictStream(wrap=self.map_items(lambda k, v: (func(k), v)))

    def filter_keys(self, func: Callable[[K], bool]):
        """
        Pick dict elements whose keys pass the test
        :param func: (key -> boolean) function each key will be tested against
        :return: A DictStream instance wrapping the filtered stream
        """
        return DictStream(wrap=((k, v) for k, v in self if func(k)))

    def map_values(self, func: Callable[[V], R]):
        """
        Apply the map function only to the values
        :param func: (value -> any) value map function
        :return: A DictStream wrapping transformed item
        """
        return DictStream(wrap=self.map_items(lambda k, v: (k, func(v))))

    def filter_values(self, func: Callable[[V], bool]):
        """
        Pick dict elements whose values pass the test
        :param func: (key -> boolean) function each value will be tested against
        :return: A DictStream instance wrapping the filtered stream
        """
        return DictStream(wrap=((k, v) for k, v in self if func(v)))

    def add_dicts(self, *list_of_dicts: Dict[K, V]):
        """
        Append with several dicts, keys of which may override the previous keys.
        :param list_of_dicts: a list of dicts to merge in
        :return: A DictStream with all items
        """
        return DictStream(wrap=self.add(DictStream(*list_of_dicts)))

    def with_overrides(self, *list_of_dicts: Dict[K, V], **literal_overrides: V):
        """
        Append with several dicts, keys of which may override the previous keys. Literal overrides may be provided.
        :param list_of_dicts: a list of dicts to merge in
        :param literal_overrides: key / value to merge in
        :return: A DictStream with all items (overrides on collections
        """
        if len(literal_overrides.keys()):
            assert len(list_of_dicts) == 0, "Cannot merge in both dicts and literal overrides. Try split up."
            return self.add_dicts(literal_overrides)
        else:
            return self.add_dicts(*list_of_dicts)

    @staticmethod
    def merge_dicts(*dicts_to_merge, dict_collector: Callable[[Iterator[Tuple[K, V]]], Dict[K, V]] = dict):
        """
        Static method to help merge dicts
        :param dicts_to_merge: a list of dicts; following dicts override previous dicts
        :param dict_collector: default built-in dict
        :return: A merged dict
        """
        return DictStream(*dicts_to_merge).build_dict(dict_collector)
