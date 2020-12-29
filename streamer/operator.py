# -*- coding: utf-8 -*-

"""
streamer.operator
---

The module contains some iterator operator - add operation to an iterator and keep its laziness.
"""

from typing import Generic, TypeVar, Iterator, Iterable, Callable, Union, Any, Tuple
from collections import Counter, namedtuple, defaultdict
from functools import reduce
from abc import ABCMeta, abstractmethod
from .collector import Collector

T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')

_EmptyReference = namedtuple("_EmptyReference", "")()


class _AbstractOperator(Generic[T], metaclass=ABCMeta):

    @abstractmethod
    def __next__(self):
        raise NotImplementedError("Abstract operator cannot generate the next element")

    def __iter__(self) -> Iterator[T]:
        return self


class Deduplicator(_AbstractOperator[T]):
    """
    An iterator as stream operator for deduplication.
    It yields distinct elements that fulfill requirements lazily.
    """
    def __init__(self, stream: Iterator[T], *, more_than: int = 1, key: Union[Callable[[T], Any], None] = None):
        self.__appeared = Counter()
        self.__stream = stream
        self.__more_than = more_than if more_than >= 1 else 1
        self.__key = key

    def _apply_key(self, elem: T):
        if self.__key is None:
            return elem
        return self.__key(elem)

    def __next__(self) -> T:
        item = next(self.__stream)
        key = self._apply_key(item)
        self.__appeared[key] += 1

        # We only need to yield this element / key exactly when it appears required time.
        while self.__appeared[key] != self.__more_than:
            item = next(self.__stream)
            key = self._apply_key(item)
            self.__appeared[key] += 1

        return item

    def key_stats(self):
        return dict(self.__appeared)


class Inserter(_AbstractOperator[T]):
    """
    An iterator as stream operator for inserting delimiter.
    It yields elements lazily.
    """
    def __init__(self, stream: Iterator[T], delimiter: T):
        self.__stream = stream
        self.__delim = delimiter
        self.__turn = 0
        self.__elem = _EmptyReference

    def __next__(self) -> T:
        if self.__elem == _EmptyReference:
            self.__elem = next(self.__stream)

        self.__turn += 1
        if self.__turn & 1:
            nxt, self.__elem = self.__elem, _EmptyReference
            return nxt
        else:
            return self.__delim


class PairUp(_AbstractOperator[T]):
    """
    An iterator as stream operator for generating adjacent pairs.
    It yields elements lazily.
    """
    def __init__(self, stream: Iterator[T]):
        self.__stream = stream
        self.__turn = 0
        self.__prev = _EmptyReference

    def __next__(self) -> Tuple[T, T]:
        if self.__prev == _EmptyReference:    # initial condition
            self.__prev = next(self.__stream)
        curr = next(self.__stream)
        pair = (self.__prev, curr)
        self.__prev = curr
        return pair


def Zipper(*iterable: Iterable, fill_none: bool = False):
    """
    A generator that zip multiple iterators together.
    """
    all_streams = [iter(it) for it in iterable]

    all_stops = False
    while not all_stops:
        nxt = [None for _ in iterable]
        all_stops = True
        for i, it in enumerate(all_streams):
            try:
                nxt[i] = next(it)
                all_stops = False
            except StopIteration:
                if not fill_none:
                    return

        if not all_stops:
            yield tuple(nxt)


class Collapser(_AbstractOperator[T]):
    """
    An iterator as stream operator for collapsing adjacent elements
    """
    def __init__(self,
                 stream: Iterator[T],
                 collapsible: Callable[[T, T], bool],
                 *,
                 combiner: Union[None, Callable[[T, T], T]] = None,
                 collector: Union[None, Collector] = None):
        assert combiner is not None or collector is not None, "At least a combiner or collector is provided."
        self.__stream = stream
        self.__collapsible = collapsible
        self.__combiner = combiner
        self.__collector = collector
        self.__prev = _EmptyReference

    def _process_collection(self, collection: Iterable[T]):
        if self.__combiner is not None:
            return reduce(self.__combiner, collection)
        elif self.__collector is not None:
            return self.__collector.collect(collection)
        raise ValueError("At least a combiner or collector is specified.")

    def __next__(self):
        if self.__prev == _EmptyReference:
            self.__prev = next(self.__stream)

        elements = [self.__prev]
        while True:
            curr = _EmptyReference
            try:
                curr = next(self.__stream)
                if not self.__collapsible(self.__prev, curr):
                    break
                elements.append(curr)
            except StopIteration:
                break
            finally:
                self.__prev = curr

        return self._process_collection(elements)


def Grouper(stream: Iterator[Tuple[K, V]]):
    """
    An iterator as stream operator for grouping-by elements to a list according to its key
    """
    # Thanks to python generator, the expensive overhead / stream consumption will be deferred until request of first
    # element.
    collect_by_key = defaultdict(list)
    for key, value in stream:
        collect_by_key[key].append(value)

    for key_value_tuple in collect_by_key.items():
        yield key_value_tuple


def RepeatApply(init, transform: Callable):
    p = init
    while True:
        yield p
        p = transform(p)


def ContantOf(constant, repeat: int):
    assert repeat > 0, "At least repeat 1 time."
    for i in range(repeat):
        yield constant
