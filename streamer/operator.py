# -*- coding: utf-8 -*-

"""
streamer.operator
---

The module contains some iterator operator - add operation to an iterator and keep its laziness.
"""

from typing import Generic, TypeVar, Iterator, Callable, Union, Any
from collections import Counter

T = TypeVar('T')


class Deduplicator(Generic[T]):
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

    def __iter__(self) -> Iterator[T]:
        return self

    def key_stats(self):
        return dict(self.__appeared)


class Inserter(Generic[T]):
    """
    An iterator as stream operator for inserting delimiter.
    It yields elements lazily.
    """

    def __init__(self, stream: Iterator[T], delimiter: T):
        self.__stream = stream
        self.__delim = delimiter
        self.__turn = 0
        self.__elem = None

    def __next__(self) -> T:
        if self.__elem is None:
            self.__elem = next(self.__stream)

        self.__turn += 1
        if self.__turn & 1:
            nxt, self.__elem = self.__elem, None
            return nxt
        else:
            return self.__delim

    def __iter__(self) -> Iterator[T]:
        return self


def RepeatApply(init, transform: Callable):
    p = init
    while True:
        yield p
        p = transform(p)


def ContantOf(constant, repeat: int):
    assert repeat > 0, "At least repeat 1 time."
    for i in range(repeat):
        yield constant
