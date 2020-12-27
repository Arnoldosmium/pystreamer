# -*- coding: utf-8 -*-

"""
streamer.util
---

The module contains some common tools used by the package.
This should be treated as an internal module and it's subjected to breaking changes.
"""

from typing import Iterable, Iterator, Union, TypeVar

T = TypeVar('T')


def to_iterator(stream_or_object: Union[Iterable[T], Iterator[T], T]) -> Iterator[T]:
    # iterators
    if hasattr(stream_or_object, "__iter__") and hasattr(stream_or_object, "__next__"):
        return stream_or_object

    # iterables, including string
    elif hasattr(stream_or_object, "__iter__") or hasattr(stream_or_object, "__getitem__"):
        return iter(stream_or_object)

    else:
        return iter((stream_or_object, ))
