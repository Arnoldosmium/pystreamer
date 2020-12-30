# -*- coding: utf-8 -*-

"""
streamer.streams
---

The module contains many stream generators.
"""
from typing import Iterable, TypeVar, Callable, Tuple, Collection, Union
from io import TextIOBase
import re
from .stream import Stream
from .operator import Cartesian, ConstantOf, RepeatApply, Splitter
from .util import cast_to_text_io

T = TypeVar("T")


def constant_of(value: T, times: int) -> Stream[T]:
    """
    A stream with constant value for x times
    :param value: constant value
    :param times: repeat times
    :return: constant stream
    """
    return Stream(ConstantOf(value, times))


def iterate(seed: T, operator: Callable) -> Stream:
    """
    A stream with all result of applying same function on a seed forever.
    :param seed: the initial value
    :param operator: the function to apply
    :return: iterate stream
    """
    return Stream(RepeatApply(seed, operator))


def generate(gen_func: Callable[[], T]) -> Stream[T]:
    """
    A stream with all result of rerunning same function forever.
    :param gen_func: the generator function
    :return: generate stream
    """
    return Stream(RepeatApply(None, lambda _: gen_func())).skip(1)


def cartesian_product_stream(*streams: Iterable) -> Stream[Tuple]:
    """
    A stream containing all possible combinations of elements in given streams.
    :param streams: streams to perform cartesian product
    :return: cartesian product stream
    """
    return Stream(Cartesian(*streams))


def cartesian_power(power: int, collection: Collection) -> Stream[Tuple]:
    """
    A stream containing all possibility of picking elements x times (with returning) in a given collection
    :param power: element numbers / self product x times
    :param collection: the collection
    :return: cartesion power stream
    """
    if power <= 0:
        raise ValueError("Cartesian power number should be at least 1")
    return Stream(Cartesian(*(iter(collection) for _ in range(power))))


def lines_of(content: Union[TextIOBase, str]) -> Stream[str]:
    """
    A stream containing all lines from a text or a text buffer. New line chars are preserved.
    :param content: text or text buffer
    :return: stream of lines
    """
    return Stream(cast_to_text_io(content))


def split(content: Union[TextIOBase, str], regex: str) -> Stream[str]:
    """
    A stream containing all splits of a regex on a text or text buffer
    :param content: text or text buffer
    :param regex: string, regex to split source
    :return: stream of split chunks
    """
    return Stream(Splitter(cast_to_text_io(content), regex))