from itertools import chain
from functools import reduce
from .util import streamify


class Dragon(object):
    @staticmethod
    def prepare_stream(*stream_or_things):
        if len(stream_or_things) == 1:
            return streamify(stream_or_things[0])
        else:
            return chain.from_iterable(map(streamify, stream_or_things))

    def __init__(self, *stream_or_things):
        self._stream = Dragon.prepare_stream(*stream_or_things)

    def __next__(self):
        return next(self._stream)

    def next(self):
        return self.__next__()

    def __iter__(self):
        return self

    def add(self, *stream_or_things):
        self._stream = Dragon.prepare_stream(self._stream, *stream_or_things)
        return self

    def chain(self, *args):
        return self.add(*args)

    def concat(self, *args):
        return self.add(*args)

    def map(self, func):
        self._stream = map(func, self._stream)
        return self

    def flat_map(self, func):
        self._stream = chain.from_iterable(map(streamify, map(func, self._stream)))
        return self

    def filter(self, func):
        self._stream = filter(func, self._stream)
        return self

    def exclude(self, func):
        return self.filter(lambda x: not func(x))

    def minus(self, func):
        return self.exclude(func)

    def collect(self, collector):
        return collector(self)

    def reduce(self, reducer, initial_value=None):
        if initial_value is not None:
            return reduce(reducer, self._stream, initial_value)
        else:
            return reduce(reducer, self._stream)

    def foreach(self, func):
        for element in self:
            func(element)

    def max(self, key=None):
        if key is None:
            return max(self)
        return max(self, key=key)

    def min(self, key=None):
        if key is None:
            return min(self)
        return min(self, key=key)


