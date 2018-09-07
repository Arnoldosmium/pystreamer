from itertools import chain
from .util import streamify


class DragonChain(object):
    def __init__(self, *stream_or_things):
        self.stream_list = map(streamify, stream_or_things)
        self._stream = chain.from_iterable(self.stream_list)

    def __next__(self):
        return next(self._stream)

    def next(self):
        return self.__next__()

    def __iter__(self):
        return self

    def map(self, func):
        return self.__class__(map(func, self))

    def filter(self, func):
        return self.__class__(filter(func, self))

    def collect(self, collector):
        return collector(self)

    def add(self, *stream_or_things):
        return self.__class__(self, *stream_or_things)

    def chain(self, *args):
        return self.add(args)

    def exclude(self, func):
        return self.filter(lambda x: not func(x))

    def minus(self, func):
        return self.exclude(func)



