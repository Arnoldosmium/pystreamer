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
        return DragonChain(map(func, self))

    def filter(self, func):
        return DragonChain(filter(func, self))

    def collect(self, collector):
        return collector(self)

