from itertools import chain, islice, dropwhile, takewhile, starmap
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
        self.__stream = Dragon.prepare_stream(*stream_or_things)

    def __next__(self):
        return next(self.__stream)

    def next(self):
        return self.__next__()

    def __iter__(self):
        return self

    def add(self, *stream_or_things):
        self.__stream = Dragon.prepare_stream(self.__stream, *stream_or_things)
        return self

    def chain(self, *args):
        return self.add(*args)

    def concat(self, *args):
        return self.add(*args)

    def map(self, func):
        self.__stream = map(func, self.__stream)
        return self

    def flat_map(self, func):
        self.__stream = chain.from_iterable(map(streamify, map(func, self.__stream)))
        return self

    def filter(self, func):
        self.__stream = filter(func, self.__stream)
        return self

    def exclude(self, func):
        return self.filter(lambda x: not func(x))

    def minus(self, func):
        return self.exclude(func)

    def collect(self, collector):
        return collector(self)

    def reduce(self, reducer, initial_value=None):
        if initial_value is not None:
            return reduce(reducer, self.__stream, initial_value)
        else:
            return reduce(reducer, self.__stream)

    def foreach(self, func):
        for element in self:
            func(element)

    def distinct(self):
        _stream = self.__stream

        def _distinct_stream():
            _stash = set()
            for item in _stream:
                if item not in _stash:
                    _stash.add(item)
                    yield item

        self.__stream = _distinct_stream()
        return self


    def max(self, key=None):
        if key is None:
            return max(self)
        return max(self, key=key)

    def min(self, key=None):
        if key is None:
            return min(self)
        return min(self, key=key)

    def limit(self, num):
        if num > 0:
            self.__stream = islice(self.__stream, num)
        return self

    def takewhile(self, func):
        self.__stream = takewhile(func, self.__stream)
        return self

    def cutoff_if(self, func):
        return self.takewhile(lambda x: not func(x))

    def skip(self, num):
        if num > 0:
            self.__stream = islice(self.__stream, num, None)
        return self

    def dropwhile(self, func):
        self.__stream = dropwhile(func, self.__stream)
        return self

    def skip_util(self, func):
        return self.dropwhile(lambda x: not func(x))

    def stream_transform(self, stream_func):
        self.__stream = stream_func(self.__stream)
        return self


class DictDragon(Dragon):

    def __init__(self, *list_of_dicts):
        super(DictDragon, self).__init__(*map(dict.items, list_of_dicts))

    def map_items(self, func):
        return self.stream_transform(lambda stream: starmap(func, stream))

    def map_keys(self, func):
        return self.map_items(lambda k, v: (func(k), v))

    def map_values(self, func):
        return self.map_items(lambda k, v: (k, func(v)))

    def merge_dicts(self, *list_of_dicts):
        return self.add(*map(dict.items, list_of_dicts))

    def with_overrides(self, *list_of_dicts):
        return self.merge_dicts(*list_of_dicts)

    def collect_dict(self):
        return self.collect(dict)

    def build_dict(self):
        return self.collect_dict()
