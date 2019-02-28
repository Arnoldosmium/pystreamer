from itertools import chain, islice, dropwhile, takewhile, starmap
from functools import reduce
from .util import streamify
from .operator import Deduplicator


class Stream(object):
    """
    The Stream class is the chainable wrapper class around any generators / iterators
    Generators and iterators are merged together during creation time.
    """

    @staticmethod
    def _prepare_stream(*generators_or_iterables):
        if len(generators_or_iterables) == 1:
            return streamify(generators_or_iterables[0])
        else:
            return chain.from_iterable(map(streamify, generators_or_iterables))

    @staticmethod
    def of_list(*elements):
        return Stream(elements)

    def __init__(self, *generators_or_iterables):
        """
        Initialize Stream objects
        :param generators_or_iterables: any numbers of generators or iterables
        """
        self.__stream = Stream._prepare_stream(*generators_or_iterables)

    # Python 3 support
    def __next__(self):
        return next(self.__stream)

    # Python 2 support
    def next(self):
        return self.__next__()

    # to iterator
    def __iter__(self):
        return self.__stream

    def add(self, *generators_or_iterables):
        """
        Append iterators to current stream
        :param generators_or_iterables: any numbers of generators or iterables
        :return: New Stream instance wrapping the union stream
        """
        return Stream(self.__stream, *generators_or_iterables)

    def chain(self, *generators_or_iterables):
        """
        (alias of add) Append iterators to current stream
        :param generators_or_iterables: any numbers of generators or iterables
        :return: New Stream instance wrapping the union stream
        """
        return self.add(*generators_or_iterables)

    def concat(self, *generators_or_iterables):
        """
        (alias of add) Append iterators to current stream
        :param generators_or_iterables: any numbers of generators or iterables
        :return: New Stream instance wrapping the union stream
        """
        return self.add(*generators_or_iterables)

    def map(self, func):
        """
        Equivalent to builtin map function
        :param func: function each element will be passed to
        :return: New Stream instance wrapping the mapped stream
        """
        return Stream(map(func, self.__stream))

    def flat_map(self, func):
        """
        Flattening (once) if any element is a collection or iterator
        :param func: function each current element will be passed to
        :return: New Stream instance wrapping the flat_mapped stream
        """
        return Stream(chain.from_iterable(map(streamify, map(func, self.__stream))))

    def filter(self, func):
        """
        Pick elements that pass the test
        :param func: (element -> boolean) function each current element will be tested against
        :return: New Stream instance wrapping the filtered stream
        """
        return Stream(filter(func, self.__stream))

    def exclude(self, func):
        """
        Filter out elements that pass the test
        :param func: (element -> boolean) function each current element will be tested against
        :return: New Stream instance wrapping the filtered stream
        """
        return self.filter(lambda x: not func(x))

    def minus(self, func):
        """
        (alias of exclude) Filter out elements that pass the test
        :param func: (element -> boolean) function each current element will be tested against
        :return: New Stream instance wrapping the filtered stream
        """
        return self.exclude(func)

    def collect(self, collector):
        """
        [Consumer operation] passes the stream to collector
        :param collector: (stream -> any) function iterates through the stream
        :return: the result after piping stream to collector function
        """
        return collector(self)

    def collect_dict(self, dict_collector=dict):
        """
        [Consumer operation] passes the stream to dict collector
        :param dict_collector: (stream<I extends item> -> D extends dict) function iterates through the item stream
        :return: the result after piping stream to dict collector function
        """
        return dict_collector((each[0], each[1] if len(each) == 2 else each[1:]) for each in self.__stream)

    def build_dict(self, dict_collector=dict):
        """
        (alias of collect_dict) [Consumer operation] passes the stream to dict collector
        :param dict_collector: (stream<I extends item> -> D extends dict) function iterates through the item stream
        :return: the result after piping stream to dict collector function
        """
        return self.collect_dict(dict_collector)

    def collect_as_map(self, map_collector=dict):
        """
        (alias of collect_dict) [Consumer operation] passes the stream to dict collector
        :param map_collector: (stream<I extends item> -> D extends dict) function iterates through the item stream
        :return: the result after piping stream to dict collector function
        """
        return self.collect_dict(map_collector)

    def reduce(self, reducer, initial_value=None):
        """
        [Consumer operation] equivalent to functool reduce. Passes the stream to reducer
        :param reducer: (T extends any, element -> T) function takes each item and produce an (aggregated) value
        :param initial_value: (T) optional value, served as the starting value.
        :return: the result after reducing the stream
        """
        if initial_value is not None:
            return reduce(reducer, self.__stream, initial_value)
        else:
            return reduce(reducer, self.__stream)

    def foreach(self, func):
        """
        [Consumer operation] passes the stream to func
        :param func: (element -> void) function runs on each element
        """
        for element in self:
            func(element)

    def distinct(self):
        """
        Gives stream with distinct elements
        :return: stream with distinct elements
        """
        return Stream(Deduplicator(self.__stream))

    def max(self, key=None):
        """
        [Consumer operation] grab the max value in the stream
        :param key: (element -> C extends comparable) optional evaluator to compare elements
        :return: the max element in the stream
        """
        if key is None:
            return max(self)
        return max(self, key=key)

    def min(self, key=None):
        """
        [Consumer operation] grab the min value in the stream
        :param key: (element -> C extends comparable) optional evaluator to compare elements
        :return: the min element in the stream
        """
        if key is None:
            return min(self)
        return min(self, key=key)

    def limit(self, num):
        """
        Limit the stream to certain amount of elements
        :param num: number of elements stream limit to
        :return: the limited stream
        """
        if num > 0:
            return Stream(islice(self.__stream, num))
        return self

    def takewhile(self, func):
        """
        Limit the stream to the first element on which test fails; equivalent to itertools takewhile
        :param func: (element -> boolean) function each current element will be tested against
        :return: the limited stream
        """
        return Stream(takewhile(func, self.__stream))

    def cutoff_if(self, func):
        """
        Limit the stream to the first element on which test succeeds
        :param func: (element -> boolean) function each current element will be tested against
        :return: the limited stream
        """
        return self.takewhile(lambda x: not func(x))

    def skip(self, num):
        """
        Skip the first several elements in the stream
        :param num: number of elements stream to skip
        :return: the skipped stream
        """
        if num > 0:
            return Stream(islice(self.__stream, num, None))
        return self

    def dropwhile(self, func):
        """
        Skip the stream util the first element on which test fails; equivalent to itertools dropwhile
        :param func: (element -> boolean) function each current element will be tested against
        :return: the skipped stream
        """
        return Stream(dropwhile(func, self.__stream))

    def skip_util(self, func):
        """
        Skip the stream util the first element on which test succeeds
        :param func: (element -> boolean) function each current element will be tested against
        :return: the skipped stream
        """
        return self.dropwhile(lambda x: not func(x))

    def stream_transform(self, stream_func):
        """
        Run arbitrary stream transformation
        :param stream_func: (stream -> stream) stream transformation function
        :return: A Stream with processed stream
        """
        return Stream(stream_func(self.__stream))

    def enumerate(self):
        """
        Similar to builtin enumerate function
        :return: A Stream with original stream enumerated
        """
        return Stream(enumerate(self.__stream))


class DictStream(Stream):

    def __init__(self, *list_of_dicts, **kwargs):
        """
        The DictStream / DictStream class is the chainable wrapper class around any generators / iterators of dict item
        like elements
        :param list_of_dicts: a list of dict-like elements
        :param wrap: <OR> wrap a stream with DictStream class
        """
        wrap = kwargs.get("wrap")
        if wrap is None:
            super(DictStream, self).__init__(*(
                ((key, aDict[key]) for key in aDict)
                for aDict in list_of_dicts
            ))
        else:
            if len(list_of_dicts):
                raise ValueError("No other iterator source should be provided when wrapping an iterator")
            super(DictStream, self).__init__(wrap)

    def map_items(self, func):
        """
        Pass key and item respectively to the map function
        :param func: (key, value -> any) item map function
        :return: A Stream wrapping resulting stream
        """
        return self.stream_transform(lambda stream: starmap(func, stream))

    def map_keys(self, func):
        """
        Apply the map function only to the keys
        :param func: (key -> key_like) key map function
        :return: A DictStream wrapping transformed item
        """
        return DictStream(wrap=self.map_items(lambda k, v: (func(k), v)))

    def filter_keys(self, func):
        """
        Pick dict elements whose keys pass the test
        :param func: (key -> boolean) function each key will be tested against
        :return: A DictStream instance wrapping the filtered stream
        """
        return DictStream(wrap=(elem for elem in self if func(elem[0])))

    def map_values(self, func):
        """
        Apply the map function only to the values
        :param func: (value -> any) value map function
        :return: A DictStream wrapping transformed item
        """
        return DictStream(wrap=self.map_items(lambda k, v: (k, func(v))))

    def filter_values(self, func):
        """
        Pick dict elements whose values pass the test
        :param func: (key -> boolean) function each value will be tested against
        :return: A DictStream instance wrapping the filtered stream
        """
        return DictStream(wrap=(elem for elem in self if func(elem[1])))

    def add_dicts(self, *list_of_dicts):
        """
        Add in several dicts
        :param list_of_dicts: a list of dicts to merge in
        :return: A DictStream with all items
        """
        return DictStream(wrap=self.add(DictStream(*list_of_dicts)))

    def with_overrides(self, *list_of_dicts):
        """
        (alias of add_dicts) Add in several dicts
        :param list_of_dicts: a list of dicts to merge in
        :return: A DictStream with all items (overrides on collections
        """
        return self.add_dicts(*list_of_dicts)

    @staticmethod
    def merge_dicts(*dicts_to_merge, **kwargs):
        """
        Static method to help merge dicts
        :param dicts_to_merge: a list of dicts; following dicts override previous dicts
        :param dict_collector: default built-in dict
        :return: A merged dict
        """
        dict_collector = kwargs.get("dict_collector", dict)
        return DictStream(*dicts_to_merge).build_dict(dict_collector)
