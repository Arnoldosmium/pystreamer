# -*- coding: utf-8 -*-

"""
streamer.collector
---

This module contains the definition and some implementations of `Collector`.
Similar to `java.util.stream.Collector`s in Java, it packages a set of map-reduce operations.
"""
from abc import ABCMeta, abstractmethod
from typing import Iterable, TypeVar, Generic, Callable, Any
from collections import Counter

T = TypeVar("T")
A = TypeVar("A")  # Accumulator intermediate
R = TypeVar("R")  # Reduction result


class Collector(Generic[T, A, R], metaclass=ABCMeta):
    SIMPLE_FLAG = False

    @abstractmethod
    def supplier(self) -> A:
        """
        The function initializes the accumulator intermediate - a mutable container or partial count/sum.
        :return: A - accumulator intermediate
        """
        raise NotImplementedError("Cannot execute `supplier` in abstract class `Collector`.")

    @abstractmethod
    def accumulator(self, acc: A, elem: T) -> None:
        """
        The function appends the element to the accumulator intermediate, should yield no result.
        :param acc: A - accumulator intermediate
        :param elem: T - element
        """
        raise NotImplementedError("Cannot execute `accumulator` in abstract class `Collector`.")

    @abstractmethod
    def combiner(self, acc1: A, acc2: A) -> A:
        """
        The function should combine two partitions of accumulator intermediates into one.
        :param acc1: A - accumulator intermediate partial
        :param acc2: A - accumulator intermediate partial
        :return: merged accumulator intermediate
        """
        raise NotImplementedError("Cannot execute `combiner` in abstract class `Collector`.")

    def finisher(self, acc: A) -> R:
        """
        The function should finish off the accumulator intermediate to the final result.
        By default this is no-op.
        :param acc: A - final accumulator intermediate
        :return: final result
        """
        return acc

    def collect(self, collection: Iterable[T]) -> R:
        """
        The function applies the collective operation on any collection.
        This doesn't block a collector to be used multiple times.
        :param collection: any iterable collections of T
        :return: final result
        """
        container = self.supplier()
        for item in collection:
            self.accumulator(container, item)
        return self.finisher(container)

    # Todo: parallel collection

    @staticmethod
    def of(func: Callable[[Iterable[T]], R]):
        class _SimpleCollector(Collector[T, R, R]):
            SIMPLE_FLAG = True

            def supplier(self) -> A:
                raise ValueError("Simple collector should never use `supplier`")

            def accumulator(self, acc: A, elem: T) -> None:
                raise ValueError("Simple collector should never use `accumulator`")

            def combiner(self, acc1: A, acc2: A) -> A:
                raise ValueError("Simple collector should never use `combiner`")

            def collect(self, collection: Iterable[T]) -> R:
                return func(collection)

        return _SimpleCollector()


class OneTimeCollector(Collector[T, A, R], metaclass=ABCMeta):

    def __init__(self):
        self.__has_executed = False

    def is_used(self):
        return self.__has_executed

    def collect(self, collection: Iterable[T]) -> R:
        if self.is_used():
            raise ValueError("One time collectors cannot be reused.")
        self.__has_executed = True
        return super(OneTimeCollector, self).collect(collection)


class CountCollector(Collector[T, Any, int]):
    class _IntPointer:
        def __init__(self, init: int = 0):
            self.__v = init

        def acc(self):
            self.__v += 1

        def __add__(self, other):
            assert isinstance(other, CountCollector._IntPointer)
            return self.__class__(self.__v + other.__v)

        def get(self):
            return self.__v

    def supplier(self) -> _IntPointer:
        return CountCollector._IntPointer()

    def accumulator(self, partial: _IntPointer, _: T) -> None:
        partial.acc()

    def combiner(self, partial1: _IntPointer, partial2: _IntPointer) -> _IntPointer:
        return partial1 + partial2

    def finisher(self, final: _IntPointer) -> int:
        return final.get()
