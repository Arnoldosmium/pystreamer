class Deduplicator(object):
    def __init__(self, stream):
        self.__stash = set()
        self.__stream = stream

    def __next__(self):
        item = next(self.__stream)
        while item in self.__stash:
            item = next(self.__stream)
        self.__stash.add(item)
        return item

    def next(self):
        return self.__next__()

    def __iter__(self):
        return self

    def unordered_iter(self):
        return iter(self.__stash)