class Deduplicator(object):
    def __init__(self, stream):
        self.__stash = set()
        self.__stream = stream

    def __next__(self):
        for item in self.__stream:
            if item not in self.__stash:
                self.__stash.add(item)
                yield item

    def next(self):
        return self.next()

    def __iter__(self):
        return self

    def unordered_iter(self):
        return iter(self.__stash)