from collections.abc import MutableSet

from utils.tools import no_space_and_case_insensitive_str


class CISpacelessSet(MutableSet):
    def __init__(self, iterable=None):
        self._storage = set()
        if iterable:
            for item in iterable:
                self.add(item)

    def _normalize(self, value):
        if not isinstance(value, str):
            raise TypeError("Only strings are allowed")
        return no_space_and_case_insensitive_str(value)

    def add(self, value):
        normalized = self._normalize(value)
        self._storage.add(normalized)

    def discard(self, value):
        normalized = self._normalize(value)
        self._storage.discard(normalized)

    def clone(self):
        new_set = CISpacelessSet()
        for item in self._storage:
            new_set.add(item)
        return new_set

    def __contains__(self, value):
        if not isinstance(value, str):
            return False
        normalized = self._normalize(value)
        return normalized in self._storage

    def __iter__(self):
        return iter(self._storage)

    def __len__(self):
        return len(self._storage)

    def __repr__(self):
        return f"{self.__class__.__name__}({list(self._storage)})"
