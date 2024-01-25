from enum import Enum
from typing import Any, Dict
from functools import total_ordering


class NestedDictAccessor:
    """
    A utility class for accessing values in a nested dictionary using a dot-separated key.

    This class provides a way to access nested dictionary values using a single string key
    with elements separated by dots, representing the nesting levels in the dictionary.
    e.g. dict['a.b.c'] == dict['a']['b']['c']
    """
    def __init__(self, data: Dict[str, Any]):
        self.data = data

    def __getitem__(self, key: str) -> Any:
        if not isinstance(key, str):
            raise TypeError("Key must be a string")

        keys = key.split('.')
        value = self.data
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                raise KeyError(f"Key '{key}' not found")
        return value


@total_ordering
class OrderedEnum(Enum):
    """
    Enum class where members are ordered by the order of their definition and can have custom values.

    This class stores a tuple of (order, custom_value) for each member. The order is determined
    by the definition sequence, and the custom_value is the assigned value of the member.
    """

    def __init__(self, custom_value):
        self._order = len(self.__class__.__members__)
        self._custom_value = custom_value

    @property
    def value(self):
        """
        Override the value property to return the custom value instead of the (order, custom_value) tuple.
        """
        return self._custom_value

    def __eq__(self, other):
        if self.__class__ is other.__class__:
            return self._order == other._order
        return NotImplemented

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self._order < other._order
        return NotImplemented

    def __hash__(self):
        return hash(self._custom_value)
