import collections
import dataclasses
import enum
from typing import Any, Dict, Optional, Union


# Decorator to add unique id to each instance of a class.
def add_unique_id(cls):
    # pylint: disable=protected-access
    cls._max_id = 0

    # Store original __init__ so it can be called within our new one
    orig_init = cls.__init__

    def new_init(self, *args, **kwargs):
        orig_init(self, *args, **kwargs)
        cls._max_id += 1
        self.id = cls._max_id

    cls.__init__ = new_init

    def __reduce__(self):
        # Capture state for both copying and pickling.
        # Use a custom factory function to handle creating a new instance with an incremented ID.
        return (_custom_unpickle, (self.__class__, self.__dict__))

    def _custom_unpickle(cls, state):
        obj = cls.__new__(cls)
        cls._max_id += 1  # Increment the class ID
        state["id"] = cls._max_id  # Assign the new ID to the state
        obj.__dict__.update(state)
        return obj

    cls.__reduce__ = __reduce__

    return cls


class AcceleratorType(enum.Enum):
    # Values means compute ability.
    A100 = 10
    T4 = 1


class GeographicalRegion(enum.Enum):
    US = "us"
    ASIA = "asia"


@dataclasses.dataclass
class Resources:
    cpu: int
    acc: Dict[AcceleratorType, Union[int, float]]

    def info(self) -> Dict[str, Any]:
        return {"cpu": self.cpu, "acc": {k.name: v for k, v in self.acc.items()}}
