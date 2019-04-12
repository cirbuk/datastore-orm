"""
Tests for the NDB compat library
"""

from datastore_orm import BaseModel
from typing import List
from dataclasses import dataclass
from google.cloud import datastore


@dataclass
class B(BaseModel):
    x: int
    y: int

    @classmethod
    def _sample(cls):
        try:
            return B._class_mapping
        except:  # noqa: E722
            cls._class_mapping = B(0, 1)
            return cls._class_mapping


@dataclass
class C(BaseModel):
    p: int
    q: int

    @classmethod
    def _sample(cls):
        try:
            return C._class_mapping
        except:  # noqa: E722
            cls._class_mapping = C(0, 1)
            return cls._class_mapping


@dataclass
class TestModel(BaseModel):
    a: int
    b: B
    c: List[C]

    @classmethod
    def _sample(cls):
        try:
            return cls._class_mapping
        except:  # noqa: E722
            cls._class_mapping = TestModel(0, B._sample(), [C._sample()])
            return cls._class_mapping


def test_dotted_dict_to_object(dict_):
    """
    Trigger BaseModel._dotted_dict_to_object and test its functionality
    """
    BaseModel(None)
    _DICT = {
        "a": 1,
        "b.x": 2,
        "b.y": 3,
        "c.p": [4, 5],
        "c.q": [6, 7]
    }
    obj = test_dotted_dict_to_object(_DICT)
    print(obj)
    return TestModel._dotted_dict_to_object(dict_)


def test_put():
    BaseModel(datastore.Client())
    test_model = TestModel(0, B(1, 2), [C(3, 4), C(5, 6)])
    test_model.put()


def test_query():
    query = TestModel.query()
    query_iter = query.fetch(start_cursor=None, limit=1)
    for page in query_iter.pages:
        print(list(page))


if __name__ == '__main__':
    test_put()
    test_query()
