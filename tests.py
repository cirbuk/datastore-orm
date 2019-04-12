"""
Tests for the NDB compat library
"""

from datastore_orm import BaseModel
from typing import List
from dataclasses import dataclass
from google.cloud import datastore
from datetime import datetime
from uuid import uuid4


@dataclass
class Brand(BaseModel):
    name: str
    description: str

    @classmethod
    def _sample(cls):
        try:
            return cls._class_mapping
        except:  # noqa: E722
            cls._class_mapping = cls("Sample Brand", "Hello from sample brand")
            return cls._class_mapping

    _exclude_from_indexes_ = ('description',)


@dataclass
class Price(BaseModel):
    value: float
    last_revised: datetime
    currency: str

    @classmethod
    def _sample(cls):
        try:
            return cls._class_mapping
        except:  # noqa: E722
            cls._class_mapping = cls(9999.9, datetime.utcnow(), "USD")
            return cls._class_mapping


@dataclass
class Car(BaseModel):
    uid: str
    brand: Brand
    prices: List[Price]

    @classmethod
    def _sample(cls):
        try:
            return cls._class_mapping
        except:  # noqa: E722
            cls._class_mapping = Car(uuid4(), Brand._sample(), [Price._sample()])
            return cls._class_mapping


def test_dotted_dict_to_object():
    """
    Trigger BaseModel._dotted_dict_to_object and test its functionality
    """
    BaseModel(None)
    dict_ = {
        "uid": str(uuid4()),
        "brand.name": "Mercedes",
        "brand.description": "Generic luxury car",
        "prices.value": [9888, 6785],
        "prices.last_revised": [datetime.utcnow(), datetime.utcnow()],
        "prices.currency": ["USD", "EUR"]
    }
    return Car._dotted_dict_to_object(dict_)


def test_put():
    BaseModel(datastore.Client(namespace="Beta"))
    car = Car(str(uuid4()), Brand("Mercedes", "Generic Brand"),
              [Price(9888, datetime.utcnow(), "USD"), Price(6899, datetime.utcnow(), "GBP")])
    car.put()


def test_query():
    query = Car.query()
    query_iter = query.fetch(start_cursor=None, limit=1)
    for page in query_iter.pages:
        print(list(page))


if __name__ == '__main__':
    test_put()
    test_query()
