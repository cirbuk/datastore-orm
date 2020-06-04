"""
Tests for the datastore-orm library
"""

from datastore_orm import BaseModel, initialize, CustomKey
from typing import List
from dataclasses import dataclass
from google.cloud import datastore
from google.oauth2 import service_account
from google.cloud.datastore import Key
from datetime import datetime
from uuid import uuid4


@dataclass
class Brand(BaseModel):
    name: str
    description: str

    @classmethod
    def _factory(cls):
        cls._class_mapping = cls("Sample Brand", "Hello from sample brand")
        return cls._class_mapping

    _exclude_from_indexes_ = ('description',)


@dataclass
class Price(BaseModel):
    value: float
    last_revised: datetime
    currency: str

    @classmethod
    def _factory(cls):
        cls._class_mapping = cls(9999.9, datetime.utcnow(), "USD")
        return cls._class_mapping


@dataclass
class Car(BaseModel):
    uid: str
    created_by: Key
    brand: Brand = None
    prices: List[Price] = None

    @classmethod
    def _factory(cls):
        cls._class_mapping = Car(str(uuid4()), CustomKey('User', 'test@test.com', project=CustomKey._client.project),
                                 Brand._factory(), [Price._factory()])
        return cls._class_mapping


def test_dotted_dict_to_object():
    """
    Trigger BaseModel._dotted_dict_to_object and test its functionality
    """
    uid = str(uuid4())
    time1 = datetime.utcnow()
    time2 = datetime.utcnow()
    dict_ = {
        "uid": uid,
        "brand.name": "Mercedes",
        "brand.description": "Generic luxury car",
        "prices.value": [9888, 6785],
        "prices.last_revised": [time1, time2],
        "prices.currency": ['USD', 'EUR'],
        "created_by": CustomKey('User', 'test@test.com')
    }
    car = Car._dotted_dict_to_object(dict_)
    assert car == Car(uid, brand=Brand(name='Mercedes', description='Generic luxury car',),
                      prices=[Price(9888, time1, 'USD'), Price(6785, time2, 'EUR')],
                      created_by=CustomKey('User', 'test@test.com'))


def test_put():
    car = Car(str(uuid4()), CustomKey("Car", "test@test.com"),
              prices=[Price(9888, datetime.utcnow(), "USD"), Price(6899, datetime.utcnow(), "GBP")])
    car_key = car.put()
    print(car_key.id)
    car_from_ds = car_key.get()


def test_query(token=None):
    query = Car.query()
    query_iter = query.fetch(start_cursor=token, limit=1)
    for page in query_iter.pages:
        print(list(page))
    return query_iter.next_page_token


if __name__ == '__main__':
    # credentials1 = service_account.Credentials.from_service_account_file('./keys/credentials.json')
    # client1 = datastore.Client(project='project-name', namespace='Beta', credentials=credentials1)
    client = datastore.Client(namespace='Beta')
    initialize(clients=[client])
    test_put()
    _token = None
    while True:
        _token = test_query(_token)
        if not _token:
            break
    # test_dotted_dict_to_object()
