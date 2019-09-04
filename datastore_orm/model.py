from google.cloud import datastore
from google.cloud.datastore import Query
from google.cloud.datastore.query import Iterator
from google.cloud.datastore import helpers
from google.cloud.datastore import Key
from typing import get_type_hints
import copy
import abc


class CustomIterator(Iterator):
    """CustomIterator overrides the default Iterator and defines a custom _item_to_object method
    in order to return BaseModel subclass objects instead of the default datastore entity
    """
    def __init__(
        self,
        model_type,
        query,
        client,
        limit=None,
        offset=None,
        start_cursor=None,
        end_cursor=None,
        eventual=False,
    ):
        super(Iterator, self).__init__(
            client=client,
            item_to_value=self._item_to_object,
            page_token=start_cursor,
            max_results=limit,
        )

        self.model_type: BaseModel = model_type
        self._query = query
        self._offset = offset
        self._end_cursor = end_cursor
        self._eventual = eventual
        # The attributes below will change over the life of the iterator.
        self._more_results = True
        self._skipped_results = 0

    def object_from_protobuf(self, pb):
        """Factory method for creating a python object based on a protobuf.

        The protobuf should be one returned from the Cloud Datastore
        Protobuf API.

        :type pb: :class:`.entity_pb2.Entity`
        :param pb: The Protobuf representing the entity.

        :rtype: :class:`google.cloud.datastore.entity.Entity`
        :returns: The entity derived from the protobuf.
        """
        key = None
        if pb.HasField("key"):  # Message field (Key)
            key = helpers.key_from_protobuf(pb.key)

        entity_props = {}

        for prop_name, value_pb in helpers._property_tuples(pb):
            value = helpers._get_value_from_value_pb(value_pb)
            entity_props[prop_name] = value

        obj = self.model_type._dotted_dict_to_object(entity_props, key)
        return obj

    def _item_to_object(self, iterator, entity_pb):
        """Convert a raw protobuf entity to the native object.

        :type iterator: :class:`~google.api_core.page_iterator.Iterator`
        :param iterator: The iterator that is currently in use.

        :type entity_pb:
            :class:`.entity_pb2.Entity`
        :param entity_pb: An entity protobuf to convert to a native entity.

        :rtype: :class:`~google.cloud.datastore.entity.Entity`
        :returns: The next entity in the page.
        """
        return self.object_from_protobuf(entity_pb)

    def key_from_protobuf(self, pb):
        """Factory method for creating a key based on a protobuf.

        The protobuf should be one returned from the Cloud Datastore
        Protobuf API.

        :type pb: :class:`.entity_pb2.Key`
        :param pb: The Protobuf representing the key.

        :rtype: :class:`google.cloud.datastore.key.Key`
        :returns: a new `Key` instance
        """
        path_args = []
        for element in pb.path:
            path_args.append(element.kind)
            if element.id:  # Simple field (int64)
                path_args.append(element.id)
            # This is safe: we expect proto objects returned will only have
            # one of `name` or `id` set.
            if element.name:  # Simple field (string)
                path_args.append(element.name)

        return CustomKey(*path_args)


class CustomKey(Key):

    _client: datastore.Client
    _type: object

    def __init__(self, *path_args, **kwargs):
        if not getattr(self, '_client', None):
            raise ValueError("Datastore _client is not set. Have you called datastore_orm.initialize()?")
        kwargs['namespace'] = self._client.namespace
        kwargs['project'] = self._client.project
        super(CustomKey, self).__init__(*path_args, **kwargs)

    def get(self):
        entity = self._client.get(self)
        obj = self._type._dotted_dict_to_object(dict(entity.items()))
        obj.key = entity.key
        return obj


class CustomQuery(Query):
    """CustomQuery class overrides the google.cloud.datastore.Query class in order to use a custom
    iterator class in the fetch method.
    """

    model_type: object

    def __init__(self, model_type, **kwargs):
        super(CustomQuery, self).__init__(**kwargs)
        self.model_type = model_type

    def fetch(
        self,
        limit=None,
        offset=0,
        start_cursor=None,
        end_cursor=None,
        client=None,
        eventual=False,
    ):
        """Execute the Query; return an iterator for the matching entities.

        For example::

          >>> from google.cloud import datastore
          >>> _client = datastore.Client()
          >>> query = _client.query(kind='Person')
          >>> query.add_filter('name', '=', 'Sally')
          >>> list(query.fetch())
          [<Entity object>, <Entity object>, ...]
          >>> list(query.fetch(1))
          [<Entity object>]

        :type limit: int
        :param limit: (Optional) limit passed through to the iterator.

        :type offset: int
        :param offset: (Optional) offset passed through to the iterator.

        :type start_cursor: bytes
        :param start_cursor: (Optional) cursor passed through to the iterator.

        :type end_cursor: bytes
        :param end_cursor: (Optional) cursor passed through to the iterator.

        :type client: :class:`google.cloud.datastore.client.Client`
        :param client: (Optional) _client used to connect to datastore.
                       If not supplied, uses the query's value.

        :type eventual: bool
        :param eventual: (Optional) Defaults to strongly consistent (False).
                                    Setting True will use eventual consistency,
                                    but cannot be used inside a transaction or
                                    will raise ValueError.

        :rtype: :class:`Iterator`
        :returns: The iterator for the query.
        """

        if client is None:
            client = self._client

        return CustomIterator(
            self.model_type,
            self,
            client,
            limit=limit,
            offset=offset,
            start_cursor=start_cursor,
            end_cursor=end_cursor,
            eventual=eventual
        )


class BaseModel(metaclass=abc.ABCMeta):
    """Typically, users will iteract with this library by creating sub-classes of BaseModel.

    BaseModel implements various helper methods (such as put, fetch etc.) to allow the user to
    interact with datastore directly from the subclass object.
    """

    _client: datastore.Client
    _exclude_from_indexes_: tuple

    @classmethod
    def __init__(cls, client=None):
        cls._exclude_from_indexes_ = tuple()
        cls._client = client

    def dottify(self, base_name):
        """Convert a standard BaseModel object with nested objects into dot notation to maintain
        compatibility with ndb created objects

        Example input -

        >>> class A(BaseModel):
        >>>    x = 1

        >>> class B(BaseModel):
        >>>    z = 2
        >>>    y = A()

        >>> b = B()
        >>> b.dottify()
        {z: 2, y.x: 1}

        """
        obj_dict = vars(self)
        dotted_dict = {}
        for k, v in obj_dict.items():
            if v is not None:
                dotted_dict[base_name + '.' + k] = v
        return dotted_dict

    @classmethod
    def _dotted_dict_to_object(cls, dict_: dict, key: Key = None):
        """Convert a dictionary that was created with dottify() back into a standard BaseModel object

        >>> dict_ = {
        >>>     "a": 1,
        >>>     "b.x": 2,
        >>>     "b.y": 3,
        >>>     "c.p": [4, 5]
        >>>     "c.q": [6, 7]
        >>> }

        >>> cls._dotted_dict_to_object(dict_)
        Model(a=1, b=B(x=2, y=3), c=[C(p=4, q=6), C(p=5, q=7)])

        """

        dotted_pairs = {}
        for k, val in dict_.copy().items():
            if '.' in k:
                dotted_pairs[k] = val
                del dict_[k]

        class_dict = {}
        for k, val in dotted_pairs.items():
            class_, prop_key = k.split('.', 1)
            if isinstance(val, list):
                class_dict[class_] = class_dict.get(class_) or list()
                for i, each_val in enumerate(val):
                    if len(class_dict[class_]) < i + 1:
                        class_dict[class_].append(dict())
                    class_dict[class_][i][prop_key] = each_val
            else:
                class_dict[class_] = class_dict.get(class_) or dict()
                class_dict[class_][prop_key] = val

        type_hints = get_type_hints(cls)
        for class_, nested_prop in class_dict.items():
            if isinstance(nested_prop, list):
                nested_prop_list = []
                for each_nested_prop in nested_prop:
                    nested_prop_list.append(type_hints[class_].__args__[0](**each_nested_prop))
                dict_[class_] = nested_prop_list
            else:
                dict_[class_] = type_hints[class_](**nested_prop)

        filtered_dict = {k: v for k, v in dict_.items() if k in type_hints}
        obj = cls(**filtered_dict)
        if key:
            obj.key = key
        return obj

    @classmethod
    def from_entity(cls, entity):
        return cls._dotted_dict_to_object(dict(entity.items()), entity.key)

    def _to_entity(self):
        """Converts a BaseModel subclass object into datastore entity. This method is called just before
        datastore's _client.put is called.
        """
        obj_dict = copy.deepcopy(vars(self))
        exclude_from_indexes = ()
        try:
            exclude_from_indexes = self._exclude_from_indexes_
        except AttributeError:
            pass

        try:
            key = self.key
        except AttributeError:
            key = CustomKey(self.__class__.__name__)

        entity = datastore.Entity(key=key, exclude_from_indexes=exclude_from_indexes)
        for dict_key, dict_val in obj_dict.copy().items():
            if dict_val is not None:
                if isinstance(dict_val, BaseModel):
                    # If the value is an instance of BaseModel, convert the instance
                    # into a "dotted" dictionary compatible with NDB entities.
                    del obj_dict[dict_key]
                    obj_dict.update(dict_val.dottify(dict_key))
                if isinstance(dict_val, list) and len(dict_val) > 0 and isinstance(dict_val[0], BaseModel):
                    # if the value is a list of BaseModel objects
                    dotted_dict_list = []
                    dotted_dict = dict()
                    for i, val in enumerate(dict_val):
                        dotted_dict_list.append(val.dottify(dict_key))
                    for dict_ in dotted_dict_list:
                        for k, v in dict_.items():
                            temp_val = dotted_dict.get(k) or []
                            temp_val.append(v)
                            dotted_dict[k] = temp_val
                    del obj_dict[dict_key]
                    obj_dict.update(dotted_dict)
            else:
                # if the value is False-y i.e. the key has not been set in the object,
                # delete the key from the object
                del obj_dict[dict_key]
        entity.update(obj_dict)
        return entity

    def put(self):
        """
        Put the object into datastore.
        """

        # TODO (Chaitanya): Directly convert object to protobuf and call PUT instead of converting to entity first.
        entity = self._to_entity()
        self._client.put(entity)
        entity.key._type = self.__class__
        self.key = entity.key
        return entity.key

    def delete(self):
        """Delete object from datastore.
        """
        self._client.delete(self.key)

    def to_dict(self, exclude: set = None):
        exclude = (exclude or set()) | {'key'}
        dict_ = {}
        for k, v in vars(self).items():
            if k not in exclude:
                if isinstance(v, list) and len(v) > 0 and isinstance(v[0], BaseModel):
                    temp_val = []
                    for obj in v:
                        temp_val.append(obj.to_dict())
                    dict_[k] = temp_val
                elif isinstance(v, BaseModel):
                    dict_[k] = v.to_dict()
                else:
                    dict_[k] = v
        return dict_

    @classmethod
    def query(cls, **kwargs) -> CustomQuery:
        kwargs["project"] = cls._client.project
        if "namespace" not in kwargs:
            kwargs["namespace"] = cls._client.namespace
        return CustomQuery(cls, client=cls._client, kind=cls.__name__, **kwargs)


def initialize(client):
    BaseModel._client = client
    CustomKey._client = client
