from google.cloud import datastore
from google.cloud.datastore import Query
from google.cloud.datastore.query import Iterator
from google.cloud.datastore import helpers


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
        entity_meanings = {}
        exclude_from_indexes = []

        for prop_name, value_pb in helpers._property_tuples(pb):
            value = helpers._get_value_from_value_pb(value_pb)
            entity_props[prop_name] = value

            # Check if the property has an associated meaning.
            is_list = isinstance(value, list)
            meaning = helpers._get_meaning(value_pb, is_list=is_list)
            if meaning is not None:
                entity_meanings[prop_name] = (meaning, value)

            # Check if ``value_pb`` was excluded from index. Lists need to be
            # special-cased and we require all ``exclude_from_indexes`` values
            # in a list agree.
            if is_list and len(value) > 0:
                exclude_values = set(
                    value_pb.exclude_from_indexes
                    for value_pb in value_pb.array_value.values
                )
                if len(exclude_values) != 1:
                    raise ValueError(
                        "For an array_value, subvalues must either "
                        "all be indexed or all excluded from "
                        "indexes."
                    )

                if exclude_values.pop():
                    exclude_from_indexes.append(prop_name)
            else:
                if value_pb.exclude_from_indexes:
                    exclude_from_indexes.append(prop_name)

        obj = self.model_type._dotted_dict_to_object(entity_props)
        obj._key_ = key
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
          >>> client = datastore.Client()
          >>> query = client.query(kind='Person')
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
        :param client: (Optional) client used to connect to datastore.
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


class BaseModel:
    """Typically, users will iteract with this library by creating sub-classes of BaseModel.

    BaseModel implements various helper methods (such as put, fetch etc.) to allow the user to
    interact with datastore directly from the subclass object.
    """

    client: datastore.Client

    @classmethod
    def __init__(cls, client=None):
        cls.client = client

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
            dotted_dict[base_name + '.' + k] = v
        return dotted_dict

    @classmethod
    def _dotted_dict_to_object(cls, dict_: dict):
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
                    if len(class_dict[class_]) < i+1:
                        class_dict[class_].append(dict())
                    class_dict[class_][i][prop_key] = each_val
            else:
                class_dict[class_] = class_dict.get(class_) or dict()
                class_dict[class_][prop_key] = val

        for class_, nested_prop in class_dict.items():
            if isinstance(nested_prop, list):
                nested_prop_list = []
                for each_nested_prop in nested_prop:
                    sample = cls._sample()
                    nested_prop_list.append(type(getattr(sample, class_)[0])(**each_nested_prop))
                dict_[class_] = nested_prop_list
            else:
                sample = cls._sample()
                dict_[class_] = type(getattr(sample, class_))(**nested_prop)
        return cls(**dict_)

    def _to_entity(self):
        """Converts a BaseModel subclass object into datastore entity. This method is called just before
        datastore's client.put is called.
        """
        obj_dict = vars(self)
        exclude_from_indexes = ()
        try:
            exclude_from_indexes = self._exclude_from_indexes_
        except AttributeError:
            pass

        key = self.client.key(self.__class__.__name__)
        try:
            key = self._key_
        except AttributeError:
            pass

        entity = datastore.Entity(key=key, exclude_from_indexes=exclude_from_indexes)
        for dict_key, dict_val in obj_dict.copy().items():
            if isinstance(dict_val, BaseModel):
                del obj_dict[dict_key]
                obj_dict.update(dict_val.dottify(dict_key))
            if isinstance(dict_val, list):
                if isinstance(dict_val[0], BaseModel):
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
        entity.update(obj_dict)
        return entity

    def put(self):
        """
        Put the object into datastore.
        """
        self.client.put(self._to_entity())

    @classmethod
    def query(cls, **kwargs) -> CustomQuery:
        kwargs["project"] = cls.client.project
        if "namespace" not in kwargs:
            kwargs["namespace"] = cls.client.namespace
        return CustomQuery(cls, client=cls.client, kind=cls.__name__, **kwargs)
