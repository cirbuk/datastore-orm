from google.cloud import datastore
from google.cloud.datastore import Query, Client
from google.cloud.datastore.query import Iterator
from google.cloud.datastore import helpers
from google.cloud.datastore import Key
import datetime
from time import sleep
from typing import get_type_hints, List, Union
import copy
import abc
from redis import StrictRedis
import pickle
import asyncio
import logging
from functools import partial
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Thread

_MAX_LOOPS = 128


def get_custom_key_from_key(key):
    """
    This method is to be called where ndb.Key().get() was being used.
    This is because some properties of models are being fetched as Key instead of CustomKey
    Once a Key is converted to CustomKey by this method, .get() function can be used similar
    to ndb.Key()
    :param key: datastore.Key
    :return: CustomKey
    >>> from google.cloud.datastore import Key
    >>> from datastore_orm import get_custom_key_from_key
    >>> key = Key('Kind','id_or_name')
    >>> key_custom = get_custom_key_from_key(key)
    >>> base_model_obj = key_custom.get()
    """
    key_custom = CustomIterator.key_from_protobuf(key.to_protobuf())
    key_custom._type = SubclassMap.get()[key_custom.kind]
    return key_custom


def _extended_lookup(datastore_api, project, key_pbs,
                     missing=None, deferred=None,
                     eventual=False, transaction_id=None):
    """Repeat lookup until all keys found (unless stop requested).

    Helper function for :meth:`Client.get_multi`.

    :type datastore_api:
        :class:`google.cloud.datastore._http.HTTPDatastoreAPI`
        or :class:`google.cloud.datastore_v1.gapic.DatastoreClient`
    :param datastore_api: The datastore API object used to connect
                          to datastore.

    :type project: str
    :param project: The project to make the request for.

    :type key_pbs: list of :class:`.entity_pb2.Key`
    :param key_pbs: The keys to retrieve from the datastore.

    :type missing: list
    :param missing: (Optional) If a list is passed, the key-only entity
                    protobufs returned by the backend as "missing" will be
                    copied into it.

    :type deferred: list
    :param deferred: (Optional) If a list is passed, the key protobufs returned
                     by the backend as "deferred" will be copied into it.

    :type eventual: bool
    :param eventual: If False (the default), request ``STRONG`` read
                     consistency.  If True, request ``EVENTUAL`` read
                     consistency.

    :type transaction_id: str
    :param transaction_id: If passed, make the request in the scope of
                           the given transaction.  Incompatible with
                           ``eventual==True``.

    :rtype: list of :class:`.entity_pb2.Entity`
    :returns: The requested entities.
    :raises: :class:`ValueError` if missing / deferred are not null or
             empty list.
    """
    if missing is not None and missing != []:
        raise ValueError('missing must be None or an empty list')

    if deferred is not None and deferred != []:
        raise ValueError('deferred must be None or an empty list')

    results = []

    loop_num = 0
    read_options = helpers.get_read_options(eventual, transaction_id)
    while loop_num < _MAX_LOOPS:  # loop against possible deferred.
        loop_num += 1
        lookup_response = datastore_api.lookup(
            project,
            key_pbs,
            read_options=read_options,
        )

        # Accumulate the new results.
        results.extend(result.entity for result in lookup_response.found)

        if missing is not None:
            missing.extend(result.entity for result in lookup_response.missing)

        if deferred is not None:
            deferred.extend(lookup_response.deferred)
            break

        if len(lookup_response.deferred) == 0:
            break

        # We have deferred keys, and the user didn't ask to know about
        # them, so retry (but only with the deferred ones).
        key_pbs = lookup_response.deferred

    return results


class UTC(datetime.tzinfo):
    """Basic UTC implementation.

    Implementing a small surface area to avoid depending on ``pytz``.
    """

    _dst = datetime.timedelta(0)
    _tzname = 'UTC'
    _utcoffset = _dst

    def dst(self, dt):  # pylint: disable=unused-argument
        """Daylight savings time offset."""
        return self._dst

    def fromutc(self, dt):
        """Convert a timestamp from (naive) UTC to this timezone."""
        if dt.tzinfo is None:
            return dt.replace(tzinfo=self)
        return super(UTC, self).fromutc(dt)

    def tzname(self, dt):  # pylint: disable=unused-argument
        """Get the name of this timezone."""
        return self._tzname

    def utcoffset(self, dt):  # pylint: disable=unused-argument
        """UTC offset of this timezone."""
        return self._utcoffset

    def __repr__(self):
        return '<%s>' % (self._tzname,)

    def __str__(self):
        return self._tzname


def pb_timestamp_to_datetime(timestamp_pb):
    """Convert a Timestamp protobuf to a datetime object.

    :type timestamp_pb: :class:`google.protobuf.timestamp_pb2.Timestamp`
    :param timestamp_pb: A Google returned timestamp protobuf.

    :rtype: :class:`datetime.datetime`
    :returns: A UTC datetime object converted from a protobuf timestamp.
    """
    return (
            _EPOCH +
            datetime.timedelta(
                seconds=timestamp_pb.seconds,
                microseconds=(timestamp_pb.nanos / 1000.0),
            )
    )


_EPOCH = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=UTC())


class SubclassMap:
    _subclass_map: dict

    @staticmethod
    def get():
        try:
            return SubclassMap._subclass_map
        except AttributeError:
            subclasses = BaseModel.__subclasses__()
            SubclassMap._subclass_map = {subclass.__name__: subclass for subclass in subclasses}
            return SubclassMap._subclass_map


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

    @staticmethod
    def object_from_protobuf(pb, model_type=None):
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
            key = CustomIterator.key_from_protobuf(pb.key)
            key._type = SubclassMap.get()[key.kind]

        entity_props = {}

        for prop_name, value_pb in helpers._property_tuples(pb):
            value = CustomIterator._get_value_from_value_pb(value_pb)
            entity_props[prop_name] = value

        obj = model_type._dotted_dict_to_object(entity_props, key)
        return obj

    @staticmethod
    def _get_value_from_value_pb(value_pb):
        """Given a protobuf for a Value, get the correct value.

        The Cloud Datastore Protobuf API returns a Property Protobuf which
        has one value set and the rest blank.  This function retrieves the
        the one value provided.

        Some work is done to coerce the return value into a more useful type
        (particularly in the case of a timestamp value, or a key value).

        :type value_pb: :class:`.entity_pb2.Value`
        :param value_pb: The Value Protobuf.

        :rtype: object
        :returns: The value provided by the Protobuf.
        :raises: :class:`ValueError <exceptions.ValueError>` if no value type
                 has been set.
        """
        value_type = value_pb.WhichOneof('value_type')

        if value_type == 'timestamp_value':
            result = pb_timestamp_to_datetime(value_pb.timestamp_value)

        elif value_type == 'key_value':
            result = CustomIterator.key_from_protobuf(value_pb.key_value)
            result._type = SubclassMap.get()[result.kind]

        elif value_type == 'boolean_value':
            result = value_pb.boolean_value

        elif value_type == 'double_value':
            result = value_pb.double_value

        elif value_type == 'integer_value':
            result = value_pb.integer_value

        elif value_type == 'string_value':
            result = value_pb.string_value

        elif value_type == 'blob_value':
            result = value_pb.blob_value

        elif value_type == 'entity_value':
            result = helpers.entity_from_protobuf(value_pb.entity_value)

        elif value_type == 'array_value':
            result = [CustomIterator._get_value_from_value_pb(value)
                      for value in value_pb.array_value.values]

        elif value_type == 'geo_point_value':
            result = helpers.GeoPoint(value_pb.geo_point_value.latitude,
                                      value_pb.geo_point_value.longitude)

        elif value_type == 'null_value':
            result = None
        else:
            raise ValueError('Value protobuf did not have any value set')

        return result

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
        return CustomIterator.object_from_protobuf(entity_pb, model_type=self.model_type)

    @staticmethod
    def key_from_protobuf(pb):
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
    _client: datastore.Client  # client to read/query from (reads are from a single client)
    _clients: List[datastore.Client]  # list of all clients (writes occur to all clients)
    _type: object
    _cache: StrictRedis

    def __init__(self, *path_args, **kwargs):
        if not getattr(self, '_client', None):
            raise ValueError("Datastore _client is not set. Have you called datastore_orm.initialize()?")
        if not kwargs.get('namespace'):
            kwargs['namespace'] = self._client.namespace
        if not kwargs.get('project'):
            kwargs['project'] = self._client.project
        super(CustomKey, self).__init__(*path_args, **kwargs)
        self._type = SubclassMap.get()[self.kind]

    @classmethod
    def __initialize_class__(cls, clients=None, cache=None):
        cls._clients = clients
        cls._client = clients[0]
        cls._cache = cache

    # use_cache should be passed as False if another process is writing to the same entity in Datastore
    def get(self, use_cache=True):
        cache_key = 'datastore_orm.{}.{}'.format(self.kind, self.id_or_name)
        try:
            if self._cache and use_cache:
                obj = self._cache.get(cache_key)
                if obj:
                    obj = pickle.loads(obj)
                    return obj
        except:
            pass

        # Get object from datastore
        obj = self._client.get(self, model_type=self._type)

        try:
            if self._cache:
                self._cache.set(cache_key, pickle.dumps(obj))
        except:
            pass
        return obj

    def _get_updated_key(self, old_key, client):
        if old_key.id_or_name:
            key = CustomKey(old_key.kind, old_key.id_or_name, namespace=client.namespace, project=client.project)
        else:
            key = CustomKey(old_key.kind, namespace=client.namespace, project=client.project)
        return key

    def background_delete(self, key, clients):
        for client in clients:
            new_key = self._get_updated_key(key, client)
            self.retry_background_delete(new_key, client, attempt=1)

    def retry_background_delete(self, key, client, attempt):
        # Sleeping before retrying. sleep time increases for each of the 3 retries.
        sleep(0.5 * attempt)
        if attempt < 4:
            try:
                client.delete(key)
            except:
                logging.warning(F"Failed to delete from datastore in background in attempt {attempt}, retrying.")
                self.retry_background_delete(key, client, attempt + 1)
        else:
            return

    def delete(self):
        """Delete object from datastore.
        """
        if self._cache:
            cache_key = 'datastore_orm.{}.{}'.format(self.kind, self.id_or_name)
            self._cache.delete(cache_key)

        if len(self._clients) > 1:
            Thread(target=self.background_delete, args=(self, self._clients[1:])).start()

        self._clients[0].delete(self)

    def get_multi(self, keys):
        objects = self._client.get_multi(keys, model_type=self._type)
        return objects


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
    """Typically, users will interact with this library by creating sub-classes of BaseModel.

    BaseModel implements various helper methods (such as put, fetch etc.) to allow the user to
    interact with datastore directly from the subclass object.
    """

    _client: datastore.Client
    _clients: List[datastore.Client]
    _exclude_from_indexes_: tuple
    _cache: StrictRedis

    @classmethod
    def __init__(cls, clients=None, cache=None):
        cls._exclude_from_indexes_ = tuple()
        cls._clients = clients
        cls._client = clients[0]
        cls._cache = cache

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

    def put(self, expiry=86400):
        """
        Put the object into datastore.
        """
        # TODO (Chaitanya): Directly convert object to protobuf and call PUT instead of converting to entity first.
        entity = self._to_entity()
        try:
            if self._cache:
                cache_key = 'datastore_orm.{}.{}'.format(self.__class__.__name__, entity.key.id_or_name)
                self._cache.set(cache_key, pickle.dumps(self), expiry)
        except:
            pass
        self.key = entity.key
        if 'key' in entity:
            del entity['key']
        self._clients[0].put(entity)
        # self.background_put(entity, self._clients[1:])
        if len(self._clients) > 1:
            Thread(target=self.background_put, args=(entity, self._clients[1:])).start()
        entity.key._type = self.__class__
        self.key = entity.key
        return self.key

    def background_put(self, entity, clients):
        for client in clients:
            new_entity = copy.deepcopy(entity)
            for k, v in entity.items():
                if isinstance(v, CustomKey):
                    new_entity[k] = self._get_updated_key(v, client)
                elif isinstance(v, list) and len(v) > 0 and isinstance(v[0], CustomKey):
                    new_entity[k] = [self._get_updated_key(old_key, client) for old_key in v]
                # TODO: Handle keys in dict and nested objects
            new_entity.key = self._get_updated_key(new_entity.key, client)
            self.retry_background_write(new_entity, client, attempt=1)

    def retry_background_write(self, entity, client, attempt):
        # Sleeping before retrying. sleep time increases for each of the 3 retries.
        sleep(0.5 * attempt)
        if attempt < 4:
            try:
                client.put(entity)
            except:
                logging.warning(F"Failed to write to datastore in background in attempt {attempt}, retrying.")
                self.retry_background_write(entity, client, attempt + 1)
        else:
            return

    def _get_updated_key(self, old_key, client):
        if old_key.id_or_name:
            key = CustomKey(old_key.kind, old_key.id_or_name, namespace=client.namespace, project=client.project)
        else:
            key = CustomKey(old_key.kind, namespace=client.namespace, project=client.project)
        return key

    def background_delete(self, key, clients):
        for client in clients:
            new_key = self._get_updated_key(key, client)
            self.retry_background_delete(new_key, client, attempt=1)

    def retry_background_delete(self, key, client, attempt):
        # Sleeping before retrying. sleep time increases for each of the 3 retries.
        sleep(0.5 * attempt)
        if attempt < 4:
            try:
                client.delete(key)
            except:
                logging.warning(F"Failed to delete from datastore in background in attempt {attempt}, retrying.")
                self.retry_background_delete(key, client, attempt + 1)
        else:
            return

    def delete(self):
        """Delete object from datastore.
        """
        # Delete from cache first
        if self._cache:
            cache_key = 'datastore_orm.{}.{}'.format(self.__class__.__name__, self.key.id_or_name)
            self._cache.delete(cache_key)

        # Pass the key for deleting from other clients in background
        if len(self._clients) > 1:
            Thread(target=self.background_delete, args=(self.key, self._clients[1:])).start()

        # Delete the key from 1st client
        self._clients[0].delete(self.key)

    def to_dict(self, exclude: set = None):
        if type(exclude) == list:
            exclude = set(exclude)
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
        name = cls.__name__
        return CustomQuery(cls, client=cls._client, kind=cls.__name__, **kwargs)


class CustomClient(Client):
    _client: datastore.Client
    _cache: StrictRedis

    def __init__(self, client=None, cache=None):
        self._client = client
        self._cache = cache
        if not getattr(self, '_client', None):
            raise ValueError("Datastore _client is not set. Have you called datastore_orm.initialize()?")
        super(CustomClient, self).__init__(project=self._client.project, namespace=self._client.namespace,
                                           credentials=self._client._credentials)

    def get(self, key, missing=None, deferred=None,
            transaction=None, eventual=False, model_type=None):
        """Retrieve an entity from a single key (if it exists).

        .. note::

           This is just a thin wrapper over :meth:`get_multi`.
           The backend API does not make a distinction between a single key or
           multiple keys in a lookup request.

        :type key: :class:`google.cloud.datastore.key.Key`
        :param key: The key to be retrieved from the datastore.

        :type missing: list
        :param missing: (Optional) If a list is passed, the key-only entities
                        returned by the backend as "missing" will be copied
                        into it.

        :type deferred: list
        :param deferred: (Optional) If a list is passed, the keys returned
                         by the backend as "deferred" will be copied into it.

        :type transaction:
            :class:`~google.cloud.datastore.transaction.Transaction`
        :param transaction: (Optional) Transaction to use for read consistency.
                            If not passed, uses current transaction, if set.

        :type eventual: bool
        :param eventual: (Optional) Defaults to strongly consistent (False).
                         Setting True will use eventual consistency, but cannot
                         be used inside a transaction or will raise ValueError.

        :rtype: :class:`google.cloud.datastore.entity.Entity` or ``NoneType``
        :returns: The requested entity if it exists.

        :raises: :class:`ValueError` if eventual is True and in a transaction.
        """
        start = datetime.datetime.now()
        entities = self.get_multi(keys=[key],
                                  missing=missing,
                                  deferred=deferred,
                                  transaction=transaction,
                                  eventual=eventual, model_type=model_type)
        if entities:
            end = datetime.datetime.now()
            print('Time taken for get {}'.format(end - start))
            return entities[0]

    def get_multi(self, keys, missing=None, deferred=None,
                  transaction=None, eventual=False, model_type=None):
        """Retrieve entities, along with their attributes.

        :type keys: list of :class:`google.cloud.datastore.key.Key`
        :param keys: The keys to be retrieved from the datastore.

        :type missing: list
        :param missing: (Optional) If a list is passed, the key-only entities
                        returned by the backend as "missing" will be copied
                        into it. If the list is not empty, an error will occur.

        :type deferred: list
        :param deferred: (Optional) If a list is passed, the keys returned
                         by the backend as "deferred" will be copied into it.
                         If the list is not empty, an error will occur.

        :type transaction:
            :class:`~google.cloud.datastore.transaction.Transaction`
        :param transaction: (Optional) Transaction to use for read consistency.
                            If not passed, uses current transaction, if set.

        :type eventual: bool
        :param eventual: (Optional) Defaults to strongly consistent (False).
                         Setting True will use eventual consistency, but cannot
                         be used inside a transaction or will raise ValueError.

        :rtype: list of :class:`google.cloud.datastore.entity.Entity`
        :returns: The requested entities.
        :raises: :class:`ValueError` if one or more of ``keys`` has a project
                 which does not match our project.
        :raises: :class:`ValueError` if eventual is True and in a transaction.
        """
        if not keys:
            return []
        get_multi_partial = partial(self.get_single, missing=missing, deferred=deferred, transaction=transaction,
                                    eventual=eventual, model_type=model_type)
        with ThreadPoolExecutor(max_workers=min(len(keys), 10)) as executor:
            basemodels = []
            map_iterator = [[key] for key in keys]
            results = executor.map(get_multi_partial, map_iterator)
            for result in results:
                basemodels.append(result[0] if result else None)
            return basemodels

    def get_single(self, keys, missing=None, deferred=None,
                   transaction=None, eventual=False, model_type=None):
        cache_key = 'datastore_orm.{}.{}'.format(keys[0].kind, keys[0].id_or_name)
        try:
            if self._cache:
                obj = self._cache.get(cache_key)
                if obj:
                    return [pickle.loads(obj)]
        except:
            pass

        ids = set(key.project for key in keys)
        for current_id in ids:
            if current_id != self.project:
                raise ValueError('Keys do not match project')

        if transaction is None:
            transaction = self.current_transaction

        entity_pbs = _extended_lookup(
            datastore_api=self._datastore_api,
            project=self.project,
            key_pbs=[key.to_protobuf() for key in keys],
            eventual=eventual,
            missing=missing,
            deferred=deferred,
            transaction_id=transaction and transaction.id,
        )

        if missing is not None:
            missing[:] = [
                CustomIterator.object_from_protobuf(missed_pb, model_type=model_type)
                for missed_pb in missing]

        if deferred is not None:
            deferred[:] = [
                CustomIterator.key_from_protobuf(deferred_pb)
                for deferred_pb in deferred]

        basemodels = [CustomIterator.object_from_protobuf(entity_pb, model_type=model_type)
                      for entity_pb in entity_pbs]
        try:
            if self._cache and basemodels:
                self._cache.set(cache_key, pickle.dumps(basemodels[0]))
        except:
            pass
        return basemodels


def initialize(clients, cache=None):
    if not isinstance(clients, list):
        clients = [clients]

    orm_clients = []
    for client in clients:
        orm_clients.append(CustomClient(client=client, cache=cache))

    BaseModel(orm_clients, cache)
    CustomKey.__initialize_class__(orm_clients, cache)
