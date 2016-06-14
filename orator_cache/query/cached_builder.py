# -*- coding: utf-8 -*-

import hashlib
import copy
from ..utils import encode
from orator.query.builder import QueryBuilder


class CachedQueryBuilder(QueryBuilder):

    def __init__(self, connection, grammar, processor, cache):
        """
        Constructor

        :param connection: A Connection instance
        :type connection: Connection

        :param grammar: A QueryGrammar instance
        :type grammar: QueryGrammar

        :param processor: A QueryProcessor instance
        :type processor: QueryProcessor

        :param cache: A CacheManager instance
        :type cache: cachy.CacheManager
        """
        super(CachedQueryBuilder, self).__init__(connection, grammar, processor)

        self._cache = cache
        self._cache_key = None
        self._cache_minutes = None
        self._cache_tags = None
        self._cache_driver = None

    def get(self, columns=None):
        """
        Execute the query as a "select" statement

        :param columns: The columns to get
        :type columns: list

        :return: The result
        :rtype: list
        """
        if columns is None:
            columns = ['*']

        if self._cache_minutes is not None:
            return self.get_cached(columns)

        return super(CachedQueryBuilder, self).get(columns)

    def get_cached(self, columns=None):
        """
        Execute the query as cached a "select" statement

        :param columns: The columns to get
        :type columns: list

        :return: The result
        :rtype: list
        """
        if columns is None:
            columns = ['*']

        if not self.columns:
            self.columns = columns

        # If the query is requested to be cached, we will cache it using a unique key
        # for this database connection and query statement, including the bindings
        # that are used on this query, providing great convenience when caching.
        key, minutes = self._get_cache_info()

        cache = self._get_cache()

        callback = self._get_cache_callback(columns)

        # If the "minutes" value is less than zero, we will use that as the indicator
        # that the values should be stored indefinitely
        # and if we have minutes we will use the typical remember function here.
        if minutes < 0:
            return cache.remember_forever(key, callback)

        return cache.remember(key, minutes, callback)

    def remember(self, minutes, key=None):
        """
        Indicate that the query results should be cached.

        :param minutes: The cache lifetime in minutes
        :type minutes: int

        :param key: The cache key
        :type key: str

        :rtype: CachedQueryBuilder
        """
        self._cache_minutes = minutes
        self._cache_key = key

        return self

    def remember_forever(self, key=None):
        """
        Indicate that the query results should be cached forever.

        :param key: The cache key
        :type key: str

        :rtype: CachedQueryBuilder
        """
        return self.remember(-1, key)

    def cache_tags(self, *tags):
        """
        Indicate that the results, if cached, should use the given tags.

        :param tags: The cache tags
        :type tags: mixed

        :rtype: CachedQueryBuilder
        """
        if len(tags) == 1 and isinstance(tags[0], list):
            tags = tags[0]

        self._cache_tags = tags

        return self

    def cache_driver(self, driver):
        """
        Indicate that the results, if cached, should use the given driver.

        :param driver: The cache driver
        :type driver: mixed

        :rtype: CachedQueryBuilder
        """
        self._cache_driver = driver

        return self

    def cache_store(self, store):
        """
        Indicate that the results, if cached, should use the given store.

        :param store: The cache store
        :type store: mixed

        :rtype: CachedQueryBuilder
        """
        return self.cache_driver(store)

    def _get_cache(self):
        """
        Get the cache object with tags assigned, if applicable

        :rtype: cachy.Repository
        """
        cache = self._cache.store(self._cache_driver)

        if self._cache_tags:
            return cache.tags(self._cache_tags)

        return cache

    def _get_cache_info(self):
        """
        Get the cache key and cache minutes as a tuple.

        :rtype: tuple
        """
        return self.get_cache_key(), self._cache_minutes

    def get_cache_key(self):
        """
        Get a unique cache key for the complete query.

        :rtype: str
        """
        if self._cache_key:
            return self._cache_key

        return self.generate_cache_key()

    def generate_cache_key(self):
        """
        Generate the unique cache key for the query.

        :rtype: str
        """
        name = self._connection.get_name()
        cache = self._get_cache()

        return '%s' % (
            hashlib.sha1(
                encode(name) + encode(self.to_sql()) + cache.serialize(self.get_bindings())
            ).hexdigest()
        )

    def _get_cache_callback(self, columns):
        """
        Get the function used when caching queries.

        :param columns: The query columns
        :type columns: list

        :rtype: callable
        """
        return lambda: self.get_fresh(columns)

    def __copy__(self):
        new = self.__class__(self._connection, self._grammar, self._processor, self._cache)

        new.__dict__.update(dict((k, copy.deepcopy(v)) for k, v
                                 in self.__dict__.items()
                                 if k not in ['_connection', '_grammar', '_processor', '_cache']))

        return new
