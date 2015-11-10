# -*- coding: utf-8 -*-

from orator import DatabaseManager as BaseDatabaseManager
from orator.connectors.connection_factory import ConnectionFactory

from .query import CachedQueryBuilder


class DatabaseManager(BaseDatabaseManager):

    def __init__(self, config, factory=ConnectionFactory(), cache=None):
        super(DatabaseManager, self).__init__(config, factory)

        self._cache = cache

    def _prepare(self, connection):
        """
        Prepares a connection.

        :param connection: The connection to prepare
        :type connection: orator.connections.Connection

        :rtype: orator.connections.Connection
        """
        connection = super(DatabaseManager, self)._prepare(connection)

        # Setting cache if it exists
        if self._cache:
            connection.set_builder_class(
                CachedQueryBuilder,
                {'cache': self._cache}
            )

        return connection

    def set_cache(self, cache):
        self._cache = cache

    def get_cache(self):
        return self._cache
