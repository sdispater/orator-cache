# -*- coding: utf-8 -*-

from unittest import TestCase
from orator import Model
from orator.connections import SQLiteConnection
from orator.connectors.sqlite_connector import SQLiteConnector
from orator_cache import DatabaseManager, Cache

cache = Cache({
    'stores': {
        'dict': {
            'driver': 'dict'
        }
    }
})


class OratorCacheTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = DatabaseIntegrationConnectionResolver({}, cache=cache)
        Model.set_connection_resolver(cls.db)

    @classmethod
    def tearDownClass(cls):
        Model.unset_connection_resolver()

    def setUp(self):
        with self.schema().create('users') as table:
            table.increments('id')
            table.string('email').unique()
            table.integer('votes').default(0)
            table.timestamps()

        with self.schema().create('friends') as table:
            table.integer('user_id')
            table.integer('friend_id')

            table.foreign('user_id').references('id').on('users').on_delete('cascade')
            table.foreign('friend_id').references('id').on('users').on_delete('cascade')

        with self.schema().create('posts') as table:
            table.increments('id')
            table.integer('user_id')
            table.string('name').unique()
            table.timestamps()

            table.foreign('user_id').references('id').on('users')

        with self.schema().create('photos') as table:
            table.increments('id')
            table.morphs('imageable')
            table.string('name')
            table.timestamps()

        for i in range(10):
            user = User.create(email='user%d@foo.com' % (i + 1))

            for j in range(10):
                post = Post(name='User %d Post %d' % (user.id, j + 1))
                user.posts().save(post)

    def tearDown(self):
        self.schema().drop('photos')
        self.schema().drop('posts')
        self.schema().drop('friends')
        self.schema().drop('users')

    def test_query_return_results_from_cache(self):
        user = self.db.table('users').cache_driver('dict').cache_tags(['test1', 'test2']).remember(10).find(1)
        self.assertEqual('user1@foo.com', user['email'])

        self.assertIsNotNone(self.db.table('users').where('email', 'user1@foo.com').first())
        self.db.table('users').where('id', 1).update(email='foo@foo.com')

        user = self.db.table('users').cache_driver('dict').cache_tags(['test1', 'test2']).remember(10).find(1)
        self.assertEqual('user1@foo.com', user['email'])

        self.assertIsNone(self.db.table('users').where('email', 'user1@foo.com').first())

        cache.tags('test1').flush()

        user = self.db.table('users').cache_driver('dict').cache_tags(['test1', 'test2']).remember(10).find(1)
        self.assertEqual('foo@foo.com', user['email'])

    def test_query_return_results_from_cache_with_models(self):
        query = lambda: User.cache_driver('dict').cache_tags(['test1', 'test2']).remember(10)
        user = query().find(1)
        self.assertEqual('user1@foo.com', user.email)

        self.assertIsNotNone(query().where('email', 'user1@foo.com').first())
        user.update(email='foo@foo.com')

        user = query().find(1)
        self.assertEqual('user1@foo.com', user.email)

        self.assertIsNone(User.where('email', 'user1@foo.com').first())

        cache.tags('test1').flush()

        user = query().find(1)
        self.assertEqual('foo@foo.com', user.email)

    def connection(self):
        return Model.get_connection_resolver().connection()

    def schema(self):
        return self.connection().get_schema_builder()


class User(Model):

    __guarded__ = []

    @property
    def friends(self):
        return self.belongs_to_many(User, 'friends', 'user_id', 'friend_id')

    @property
    def posts(self):
        return self.has_many(Post, 'user_id')

    @property
    def post(self):
        return self.has_one(Post, 'user_id')

    @property
    def photos(self):
        return self.morph_many(Photo, 'imageable')


class Post(Model):

    __guarded__ = []

    @property
    def user(self):
        return self.belongs_to(User, 'user_id')

    @property
    def photos(self):
        return self.morph_many(Photo, 'imageable')


class Photo(Model):

    __guarded__ = []

    @property
    def imageable(self):
        return self.morph_to()


class DatabaseIntegrationConnectionResolver(DatabaseManager):

    _connection = None

    def connection(self, name=None):
        if self._connection:
            return self._connection

        self._connection = SQLiteConnection(
            SQLiteConnector().connect({'database': ':memory:'}),
            config={'name': u'test'}
        )

        self._prepare(self._connection)

        return self._connection

    def get_default_connection(self):
        return 'default'

    def set_default_connection(self, name):
        pass
