# -*- coding: utf-8 -*-

from cachy import CacheManager
from cachy.serializers import PickleSerializer


class Cache(CacheManager):

    _serializers = {
        'pickle': PickleSerializer()
    }
