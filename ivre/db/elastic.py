#! /usr/bin/env python
# -*- coding: utf-8 -*-

# This file is part of IVRE.
# Copyright 2011 - 2019 Pierre LALET <pierre.lalet@cea.fr>
#
# IVRE is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# IVRE is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
# License for more details.
#
# You should have received a copy of the GNU General Public License
# along with IVRE. If not, see <http://www.gnu.org/licenses/>.

"""This sub-module contains functions to interact with the ElasticSearch
databases.

"""

try:
    from urllib.parse import unquote
except ImportError:
    from urllib import unquote


from elasticsearch import Elasticsearch, helpers


from ivre.db import DB, DBActive, DBView
from ivre import utils


class ElasticDB(DB):

    # filters
    flt_empty = {'match_all': {}}

    def __init__(self, url):
        super(ElasticDB, self).__init__()
        self.username = ''
        self.password = ''
        self.hosts = None
        if '@' in url.netloc:
            username, hostname = url.netloc.split('@', 1)
            if ':' in username:
                self.username, self.password = (unquote(val) for val in
                                                username.split(':', 1))
            else:
                self.username = unquote(username)
            if hostname:
                self.hosts = [hostname]
        elif url.netloc:
            self.hosts = [url.netloc]
        index_prefix = url.path.lstrip('/')
        if index_prefix:
            self.index_prefix = index_prefix + '-'
        else:
            self.index_prefix = 'ivre-'
        self.params = dict(x.split('=', 1) if '=' in x else (x, None)
                           for x in url.query.split('&') if x)

    def init(self):
        """Initializes the mappings."""
        for idxnum, mapping in enumerate(self.mappings):
            idxname = self.indexes[idxnum]
            self.db_client.indices.delete(
                index=idxname,
                ignore=[400, 404],
            )
            self.db_client.indices.create(
                index=idxname,
                body={"mappings": {"properties": mapping}},
                ignore=400,
            )

    @property
    def db_client(self):
        """The DB connection."""
        try:
            return self._db_client
        except AttributeError:
            self._db_client = Elasticsearch(
                hosts=self.hosts,
                http_auth=(self.username, self.password)
            )
            return self._db_client

    @property
    def server_info(self):
        """Server information."""
        try:
            return self._server_info
        except AttributeError:
            self._server_info = self.db_client.info()
            return self._server_info

    @staticmethod
    def to_binary(data):
        return utils.encode_b64(data).decode()

    @staticmethod
    def from_binary(data):
        return utils.decode_b64(data.encode())

    @classmethod
    def searchhost(cls, addr, neg=False):
        """Filters (if `neg` == True, filters out) one particular host
        (IP address).
        """
        return {"match": {"addr": addr}}

    @classmethod
    def searchhosts(cls, hosts, neg=False):
        pass


class ElasticDBActive(ElasticDB, DBActive):

    mappings = [
        {
            "addr": {"type": "ip"},
        },
    ]
    index_hosts = 0

    def store_or_merge_host(self, host):
        raise NotImplementedError

    def store_host(self, host):
        self.db_client.index(index=self.indexes[0],
                             body=host)

    def count(self, flt):
        return self.db_client.search(
            body={"query": flt},
            index=self.indexes[0],
            size=0
        )['hits']['total']['value']

    def get(self, spec, **kargs):
        """Queries the active index."""
        for rec in helpers.scan(self.db_client, query={"query": spec},
                                index=self.indexes[0], ignore_unavailable=True):
            yield dict(rec['_source'], _id=rec['_id'])


class ElasticDBView(ElasticDBActive, DBView):

    def __init__(self, url):
        super(ElasticDBView, self).__init__(url)
        self.indexes = ['%s%s' % (self.index_prefix,
                                  self.params.pop('indexname_hosts', 'hosts'))]

    def store_or_merge_host(self, host):
        if not self.merge_host(host):
            self.store_host(host)
