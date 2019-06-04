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

from elasticsearch import Elasticsearch
import json
import bson
from ivre.db import DB, DBActive, DBView, LockError
from ivre import config, utils, xmlnmap


class Nmap2MongElastic(xmlnmap.Nmap2DB):
    @staticmethod
    def _to_binary(data):
        return bson.Binary(data)


class ElasticDB(DB):

    def __init__(self, host, dbname):
        self.host = host
        self.dbname = dbname

    @property
    def db_client(self):
        """The DB connection."""
        try:
            return self._db_client
        except AttributeError:
            self._db_client = Elasticsearch([self.host])
            return self._db_client

    @property
    def db(self):
        """The DB."""
        try:
            return self._db
        except AttributeError:
            self._db = self.db_client[self.dbname]
            if self.username is not None:
                if self.password is not None:
                    self.db.authenticate(self.username, self.password)
                elif self.mechanism is not None:
                    self.db.authenticate(self.username,
                                         mechanism=self.mechanism)
                else:
                    raise TypeError("provide either 'password' or 'mechanism'"
                                    " with 'username'")
            return self._db

    # filters
    flt_empty = {}


class ElasticDBActive(ElasticDB, DBActive):

    column_hosts = 0

    def __init__(self, host, dbname, colname_hosts, **kwargs):
        ElasticDB.__init__(self, host, dbname, **kwargs)
        DBActive.__init__(self)
        self.columns = [colname_hosts]

    def init(self):
        """Initializes the "active" columns, i.e., drops those columns and
creates the default indexes."""

    def store_or_merge_host(self, host):
        raise NotImplementedError


class ElasticDBView(ElasticDBActive, DBView):

    def __init__(self, host, dbname,  colname_hosts="views", **kwargs):
        ElasticDBActive.__init__(self, host, dbname, colname_hosts=colname_hosts,
                               **kwargs)
        DBView.__init__(self)

    def store_or_merge_host(self, host):
        if not self.merge_host(host):
            self.store_host(host)
