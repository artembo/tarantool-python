# -*- coding: utf-8 -*-

from __future__ import print_function

import sys
import unittest

import dbapi20

import tarantool
from tarantool.dbapi import Connection
from .lib.tarantool_server import TarantoolServer


class TestSuite_DBAPI(dbapi20.DatabaseAPI20Test):
    table_prefix = 'dbapi20test_'  # If you need to specify a prefix for tables

    ddl1 = 'create table %sbooze (name varchar(20) primary key)' % table_prefix
    ddl2 = 'create table %sbarflys (name varchar(20) primary key, ' \
           'drink varchar(30))' % table_prefix

    @classmethod
    def setUpClass(self):
        print(' DBAPI '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        self.srv = TarantoolServer()
        self.srv.script = 'unit/suites/box.lua'
        self.srv.start()
        self.con = tarantool.Connection(self.srv.host, self.srv.args['primary'])
        self.driver = Connection(self.srv.host, self.srv.args['primary'])

    def setUp(self):
        # prevent a remote tarantool from clean our session
        if self.srv.is_started():
            self.srv.touch_lock()
        self.con.flush_schema()

        # grant full access to guest
        self.srv.admin("box.schema.user.grant('guest', 'create,read,write,"
                       "execute', 'universe')")

    @classmethod
    def tearDownClass(self):
        self.con.close()
        self.srv.stop()
        self.srv.clean()

    @unittest.skip('Not implemented')
    def test_Binary(self):
        pass

    @unittest.skip('Not implemented')
    def test_STRING(self):
        pass

    @unittest.skip('Not implemented')
    def test_BINARY(self):
        pass

    @unittest.skip('Not implemented')
    def test_NUMBER(self):
        pass

    @unittest.skip('Not implemented')
    def test_DATETIME(self):
        pass

    @unittest.skip('Not implemented')
    def test_ROWID(self):
        pass

    @unittest.skip('Not implemented')
    def test_Date(self):
        pass

    @unittest.skip('Not implemented')
    def test_Time(self):
        pass

    @unittest.skip('Not implemented')
    def test_Timestamp(self):
        pass

    @unittest.skip('Not implemented as optional.')
    def test_nextset(self):
        pass

    @unittest.skip('To do')
    def test_callproc(self):
        pass

    def test_setoutputsize(self):  # Do nothing
        pass

    @unittest.skip('To do')
    def test_description(self):
        pass
