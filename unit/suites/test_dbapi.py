# -*- coding: utf-8 -*-

from __future__ import print_function

import sys
import unittest

from dbapi20 import DatabaseAPI20Test

from tarantool.dbapi import Connection
from .lib.tarantool_server import TarantoolServer


class TestSuite_DBAPI(DatabaseAPI20Test):
    table_prefix = 'dbapi20test_'  # If you need to specify a prefix for tables

    ddl1 = 'create table %sbooze (name varchar(20) primary key)' % table_prefix
    ddl2 = 'create table %sbarflys (name varchar(20) primary key, ' \
           'drink varchar(30))' % table_prefix

    @classmethod
    def setUpClass(cls):
        print(' DBAPI '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)

    def setUp(self):
        self.srv = TarantoolServer()
        self.srv.script = 'unit/suites/box.lua'
        self.srv.start()
        self.driver = Connection(self.srv.host, self.srv.args['primary'])
        # prevent a remote tarantool from clean our session
        if self.srv.is_started():
            self.srv.touch_lock()
        # grant full access to guest
        self.srv.admin("box.schema.user.grant('guest', 'create,read,write,"
                       "execute', 'universe')")

    def tearDown(self):
        # self.driver.close()
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

    @unittest.skip('To do')
    def test_setoutputsize(self):
        pass

    @unittest.skip('To do')
    def test_description(self):
        pass

    @unittest.skip('To do')
    def test_close(self):
        pass
