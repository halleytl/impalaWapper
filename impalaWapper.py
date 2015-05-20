#!/usr/bin/env python
#-*- coding:utf-8 -*-

import impala
from impala.dbapi import connect


def _check_one(info=None):
    if not info:
        return None
    elif len(info) > 1:
        raise Exception("Multiple rows returned for Database.get() query")
    else:
        return info[0]

class ImpalaWapper(object):

    def __init__(self, host=None, port=21050):
        self.set_host(host)
        self.set_port(port)
        self._db = None
        self.cursor = None
        self.connect()

    def __del__(self):
        self.close()

    def set_host(self, host):
        self.host = host

    def get_host(self):
        return self.host

    def set_port(self, port):
        self.port = port

    def get_port(self):
        return self.port

    def reconnect(self):
        self.close()
        self.connect()

    def connect(self):
        _base_ = {"host": self.get_host(),
                  "port": self.get_port()
                 }
        self._db = connect(**_base_)
        self.cursor = self._cursor()

    def close(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self._db:
            self._db.close()
            self._db = None

    def _cursor(self):
        if self._db:
            return self._db.cursor()
        else:
            raise Exception("Impala not connect")

    def raw_query(self, query, **kwargs):
        self.cursor.execute(query, parameters=kwargs)
        table_keys = [keys[0] for keys in self.cursor.description]
        table_vales = [value for value in self.cursor]
        return table_keys, table_vales

    def get(self, query, **kwargs):
        rows = self.query(query, **kwargs)
        _check_one(rows)

    def one(self, query, **kwargs):
        rows = self.onelist(query, **kwargs)
        _check_one(rows)

    def query(self, query, **kwargs):
        keys, values = self.raw_query(query, **kwargs)
        return [dict(zip(keys, value)) for value in values]

    def onelist(self, query, **kwargs):
        _, values = self.raw_query(query, **kwargs)
        return values

    def oneset(self, query, **kwargs):
        return set(self.onelist(query, **kwargs))

    def execute(self, query, **kwargs):
        self.cursor.execute(query, parameters=kwargs)

def main():
    c = ImpalaWapper("192.168.1.97")
    while 1:
        tmp = raw_input("impala>>")
        try:
            cmd, sql = tmp.split(" ", 1)
            print eval("c.{0}(\"{1}\")".format(cmd, sql))
        except impala.error.HiveServer2Error, e:
            print "[error], %s" % str(e)
            print tmp  

if __name__ == "__main__":
    main()
