#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from sqlalchemy import create_engine, Column, Table, MetaData
from sqlalchemy.sql import select

CONNECTION_STRING = 'postgres://postgres@localhost'


def _raw_execute(sql: str) -> bool:
    """ Try to execute a raw SQL query, dealing with PostgreSQL nuances. """

    with get_engine().connect() as conn:
        try:
            conn.execute('commit')
            conn.execute(sql)
            return True

        except Exception as e:
            print('error: %s' % str(e), file=sys.stderr)

    return False


def create_db(name: str) -> bool:
    """ Try to create a new db. Return True on success, False on failure. """
    return _raw_execute("CREATE DATABASE %s ENCODING 'UTF8';" % name)


def drop_db(name: str) -> bool:
    """ Try to destroy a db by name. True on success, False on failure. """
    return _raw_execute("DROP DATABASE %s;" % name)


def get_engine(db: str = None):
    if db:
        return create_engine('%s/%s' % (CONNECTION_STRING, db))
    return create_engine(CONNECTION_STRING)


def get_metadata(db: str) -> MetaData:
    return MetaData(get_engine(db))


def create_table(name: str, db: str, *cols: Column) -> Table:
    """ Try to create a table in a given database (by name) """
    table = Table(name, get_metadata(db), *cols)
    table.create()

    return table


def select_table(table: Table, db: str):
    with get_engine(db).connect() as conn:
        rs = conn.execute(select([table]))
        return rs.fetchall()
