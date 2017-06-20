#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from sqlalchemy import create_engine

eng = create_engine('postgres://postgres@localhost')


def create_db(name: str) -> bool:
    """ Try to create a new db. Return True on success, False on failure. """
    result = False
    try:
        conn = eng.connect()
        try:
            conn.execute('commit')
            conn.execute("CREATE DATABASE %s ENCODING 'UTF8';" % name)
        finally:
            conn.close()
        result = True
    except Exception as e:
        print('create_db: cannot create database %s' % name, file=sys.stderr)
        print('error: %s' % str(e), file=sys.stderr)

    return result


def drop_db(name: str) -> bool:
    """ Try to destroy a db by name. True on success, False on failure. """
    result = False
    try:
        conn = eng.connect()
        try:
            conn.execute('commit')
            conn.execute('DROP DATABASE %s' % name)
        finally:
            conn.close()
        result = True
    except Exception as e:
        print('drop_db: cannot drop database %s' % name, file=sys.stderr)
        print('error: %s' % str(e), file=sys.stderr)

    return result
