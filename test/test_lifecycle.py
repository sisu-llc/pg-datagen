#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import Column, Integer, String
from db import create_db, drop_db, create_table, select_table


def test_create_and_destroy():
    assert create_db('test')
    assert drop_db('test')


def test_create_table():
    table_name = 'test_table'
    drop_db(table_name)

    assert create_db(table_name)
    cols = (
        Column('Id', Integer, primary_key=True),
        Column('Name', String),
        Column('Price', Integer)
    )
    table = create_table('cars', table_name, *cols)
    assert table.name == 'cars'
    assert len(table.columns) == 3

    results = select_table(table, table_name)
    assert len(results) == 0

    drop_db(table_name)


if __name__ == '__main__':
    test_create_and_destroy()
    test_create_table()
