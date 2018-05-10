"""
I made 2 common test of load_csv and load_xml.
Of course this utility is needed more tests.
I have tested script from IDE PyCharm which it integrates with unittest.
"""
import unittest
import csv2db
import sqlalchemy
import datetime
import decimal


class DataBaseTestCase(unittest.TestCase):

    def test_load_csv(self):
        """
            Common case test of load_csv.
                GOAL: Check right of insert rows.
                DESCRIPTION:
                    Test input csv file contains different errors of format in lines.
                    We push this file in table. Then we check right of insert lines.
        """
        name_db = 'test_db.sqlite'
        name_table = 'test_csv'
        name_csv = 'test_input.csv'
        com_separate = ';'
        answer = [('A-200', datetime.datetime(2018, 4, 1, 0, 0), decimal.Decimal('100.0000000000')),
                  ('A-500', datetime.datetime(2017, 1, 23, 0, 0), decimal.Decimal('300.5032234000')),
                  ('B-21', datetime.datetime(2017, 1, 21, 0, 0), decimal.Decimal('500.0000000000')),
                  ('A-100', datetime.datetime(2018, 3, 1, 0, 0), decimal.Decimal('200.0000000000')),
                  ('B-21', datetime.datetime(2017, 1, 21, 0, 0), decimal.Decimal('10.0000000000')),
                  ('B-21', datetime.datetime(2017, 1, 21, 0, 0), decimal.Decimal('130.0000000000'))]

        db = csv2db.DataBase(name_db, name_table)
        db.load_csv(name_csv, com_separate)

        with db.engine.connect() as conn:
            meta = sqlalchemy.MetaData(db.engine)
            users_table = sqlalchemy.Table(db.name_tab, meta, autoload=True)
            records = conn.execute(sqlalchemy.select([users_table.c.customer,
                                                      users_table.c.posting_date,
                                                      users_table.c.amount])).fetchall()
            conn.execute(sqlalchemy.delete(users_table))

        self.assertEqual(records, answer, 'Error! Push rows are not correct!')

    def test_load_xml(self):
        """
            Common case test of load_xml.
                GOAL: Check right of insert rows.
                DESCRIPTION:
                    Test input xml file contains different errors of format in lines.
                    We push this file in table. Then we check right of insert.
        """
        name_db = 'test_db.sqlite'
        name_table = 'test_xml'
        name_csv = 'test_input.xml'
        answer = [('A-120xml', datetime.datetime(2017, 1, 23, 0, 0), decimal.Decimal('27.0000000000')),
                  ('A-125xml', datetime.datetime(2017, 1, 23, 0, 0), decimal.Decimal('505.0000000000')),
                  ('B-220xml', datetime.datetime(2017, 1, 26, 0, 0), decimal.Decimal('150.0000000000')),
                  ('B-220xml', datetime.datetime(2017, 1, 23, 0, 0), decimal.Decimal('900.0000000000')),
                  ('B-220xml', datetime.datetime(2017, 1, 23, 0, 0), decimal.Decimal('30.0000000000')),
                  ('B-220xml', datetime.datetime(2017, 1, 23, 0, 0), decimal.Decimal('50.0000000000'))]

        db = csv2db.DataBase(name_db, name_table)
        db.load_xml(name_csv)

        with db.engine.connect() as conn:
            meta = sqlalchemy.MetaData(db.engine)
            users_table = sqlalchemy.Table(db.name_tab, meta, autoload=True)
            records = conn.execute(sqlalchemy.select([users_table.c.customer,
                                                      users_table.c.posting_date,
                                                      users_table.c.amount])).fetchall()
            conn.execute(sqlalchemy.delete(users_table))

        self.assertEqual(records, answer, 'Error! Push rows are not correct!')


