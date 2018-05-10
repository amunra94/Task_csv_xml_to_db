import datetime
import sqlalchemy
import logging
import decimal
import xml.etree.ElementTree

NAME_DB = 'test_db.sqlite'
NAME_TABLE = 'A'


class DataBase:
    def __init__(self, name_db, name_tab):
        logger = logging.getLogger('DataBase.initial')
        self.name_tab = name_tab
        self.engine = sqlalchemy.create_engine('%s:///' % (name_db.split('.')[1]) + name_db, echo=False)
        self.metadata = sqlalchemy.MetaData()
        self.users_table = sqlalchemy.Table(name_tab, self.metadata,
                                            sqlalchemy.Column('customer', sqlalchemy.String(20)),
                                            sqlalchemy.Column('posting_date', sqlalchemy.DateTime),
                                            sqlalchemy.Column('amount', sqlalchemy.DECIMAL),
                                            )
        self.metadata.create_all(bind=self.engine)
        logger.info("Table has been created.")

    def load_csv(self, name_csv, separate=','):
        """ To load csv values in database """

        logger = logging.getLogger('DataBase.load_csv')
        for line in open(name_csv, 'r'):
            try:
                customer, posting_date, amount = line.strip().split(separate)
                amount_dec = decimal.Decimal(amount)
                posting_date = datetime.datetime.strptime(posting_date, '%d%m%Y')
            except Exception:
                logger.error("Line isn't correct format! Check format of all lines in input file!")
                continue

            if self.check_limit(customer, posting_date, amount_dec):
                self.insert_values(customer=customer, posting_date=posting_date, amount=amount)
        logger.info('Well done!')

    def load_xml(self, name_xml):
        """ To load xml values in database """

        logger = logging.getLogger('DataBase.load_xml')
        tree = xml.etree.ElementTree.parse(name_xml)
        root = tree.getroot()
        for entry in root:
            try:
                customer, posting_date, amount = list(entry)
                amount_dec = decimal.Decimal(amount.text)
                posting_date = datetime.datetime.strptime(posting_date.text, '%d%m%Y')
            except Exception:
                logger.error("Tag isn't correct format! Check format of all tags in input file!")
                continue
            if self.check_limit(customer.text, posting_date, amount_dec):
                self.insert_values(customer=customer.text,
                                   posting_date=posting_date,
                                   amount=amount.text)
        logger.info('Well done!')

    def insert_values(self, **kwargs):
        """ Insert line of values """
        logger = logging.getLogger('DataBase.insert_values')
        with self.engine.connect() as conn:
            meta = sqlalchemy.MetaData(self.engine)
            users_table = sqlalchemy.Table(self.name_tab, meta, autoload=True)
            conn.execute(users_table.insert(),
                         customer=kwargs['customer'],
                         posting_date=kwargs['posting_date'],
                         amount=kwargs['amount']
                         )
        logger.info('Well done!')

    def check_limit(self, customer, posting_date, amount):
        """ Check additional cases of task """

        logger = logging.getLogger('DataBase.check_limit')
        if amount <= 0:
            logger.warning("CUSTOMER: %s DATE: %s AMOUNT: %f. Amount is'not correct!" % (customer, posting_date, amount))
            return False
        if len(customer) > 20:
            logger.warning('Length of customer:%s is more than 20 symbols!' % customer)
            return False
        total_amount = self.get_total_amount(customer, posting_date)
        if total_amount is not None:
            if total_amount + amount >= 1000:
                logger.warning('TOTAL AMOUNT per day: %s of CUSTOMER: %s exceeded 1000!' % (posting_date, customer))
                return False
        else:
            return amount < 1000
        return True

    def get_total_amount(self, customer, date):
        """ Get total amount of one customer per day """
        logger = logging.getLogger('DataBase.get_total_amount')
        with self.engine.connect() as conn:
            meta = sqlalchemy.MetaData(self.engine)
            users_table = sqlalchemy.Table(self.name_tab, meta, autoload=True)
            records = conn.execute(sqlalchemy.select([sqlalchemy.func.sum(users_table.c.amount)])
                                   .where(sqlalchemy.and_(users_table.c.customer == customer,
                                                          users_table.c.posting_date == date))).fetchall()
        logger.info('Well done!')
        return records[0][0]


def main():
    logging.basicConfig(filename='logging.log',
                        level=logging.INFO,
                        filemode='w',
                        format=u'%(filename)s:%(name)-30s [LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s')

    logger = logging.getLogger(__name__)
    logger.info('Program has been started!')
    db = DataBase(NAME_DB, NAME_TABLE)
    db.load_csv('input.csv', ';')
    db.load_xml('input.xml')
    logger.info('Program has been finished!')


if __name__ == '__main__':
    main()
