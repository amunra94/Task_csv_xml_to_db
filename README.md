# Task_csv_xml_to_db
Simple task to add csv or xml file into define table(customer,posting_date,amount) of any database. 

1)You have 2 type of input files:csv or xml, which you should add in table.

2)You have table with columns(customer(varchar(20), posting_date(datetime), amount(decimal))).

3)You some additional causes:

- Customer can be more one time

- Total amount some customer per day has limit in 1000.

- You need to logg some errors and warnings
    
This is simple implementation for limit time on this work. Just for fan. It isn't faster implementation.

I have used SQLite database, because it's very simple for little task.


# Requirements
SQLAlchemy.

Other components are from standard library. 
