# BITA Interview Assignment

This projects aims to read stock data from a CSV file
and store it in a PostgreSQL database. It uses the Python csv reader
to load the data in batches to reduce memory overhead and handles
duplicated data detection. Throughout the reading process, data is 
validated before saving.

The database keeps track of the loads so that we can identify and 
delete a previous import.

## Running the reader
First install the dependencies (the only non-native Python module used is
psycopg2) and then run the main.py script.

```commandline
pip install -r requirements.txt
python main.py 
```

You will then be prompted to a menu where you can load data from a csv file or
delete a previous import.

## Organising the data
The reader expects to read a file with the following 4 attributes:
- "PointOfSale": string identifier of the point of sale
- "Product": string identifier of the product
- "Date": date of the point of sale of the product 
- "Stock": quantity of the stock 

The PointOfSale and Product are string identifiers. String identifiers affect 
performance when querying and analyzing data. Matching two numerical values 
is faster than matching two strings. Therefore, I made two tables, 'point_of_sale' and
'product', to map these string identifiers to numeric ones. 

These numeric identifiers become foreign keys 
in the 'stock' table that groups everything together, mapping the respective the date and quantity of the stock.

An index on the point of sale name and product name is created in the respective 'point_of_sale' and 
'product' tables. This enables a fast query for their ids.

The 'stock' table will also have a many-to-one relationship with a 'load' table. The purpose of the 'load'
table is to help manipulate data from a specific import. To be able to query the 'stock' table quickly by 
load, an index on the load id is added.

The psycopg driver is adequate for the problem. Due to the simplicity of the data, no ORM is chosen has 
it won't improve performance.

Here's the resulting schema for organizing the data:
```postgresql
CREATE TABLE point_of_sale (
   id serial primary key,
   name text not null
);
CREATE INDEX point_of_sale_by_name ON point_of_sale(name);

CREATE TABLE product (
   id serial primary key,
   name text not null 
);

CREATE INDEX product_by_name ON product(name);

CREATE TABLE load (
    id serial primary key,
    timestamp timestamp not null,
    filename text not null
);

CREATE TABLE stock (
    date date not null,
    stock int not null,
    product_id int not null,
    point_of_sale_id int not null,
    load_id int not null,
    
    FOREIGN KEY (product_id)
      REFERENCES product (id),
    
    FOREIGN KEY (point_of_sale_id)
      REFERENCES point_of_sale (id),
    
    FOREIGN KEY (load_id)
      REFERENCES load (id),
        
    PRIMARY KEY (date, product_id, point_of_sale_id)
);

CREATE INDEX stocks_by_load_id ON stock(load_id);
```

## Reading and saving the data

Because the purpose of the project is to simply load data from a CSV
and there are no calculations to do on the data, the built-in csv library
was chosen. Another option could the pandas library. Even though it is a commonly-used option for
the handling of large datasets, it's overkill for this use case.

During the execution of the program every write and read to the database is done in 
a single transaction. This avoids race conditions with other files being loaded. 
Also ensures data integrity: either all data is loaded or not.

The file is read in batches and written in a temporary table. As the data is read, the values of the file 
are checked to be the same type of the ones set in the database. 

The insertion in the temporary table is 
done in a single "INSERT ... VALUES" SQL operation with the help of psycopg2 helper execute_values(). 
This method provides an efficient way of making a single server round trip per batch via a single insert, 
improving performance.

After going through all the batches of data, the temporary table is used to dump the data in the 'stock' table.
We first find the numeric ids for point of sale and product, if they don't exist we create new ones. 

Then the temporary table does a cross-check to the 'stock' table and certifies we are not duplicating data in the stock table.
That detection occurs when breaking the primary key constraint of the 'stock' table
(point_of_sale_id, product_id, date). The loads affected by conflicts are identified and shown in the program's output.

Finally, the temporary table and the necessary identifiers are dumped in the 'stock' table. 

