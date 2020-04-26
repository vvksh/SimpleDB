# User guide
From [Lab 2 instructions](instructions/lab2/MIT6_830F10_lab2.pdf). The instruction also shows how to with [nsf data](instructions/lab2/nsf_data.tar.gz).

-----------
To build jar

```
./gradlew shadowJar
```
This will create a jar - `SimpleDB-1.0-SNAPSHOT-all.jar` in `build/libs/`

--------
We've provided you with a query parser for SimpleDB that you can use to write and run SQL queries against your database once you have completed the exercises in this lab. 

The first step is to create some data tables and a catalog. Suppose you have a file data.txt with the following contents: 

```
1,10
2,20
3,30
4,40
5,50
5,50
```

You can convert this into a SimpleDB table using the convert command

```
java -jar build/libs/SimpleDB-1.0-SNAPSHOT-all.jar convert data.txt 2 "int,int"
```


This creates a file data.dat. In addition to the table's raw data, the two additional parameters specify that each record has two fields and that their types are int and int. 

Next, create a catalog file, catalog.txt, with the follow contents: 

```
data (f1 int, f2 int)
```

This tells SimpleDB that there is one table, data (stored in data.dat) with two integer fields named f1 and f2. 

Finally, invoke the parser. You must run java from the command line  From the simpledb/ directory, type: 

```
java -jar build/libs/SimpleDB-1.0-SNAPSHOT-all.jar  parser catalog.txt
```

You should see output like

```
Added table : data with schema INT(f1), INT(f2),
SimpleDB> 
```

Finally, you can run a query: 

```
SimpleDB> select d.f1, d.f2 from data d;
Started a new transaction tid = 1221852405823
 ADDING TABLE d(data) TO tableMap
      TABLE HAS  tupleDesc INT(d.f1), INT(d.f2),
 1       10
 2       20
 3       30
 4       40
 5       50
 5       50 
 6 rows.
 ----------------
 0.16 seconds

 SimpleDB>

 ```

 The parser is relatively full featured (including support for SELECTs, INSERTs, DELETEs, and transactions), but does have some problems and does not necessarily report completely informative error messages. Here are some limitations to bear in mind: 

 - You must preface every field name with its table name, even if the field name is unique (you can use table name aliases, as in the example above, but you cannot use the AS keyword.)

 - Nested queries are supported in the WHERE clause, but not the FROM clause. 
 - No arithmetic expressions are supported (for example, you can't take the sum of two fields. )
 - At most one GROUP BY and one aggregate column are allowed. 
 - Set-oriented operators like IN, UNION, and EXCEPT are not allowed.
 - Only AND expressions in the WHERE clause are allowed.
 - UPDATE expressions are not supported. 
 - The string operator LIKE is allowed, but must be written out fully (that is, the Postgres tilde [~] shorthand is not allowed.)   



