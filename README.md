# DDS_PostgreSQL

Repository meant for implementing PostgreSQL in Python for distributed database management system.

PostgreSQL Database Management System
=====================================
PostgreSQL is an advanced object-relational database management system
that supports an extended subset of the SQL standard, including
transactions, foreign keys, subqueries, triggers, user-defined types
and functions.

PostgreSQL has many language interfaces, many of which are listed here:

	https://www.postgresql.org/download
	
Process for setting up assignment environment
=============================================
Purpose of this tutorial is to guide you towards setting up the working environment. You need to finish the Virtual Machine section.

PS: You need to make sure your system has Postgres and Python installed and they are compatible with the following set up:

python 2.7,
Ubuntu 16.04,
PostgreSQL 9.5
Virtual Machine

Postgres with Terminal
======================
1 . Open a terminal. Log into Postgres with the default root user.

sudo -su postgres psql
2 . Create a test database and try some operations.

create a test database and switch to it

create database test;
\c test
create a table:

CREATE TABLE USERS(
   USERID INT PRIMARY KEY NOT NULL,
   NAME TEXT NOT NULL
);
CREATE TABLE MOVIES(
   MOVIEID INT PRIMARY KEY NOT NULL,
   TITLE TEXT NOT NULL,
   Genre TEXT
);
CREATE TABLE RATINGS(
   USERID INT REFERENCES USERS(USERID),
   MOVIEID INT REFERENCES MOVIES(MOVIEID),
   RATING NUMERIC NOT NULL CHECK(RATING>=0 AND RATING<=5)
);
check the schema:

select * from users;
select * from movies;
select * from ratings;
insert some data:

# users table
INSERT INTO users VALUES (1, 'David');
INSERT INTO users VALUES (2, 'Eric');
INSERT INTO users VALUES (3, 'Kevin');

# movies table
COPY MOVIES FROM 'movies.dat' delimiter '_';

# ratings table
insert into ratings VALUES (1, 122, 5.0);
insert into ratings VALUES (1, 185, 4.5);
insert into ratings VALUES (2, 231, 4.0);
insert into ratings VALUES (2, 292, 3.5);
insert into ratings VALUES (3, 316, 3.0);
perform search:

select * from ratings where userid=1;
select * from ratings where rating>=3;

select * from users, movies, ratings 
where users.name='David' 
	and users.userid=ratings.userid 
	and movies.movieid=ratings.movieid;

select users.name, movies.title
from users, movies, ratings 
where users.name='David' 
	and users.userid=ratings.userid 
	and movies.movieid=ratings.movieid;

select users.name, avg(ratings.rating) 
from users, ratings
	where users.userid=ratings.userid 
	group by users.name;
switch back to postgres db and delete the test db

\c postgres
drop database test
\l

Connect to Postgres in Python
=============================
We use psycopg2 package to connect to Postgres in python. It has been installed in the vm.
1 . Go to python shell and connect to the database named with “postgres”. Create a test database:

# python shell
python
# connect to Postgres
import psycopg2
con = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='1234'")
con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cur = con.cursor()
cur.execute("create database test")
# disconnect from Postgres
cur.close()
con.close()
2 . Connect to the test database and build the same table:

con = psycopg2.connect("dbname='test' user='postgres' host='localhost' password='1234'")
con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cur = con.cursor()
cur.execute("create table ratings (userid integer, movieid integer, rating float)")

insert_command="insert into ratings VALUES (1, 122, 5.0);"
insert_command+="insert into ratings VALUES (1, 185, 4.5);"
insert_command+="insert into ratings VALUES (2, 231, 4.0);"
insert_command+="insert into ratings VALUES (2, 292, 3.5);"
insert_command+="insert into ratings VALUES (3, 316, 3.0);"
cur.execute(insert_command)
3 . Perform queries

cur.execute("select * from ratings")
res=cur.fetchall()
print res

cur.execute("select * from ratings where userid=1")
res=cur.fetchall()
print res

cur.execute("select * from ratings where rating>=3")
res=cur.fetchall()
print res

cur.close()
con.close()
4 . Drop the test database

con = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='1234'")
con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cur = con.cursor()
cur.execute("drop database test")
cur.close()
con.close()
