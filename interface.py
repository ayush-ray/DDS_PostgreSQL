#!/usr/bin/python2.7

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    ratings = open(ratingsfilepath, 'r')

    # Creates ratings table
    cur.execute("DROP TABLE IF EXISTS " + ratingstablename)
    query = "CREATE TABLE " + ratingstablename + \
            "(UserID int,          \
            temp_1 VARCHAR(1),     \
            MovieID int,           \
            temp_2 VARCHAR(10),    \
            Rating float,          \
            temp_3 VARCHAR(10),    \
            temp_4 VARCHAR(10))"
    cur.execute(query)
    cur.copy_from(file=ratings, table=ratingstablename, sep=':',
                  columns=('UserID', 'temp_1', 'MovieID', 'temp_2', 'Rating', 'temp_3', 'temp_4'))
    cur.execute("ALTER TABLE " + ratingstablename + " \
              DROP COLUMN temp_1, DROP COLUMN temp_2, DROP COLUMN temp_3, DROP COLUMN temp_4")

    # Prints contents of ratings table
    cur.execute("SELECT * FROM " + ratingstablename)
    print(cur.fetchall())

    openconnection.commit()

    # Clean Up
    ratings.close()
    cur.close()
    pass


# Function calls
create_db("trial_a1_1")
con = getopenconnection()
con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
loadratings("ratings", "/home/user/Downloads/test_data.dat", con)
