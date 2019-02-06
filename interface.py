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


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    #sets a upper bound for each range
    rating_range = 5.0 / numberofpartitions

    range = rating_range

    partitions = numberofpartitions

    count = 0

    cursor = openconnection.cursor()

    i = 0
    while i < numberofpartitions:

        #create as many range partitions as are the number of partitions
        cursor.execute('DROP TABLE IF EXISTS range_part' + str(i))
        query4 = "CREATE TABLE range_part" + str(i) + "(UserID int, MovieID int, Rating float)"

        cursor.execute(query4)

        if i==0:
            query6 ="INSERT INTO range_part" + str(i) + "(UserID, MovieID, Rating)" + "SELECT * FROM " + ratingstablename + " WHERE Rating" + "<=" + str(rating_range)+ " AND Rating" + ">=" + str(count)
            cursor.execute(query6)

        else:
            query6 = "INSERT INTO range_part" + str(i) + "(UserID, MovieID, Rating)" + "SELECT * FROM " + ratingstablename + " WHERE Rating" + "<=" + str(rating_range) + " AND Rating" + ">" + str(count)
            cursor.execute(query6)


        count = rating_range
        rating_range += range

        i += 1

    #stores the upper bound for range and the number of partitions to be used for range insertion
    cursor.execute('DROP TABLE IF EXISTS PART')
    cursor.execute('CREATE TABLE PART (RatingRange int, Numberofpartitions int)')
    cursor.execute('INSERT INTO PART (RatingRange, Numberofpartitions) VALUES (' + str(range) + ', ' + str(partitions) + ')')

    openconnection.commit()

    cursor.close()

    pass



# Function calls
create_db("trial_a1_1")
con = getopenconnection()
con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
loadratings("ratings", "/home/user/Downloads/test_data.dat", con)
