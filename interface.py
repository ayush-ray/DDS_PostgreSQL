#!/usr/bin/python2.7

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def create_db(dbname):
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()


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
    cur = openconnection.cursor()
    rating_range = 5.0 / numberofpartitions
    current = 0
    next = rating_range

    for i in range(0, numberofpartitions):
	
		# create as many range partitions as are the number of partitions
        cur.execute("DROP TABLE IF EXISTS range_part" + str(i))
        cur.execute("CREATE TABLE range_part" + str(i) + "(UserID int, MovieID int, Rating float)")
        cur.execute("INSERT INTO range_part" + str(i) + " (UserID, MovieID, Rating) SELECT * FROM " +\
                ratingstablename + " WHERE Rating" + ">=" + str(current) + " AND Rating" + "<=" + str(next))

        # cur.execute("select * from range_part" + str(i))
        # print(cur.fetchall())

        current += rating_range + 0.01
        next += rating_range

    # stores the upper bound for range and the number of partitions to be used for range insertion
    cur.execute("DROP TABLE IF EXISTS RANGE_PARTITION_INFO")
    cur.execute("CREATE TABLE RANGE_PARTITION_INFO (Rating_Range real, Number_Of_Partitions int)")
    cur.execute(
        "INSERT INTO RANGE_PARTITION_INFO (Rating_Range, Number_Of_Partitions) VALUES (" +
        str(rating_range) + ', ' + str(numberofpartitions) + ')')

    openconnection.commit()
    cur.close()
    pass

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
	cur = openconnection.cursor()

	up_cursor_var.execute(
        'SELECT COUNT(*) FROM {0} WHERE UserID = {1} AND MovieID = {2} AND Rating = {3}'.format(ratingstablename, userid
                                                                                                , itemid, rating))
    up_count = int(up_cursor_var.fetchone()[0])
    if up_count != 0:
        return

    cur.execute("INSERT INTO " + ratingstablename + " VALUES(" +
                str(userid) + "," + str(itemid) + "," + str(rating) + ")")

    cur.execute("SELECT * FROM RANGE_PARTITION_INFO")
    range_table = cur.fetchall()
	
	rating_range = range_table[0][0]
    lower_range = 0
    partition = 0

    while lower_range+rating_range < rating:
        lower_range += rating_range
        partition += 1

    cur.execute("INSERT INTO range_part" + str(partition) + " VALUES (" +
                str(userid) + "," + str(itemid) + "," + str(rating) + ")")

    openconnection.commit()
    cur.close()
    pass


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()

    for i in range(0, numberofpartitions):
        cur.execute("DROP TABLE IF EXISTS rrobin_part" + str(i))
        query = "CREATE TABLE rrobin_part" + str(i) + "(UserID int, MovieID int, Rating float)"
        cur.execute(query)

    cur.execute("SELECT * FROM " + ratingstablename)
    rating_table = cur.fetchall()

    j = 0

    for i in range(0, len(rating_table)):
        cur.execute("INSERT INTO rrobin_part" + str(j) + " (UserID,MovieID,Rating) VALUES (%s,%s,%s)",
                    (rating_table[i][0], rating_table[i][1], rating_table[i][2]))
        j += 1
        if j >= numberofpartitions:
            j = 0

    if j == 0:
        last_inserted_partition = numberofpartitions - 1
    else:
        last_inserted_partition = j - 1

    cur.execute("DROP TABLE IF EXISTS PART_RROBIN_INFO")
    cur.execute("CREATE TABLE PART_RROBIN_INFO (number_of_partitions int, last_inserted_partition int)")
    cur.execute("INSERT INTO PART_RROBIN_INFO (number_of_partitions, last_inserted_partition) VALUES (%s, %s)",
                (str(numberofpartitions), str(last_inserted_partition)))

    openconnection.commit()
    cur.close()

    pass


# Function calls
create_db("trial_a1_1")
con = getopenconnection()
con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
loadratings("ratings", "/home/user/Downloads/test_data.dat", con)
rangepartition("ratings", 3, con)
