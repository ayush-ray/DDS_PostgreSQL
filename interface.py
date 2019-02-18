import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):

    # Opened a cursor from already opened connection
    up_cursor_var = openconnection.cursor()

    # Opened the test file that has all the ratings record
    up_ratings_table = open(ratingsfilepath, 'r')

    # check for already existing tables with rating table name
    up_cursor_var.execute("DROP TABLE IF EXISTS " + ratingstablename)

    # query for creating new rating table
    up_query = "CREATE TABLE " + ratingstablename + \
               "(UserID int,          \
               temp_1 VARCHAR(1),     \
               MovieID int,           \
               temp_2 VARCHAR(10),    \
               Rating float,          \
               temp_3 VARCHAR(10),    \
               temp_4 VARCHAR(10))"
    up_cursor_var.execute(up_query)

    # Moving contents of temporary data file into newly created rating table
    up_cursor_var.copy_from(file=up_ratings_table, table=ratingstablename, sep=':',
                            columns=('UserID', 'temp_1', 'MovieID', 'temp_2', 'Rating', 'temp_3', 'temp_4'))

    # Removing useless columns as data file has :: as separator, but we can select one char as separator in Postgres
    up_cursor_var.execute("ALTER TABLE " + ratingstablename + " \
              DROP COLUMN temp_1, DROP COLUMN temp_2, DROP COLUMN temp_3, DROP COLUMN temp_4")

    # Committing changes into DB
    openconnection.commit()

    # Cleaning up
    up_ratings_table.close()
    up_cursor_var.close()


def rangepartition(ratingstablename, numberofpartitions, openconnection):

    # open Cursor
    up_cursor_var = openconnection.cursor()

    # finding range of rating for each partition
    up_rating_range = 5.0 / numberofpartitions
    up_lower_range = 0
    up_upper_range = up_rating_range

    # loop for number of partitions
    for i in range(0, numberofpartitions):

        # table created for number of partitions
        up_cursor_var.execute("DROP TABLE IF EXISTS range_part" + str(i))
        up_cursor_var.execute("CREATE TABLE range_part" + str(i) + "(UserID int, MovieID int, Rating float)")

        # inserting record from data table into appropriate horizontal fragment
        up_cursor_var.execute("INSERT INTO range_part" + str(i) + " (UserID, MovieID, Rating) SELECT * FROM " +
                              ratingstablename + " WHERE Rating" + ">=" + str(up_lower_range) + " AND Rating" + "<=" +
                              str(up_upper_range))

        # range updation for next partition
        up_lower_range += up_rating_range + 0.01
        up_upper_range += up_rating_range

    # new table for saving details about partition
    up_cursor_var.execute("DROP TABLE IF EXISTS PART_RANGE_INFO")
    up_cursor_var.execute("CREATE TABLE PART_RANGE_INFO (Rating_Range real, Number_Of_Partitions int)")
    up_cursor_var.execute("INSERT INTO PART_RANGE_INFO (Rating_Range, Number_Of_Partitions) VALUES (" +
                          str(up_rating_range) + ', ' + str(numberofpartitions) + ')')

    # commmitting changes into DB
    openconnection.commit()

    #Cleaning up
    up_cursor_var.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):

    up_cursor_var = openconnection.cursor()

    # loop for no of partitions
    for i in range(0, numberofpartitions):

        # tables created for no of partitions
        up_cursor_var.execute("DROP TABLE IF EXISTS rrobin_part" + str(i))
        up_query = "CREATE TABLE rrobin_part" + str(i) + "(UserID int, MovieID int, Rating float)"
        up_cursor_var.execute(up_query)

    # fetching records from rating table
    up_cursor_var.execute("SELECT * FROM " + ratingstablename)
    up_rating_table = up_cursor_var.fetchall()

    # temporary variable for keeping partition no. track
    up_j = 0

    # inserting into horizontal fragments in round-robin fashion
    for i in range(0, len(up_rating_table)):
        up_cursor_var.execute("INSERT INTO rrobin_part" + str(up_j) + " (UserID,MovieID,Rating) VALUES (%s,%s,%s)",
                              (up_rating_table[i][0], up_rating_table[i][1], up_rating_table[i][2]))
        up_j += 1
        if up_j >= numberofpartitions:
            up_j = 0

    # finding last partition into which data was inserted
    if up_j == 0:
        last_inserted_partition = numberofpartitions - 1
    else:
        last_inserted_partition = up_j - 1

    # Storing partition details into a new table for later use
    up_cursor_var.execute("DROP TABLE IF EXISTS PART_RROBIN_INFO")

    up_cursor_var.execute("CREATE TABLE PART_RROBIN_INFO (number_of_partitions int, last_inserted_partition int)")

    up_cursor_var.execute("INSERT INTO PART_RROBIN_INFO (number_of_partitions, last_inserted_partition) VALUES (%s, %s)"
                          , (str(numberofpartitions), str(last_inserted_partition)))

    # changes to DB
    openconnection.commit()

    # Cleaning up
    up_cursor_var.close()

    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):

    up_cursor_var = openconnection.cursor()

    # next 6 lines are to ensure that the same record is not inserted twice, this is due to a condition present in
    # testHelper.testrangerobin function
    up_cursor_var.execute(
        'SELECT COUNT(*) FROM {0} WHERE UserID = {1} AND MovieID = {2} AND Rating = {3}'.format(ratingstablename, userid
                                                                                                , itemid, rating))
    up_count = int(up_cursor_var.fetchone()[0])
    if up_count != 0:
        return

    # inserting record in ratings table
    up_cursor_var.execute("INSERT INTO " + ratingstablename + " VALUES(" +
                          str(userid) + "," + str(itemid) + "," + str(rating) + ")")

    # finding appropriate fragment in which record is to be inserted
    up_cursor_var.execute("SELECT * FROM PART_RROBIN_INFO")
    up_rrobin_info = up_cursor_var.fetchall()
    no_of_partitions = up_rrobin_info[0][0]
    last_inserted_partition = up_rrobin_info[0][1]
    if last_inserted_partition == no_of_partitions - 1:
        up_partition = 0
    else:
        up_partition = (last_inserted_partition + 1)

    # inserting record into appropriate fragment
    up_cursor_var.execute(
        "INSERT INTO rrobin_part" + str(up_partition) + " VALUES (" +
        str(userid) + "," + str(itemid) + "," + str(rating) + ")")

    # updating last inserted partition info into table
    up_cursor_var.execute("UPDATE PART_RROBIN_INFO SET last_inserted_partition = "+str(up_partition) +
                          " WHERE number_of_partitions = " + str(no_of_partitions))

    # committing to db
    openconnection.commit()

    # Cleaning up
    up_cursor_var.close()
    pass

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    up_cursor_var = openconnection.cursor()

    # next 6 lines are to ensure that the same record is not inserted twice, this is due to a condition present in
    # testHelper.testrangerobin function
    up_cursor_var.execute(
        'SELECT COUNT(*) FROM {0} WHERE UserID = {1} AND MovieID = {2} AND Rating = {3}'.format(ratingstablename, userid
                                                                                                , itemid, rating))
    up_count = int(up_cursor_var.fetchone()[0])
    if up_count != 0:
        return

    # inserting record to ratings table
    up_cursor_var.execute("INSERT INTO " + ratingstablename + " VALUES(" +
                          str(userid) + "," + str(itemid) + "," + str(rating) + ")")

    # fetching partition details
    up_cursor_var.execute("SELECT * FROM PART_RANGE_INFO")
    up_range_info = up_cursor_var.fetchall()
    rating_range = up_range_info[0][0]
    lower_range = 0
    up_partition = 0

    # finding appropriate fragment for record insertion
    while lower_range+rating_range < rating:
        lower_range += rating_range
        up_partition += 1

    # inserting into correct fragment
    up_cursor_var.execute("INSERT INTO range_part" + str(up_partition) + " VALUES (" +
                          str(userid) + "," + str(itemid) + "," + str(rating) + ")")

    # committing to db
    openconnection.commit()

    # Cleaning up
    up_cursor_var.close()
    pass

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
