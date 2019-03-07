import psycopg2
import os
import sys


def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
	# opening cursor and output path
    cur = openconnection.cursor()
    var_file = open(outputPath, 'w+')

    # getting the count of ratings table created by Assignment1 in DB
    cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables where relname like 'rangeratingspart%'")
    metadata_range = cur.fetchall()
    number_of_partitions = metadata_range[0][0]
    for i in range(number_of_partitions):
        cur.execute("SELECT * FROM RangeRatingsPart" + str(i))
        rangepart_for_output = cur.fetchall()
        for j in rangepart_for_output:
            if j[2] <= ratingMaxValue and j[2] >= ratingMinValue:
                var_file.write("RangeRatingsPart" + str(i) + "," + str(j[0]) + "," + str(j[1]) + "," + str(j[2]) + "\n")
    

    var_file.close()
    cur.close()


def PointQuery(ratingValue, openconnection, outputPath):
	pass
