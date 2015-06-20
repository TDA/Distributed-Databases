#!/usr/bin/python2.7
#
# Interface for assignement 2
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def rangequery(ratingstablename,minval,max_rating,con):
    table=ratingstablename
    rating=minval
    range_table=False
    rr_table=False
    if(rating<0 or rating>5):
        print("Invalid rating")
        exit()
    cur=con.cursor()
    cur.execute("select * from information_schema.tables where table_name='Range_Meta_Table'")
    range_table=(bool(cur.rowcount))
    cur.execute("select * from information_schema.tables where table_name='RoundRobin_Meta_Table'")
    rr_table=(bool(cur.rowcount))
    if(range_table):
        printRangeTables(con,minval,max_rating)
    elif(rr_table):
        printRRTables(con,minval,True,max_rating)
    else:
        print('No partitions found')
        exit()


def pointquery(ratingstablename,minval,con):
    max_rating=5
    rating=minval
    table=ratingstablename
    range_table=False
    rr_table=False
    if(rating<0 or rating>5):
        print("Invalid rating")
        exit()
    cur=con.cursor()
    cur.execute("select * from information_schema.tables where table_name='Range_Meta_Table'")
    range_table=(bool(cur.rowcount))
    cur.execute("select * from information_schema.tables where table_name='RoundRobin_Meta_Table'")
    rr_table=(bool(cur.rowcount))
    
    if(range_table):
        printPointRangeTable(con,minval)
    elif(rr_table):
        printRRTables(con,minval,False,max_rating)
    else:
        print('No partitions found')
        exit()



        
def printRangeTables(con,rating,max_rating):
    tables=getRangeTable(con,rating,max_rating)
    for table in tables:
        print("Table to be queried is ",table)
    rows=[]
    with open('RangeQueryOut.txt','w') as op:
        for table in tables:
            query='SELECT * FROM "'+table+'" where "Rating">='+str(rating)+' and "Rating"<='+str(max_rating)+' ORDER BY "Rating","UserID","MovieID" ASC'
            print(query)
            cur=con.cursor()
            cur.execute(query)
            rows=cur.fetchall()
            for row in rows:
                print(table,row[0],row[1],row[2],sep='--',end='\n',file=op)
            #print(rows)

def printRRTables(con,rating,range,max_rating):
    tables=getAllTables(con)
    if(range==True):
        with open('RangeQueryOut.txt','w') as op:
            for table in tables:
                query_string='SELECT * FROM "'+table+'" where "Rating">='+str(rating)+' and "Rating"<='+str(max_rating)+' ORDER BY "Rating","UserID","MovieID" ASC'
                print(query_string)
                cur=con.cursor()
                cur.execute(query_string)
                rows=cur.fetchall()
                for row in rows:
                    print(table,row[0],row[1],row[2],sep='--',end='\n',file=op)
    else:
        with open('PointQueryOut.txt','w') as op:
            for table in tables:
                query_string='SELECT * FROM "'+table+'" where "Rating"='+str(rating)+' ORDER BY "Rating","UserID","MovieID" ASC'
                print(query_string)
                cur=con.cursor()
                cur.execute(query_string)
                rows=cur.fetchall()
                for row in rows:
                    print(table,row[0],row[1],row[2],sep='--',end='\n',file=op)


def getAllTables(con):
    query_string='SELECT * FROM "RoundRobin_Meta_Table"'
    cur=con.cursor()
    cur.execute(query_string)
    row=cur.fetchall();
    N=row[0][0]
    tables=[]
    for x in range(1,N+1):
        new_table_name='rr_table'+str(x)
        print(new_table_name)
        tables.append(new_table_name)
    return tables
    
    
def printPointRangeTable(con,rating):
    tables=getRangeTable(con,rating,5)
    start_table=tables[0]
    print("Table to be queried is ",start_table)
    rows=[]
    with open('PointQueryOut.txt','w') as op:
        query='SELECT * FROM "'+start_table+'" where "Rating"='+str(rating)+' ORDER BY "Rating","UserID","MovieID" ASC'
        print(query)
        cur=con.cursor()
        cur.execute(query)
        rows=cur.fetchall()
        for row in rows:
            print(start_table,row[0],row[1],row[2],sep='--',end='\n',file=op)
    

def getRangeTable(con,rating,max_rating):
    
    query_string='SELECT * FROM "Range_Meta_Table"'
    cur=con.cursor()
    cur.execute(query_string)
    row=cur.fetchall();
    frag_start=0
    no_tables=row[0][0]
    frag_width=row[0][1]
    print(no_tables,frag_width)
    tables=[]
    last_frag_start=round(frag_width*(no_tables-1),2)
    print("No of tables is ",no_tables)
    print("Range is ",frag_width)
    print("Last frag is ",last_frag_start)
    if(rating==0):
        frag_start=0
        next_frag_start=round(frag_start+frag_width,2)
    else:
        while(frag_start<=last_frag_start and frag_start<=max_rating):
            next_frag_start=round(frag_start+frag_width,2)
            if(rating>frag_start and rating<=next_frag_start):
                break;
            frag_start=next_frag_start
            
        while(frag_start<=last_frag_start and frag_start<=max_rating):
            next_frag_start=round(frag_start+frag_width,2)    
            new_table_name='table'+str(frag_start)+'to'+str(next_frag_start)
            #print("Table to be queried is ",new_table_name)
            tables.append(new_table_name)
            frag_start=next_frag_start
        return tables






def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
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
        print ('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            rangequery('tings3', 2, 4, con)
            pointquery('tings3',3,con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print ("OOPS! This is the error ==> ", detail)
