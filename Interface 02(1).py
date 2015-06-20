#!/usr/bin/python3.4

import psycopg2
import os

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    file_path=os.path.abspath(ratingsfilepath)
    file_name=os.path.basename(file_path)
    print("Reading "+file_name)
    file_dir=os.path.dirname(file_path)
    print("Creating and writing to temporary file in "+file_dir)
    os.chdir(file_dir)
    with open(file_name,'r') as file:
        with open('temp.dat','w') as file_temp:
            for each_line in file:
                (uid,mid,rat,ts)=each_line.split('::')
                file_temp.write(uid+","+mid+","+rat+"\n")

    conn = openconnection
    cur = conn.cursor()
    query='CREATE TABLE "'+ratingstablename+'" ("UserID" bigint, "MovieID" Integer, "Rating" double precision)'
    cur.execute(query)
    query_string='COPY "'+ratingstablename+'" ("UserID", "MovieID", "Rating") FROM \''+file_dir+'\\temp.dat\' WITH (FORMAT CSV)'
    cur.execute(query_string)    
    conn.commit()
    print("success, table created")



def rangepartition(ratingstablename, numberofpartitions, openconnection):
    N=numberofpartitions
    table_name=ratingstablename
    max_rating=5
    frag_width=round(max_rating/N,2)
    last_frag_start=round(frag_width*(N-1),2)

    print("range is ",frag_width)
    print("last frag is ",last_frag_start)
    conn = openconnection
    cur = conn.cursor()
    frag_start=0
    print('Created Tables:')
    while(frag_start<=last_frag_start):
        next_frag_start=round(frag_start+frag_width,2)
        if(frag_start==0):
            query_string='SELECT * FROM "'+table_name+'" WHERE "Rating">='+str(frag_start)+' AND '+'"Rating"<='+str(next_frag_start)+' ORDER BY "Rating","UserID","MovieID" ASC'
        else:
            query_string='SELECT * FROM "'+table_name+'" WHERE "Rating">'+str(frag_start)+' AND '+'"Rating"<='+str(next_frag_start)+' ORDER BY "Rating","UserID","MovieID" ASC'
        cur.execute(query_string)
        print ('from ',frag_start,' to ',round(frag_start+frag_width,2))
        rows=cur.fetchall()
        print('--------FRAGMENT CREATED-----------')
        print('--------INSERTING INTO FRAGMENT----------')
        
        new_table_name='table'+str(frag_start)+'to'+str(next_frag_start)
        print(new_table_name)
        cur.execute('DROP TABLE IF EXISTS "'+new_table_name+'"')
        create_table_string='CREATE TABLE "'+new_table_name+'"("UserID" bigint,  "MovieID" integer,  "Rating" double precision)'
        cur.execute(create_table_string)
        query = 'INSERT INTO "'+new_table_name+'" VALUES (%s, %s, %s)'
        cur.executemany(query,rows)
        print(str(cur.rowcount)+ " inserted")
        frag_start=next_frag_start
    print('--------FRAGMENTATION DONE-----------')
        
    cur.execute('DROP TABLE IF EXISTS "Range_Meta_Table"')
    create_meta_table='CREATE TABLE "Range_Meta_Table"("NumTables" integer,  "FragWidth" double precision  )'
    cur.execute(create_meta_table)
    meta_query= 'INSERT INTO "Range_Meta_Table" VALUES (%s, %s)'
    cur.execute(meta_query,(N,frag_width))
    print('--------META TABLE CREATED-----------')

    conn.commit()
    



def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    conn = openconnection
    table_name=ratingstablename
    N=numberofpartitions
    cur = conn.cursor()
    query_string='SELECT * FROM "'+table_name+'"'
    cur.execute(query_string)
    rows=cur.fetchall()
    no_rows=cur.rowcount
    next_insert=1
    print('--------FETCH OVER-------')
    for x in range(1,N+1):
        new_table_name='rr_table'+str(x)
        print(new_table_name)
        cur.execute('DROP TABLE IF EXISTS "'+new_table_name+'"')
        create_table_string='CREATE TABLE "'+new_table_name+'"("UserID" bigint,  "MovieID" integer,  "Rating" double precision)'
        cur.execute(create_table_string)
    print('----TABLES CREATED----')
    for next_insert in range(1,N+1):
        with open('rr_temp'+str(next_insert),'w') as file:
            for x in range(next_insert-1,no_rows,N):
                file.write(str(rows[x][0])+','+str(rows[x][1])+','+str(rows[x][2])+'\n')
    print('----TEMP FILES CREATED-----')
    
    for next_insert in range(1,N+1):
        query_string='COPY "rr_table'+str(next_insert)+'" ("UserID", "MovieID", "Rating") FROM \''+os.getcwd()+'\\rr_temp'+str(next_insert)+'\' WITH (FORMAT CSV)'
        cur.execute(query_string)
    print('-----INSERTS DONE-----')
    
    final_insert=(no_rows%N)    
    cur.execute('DROP TABLE IF EXISTS "RoundRobin_Meta_Table"')
    create_meta_table='CREATE TABLE "RoundRobin_Meta_Table"("NumTables" integer,  "NextInsert" integer  )'
    cur.execute(create_meta_table)
    meta_query= 'INSERT INTO "RoundRobin_Meta_Table" VALUES (%s, %s)'
    cur.execute(meta_query,(N,final_insert+1))
    
        
    conn.commit()
    print("Partition success")




def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    uid=userid
    mid=itemid
    table_name=ratingstablename
    max_rating=5
    if(rating<0 or rating>max_rating):
        print("Invalid rating")
        exit()
    conn = openconnection
    cur = conn.cursor()
    query_string='SELECT * FROM "RoundRobin_Meta_Table"'
    cur.execute(query_string)    
    row=cur.fetchall();
    
    
    next_insert=row[0][1]
    no_tables=row[0][0]
    print("No of tables is ",no_tables)
    print("Next Insert is ",next_insert)

        
    new_table_name='rr_table'+str(next_insert)
    print("Table to be inserted into is ",new_table_name)
    query = 'INSERT INTO "'+new_table_name+'" VALUES (%s, %s, %s)'
    cur.execute(query,(uid,mid,rating))
    next_insert=(next_insert+1)
    if(next_insert>no_tables):
        next_insert=1
    print(next_insert)
    update_query='UPDATE "RoundRobin_Meta_Table" SET "NextInsert"='+str(next_insert)
    cur.execute(update_query)
    print(update_query)
    print('-----------------DONE------------------')
        
    conn.commit()
    print("Insert success")




def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    uid=userid
    mid=itemid
    max_rating=5
    if(rating<0 or rating>max_rating):
        print("Invalid rating")
        exit()
    conn = openconnection
    cur = conn.cursor()
    query_string='SELECT * FROM "Range_Meta_Table"'
    cur.execute(query_string)    
    row=cur.fetchall();
    
    frag_start=0
    frag_width=row[0][1]
    no_tables=row[0][0]
    last_frag_start=round(frag_width*(no_tables-1),2)
    print("No of tables is ",no_tables)
    print("Range is ",frag_width)
    print("Last frag is ",last_frag_start)
    if(rating==0):
        frag_start=0
        next_frag_start=round(frag_start+frag_width,2)
    else:
        while(frag_start<=last_frag_start):
            next_frag_start=round(frag_start+frag_width,2)
            if(rating>frag_start and rating<=next_frag_start):
                break;
            frag_start=next_frag_start
        
    new_table_name='table'+str(frag_start)+'to'+str(next_frag_start)
    print("Table to be inserted into is ",new_table_name)
    query = 'INSERT INTO "'+new_table_name+'" VALUES (%s, %s, %s)'
    cur.execute(query,(uid,mid,rating))
    print('-----------------DONE------------------')
    conn.commit()
    print("Insert successful")


def deletepartitions(table_name, openconnection):
    conn = openconnection
    cur = conn.cursor()
    check='SELECT table_name FROM information_schema.tables WHERE table_schema=\'public\' ORDER BY table_name'
    cur.execute(check)
    rows=cur.fetchall()
    rr=0
    rng=0
    for row in rows:
        if("RoundRobin_Meta_Table" in row):
            rr=1
        if("Range_Meta_Table" in row):
            rng=1
        
    if(rr==1):
        print("round robin partition found")
        query_string='SELECT * FROM "RoundRobin_Meta_Table"'
        cur.execute(query_string)    
        row=cur.fetchall()
        no_tables=row[0][0]
        rows=[]
        print("No of tables to be deleted is ",str(no_tables))
       
        for i in range(1,no_tables+1):
            print(i)
            rows.append("rr_table"+str(i))
        for row in rows:
            query = 'DROP TABLE IF EXISTS "'+row+'"'
            cur.execute(query)
            print(row + " deleted")
        query = 'DROP TABLE IF EXISTS "RoundRobin_Meta_Table"'
        cur.execute(query)
        print("Round robin partition has been removed")
    
    else:
        print("Round robin partition not found")
    
        
    if(rng==1):
        print("range partition found")
        query_string='SELECT * FROM "Range_Meta_Table"'
        cur.execute(query_string)    
        row=cur.fetchall();
        max_rating=5
        frag_start=0
        frag_width=row[0][1]
        no_tables=row[0][0]
        last_frag_start=round(frag_width*(no_tables-1),2)
        print("No of tables is ",no_tables)
        print("Range is ",frag_width)
        
        while(frag_start<=last_frag_start):
            next_frag_start=round(frag_start+frag_width,2)
            new_table_name='table'+str(frag_start)+'to'+str(next_frag_start)
            print(new_table_name + " deleted ")
            cur.execute('DROP TABLE IF EXISTS "'+new_table_name+'"')
            frag_start=next_frag_start
        query = 'DROP TABLE IF EXISTS "Range_Meta_Table"'
        cur.execute(query)
        print("Range partition has been removed")
    
    
        
    else:
        print("range tables not found")
        
    conn.commit()
    

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
        print ('A database named '+str(dbname)+' already exists')

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
            loadratings('iptable',r'ratings1.dat', con)
            #rangepartition('tings3',9,con)
            #rangeinsert('ratings',12,3,3.4,con)
            #roundrobinpartition('tings3',6,con)
            #roundrobininsert('ratings',1,2,3,con)
            #deletepartitions('tings2',con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print ("OOPS! This is the error ==> ", detail)
