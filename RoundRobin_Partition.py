import psycopg2
import os
def RoundRobin_Partition(table_name,N):
    conn = psycopg2.connect(database="testdb", user="postgres", password="user", host="127.0.0.1", port="5432")
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
    conn.close()
    print("success")

