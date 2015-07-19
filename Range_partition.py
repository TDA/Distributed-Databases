import psycopg2
def Range_Partition(table_name,N):
    max_rating=5
    frag_width=max_rating/N
    last_frag_start=max_rating-frag_width

    print("range is ",frag_width)
    print("last frag is ",last_frag_start)

    conn = psycopg2.connect(database="testdb", user="postgres", password="user", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    frag_start=0
    print('Created Tables:')
    while(frag_start<=last_frag_start):
        next_frag_start=frag_start+frag_width
        if(frag_start==0):
            query_string='SELECT * FROM "'+table_name+'" WHERE "Rating">='+str(frag_start)+' AND '+'"Rating"<='+str(next_frag_start)+' ORDER BY "Rating","UserID","MovieID" ASC'
        else:
            query_string='SELECT * FROM "'+table_name+'" WHERE "Rating">'+str(frag_start)+' AND '+'"Rating"<='+str(next_frag_start)+' ORDER BY "Rating","UserID","MovieID" ASC'
        cur.execute(query_string)
        print ('from ',frag_start,' to ',frag_start+frag_width)
        rows=cur.fetchall()
        #for row in rows:
            #print(row)
        print('-----------------DONE------------------')
        new_table_name='table'+str(frag_start)+'to'+str(next_frag_start)
        print(new_table_name)
        cur.execute('DROP TABLE IF EXISTS "'+new_table_name+'"')
        create_table_string='CREATE TABLE "'+new_table_name+'"("UserID" bigint,  "MovieID" integer,  "Rating" double precision)'
        cur.execute(create_table_string)
        query = 'INSERT INTO "'+new_table_name+'" VALUES (%s, %s, %s)'
        cur.executemany(query,rows)
        print(str(cur.rowcount)+ " inserted")
        frag_start=next_frag_start
        
    cur.execute('DROP TABLE IF EXISTS "Range_Meta_Table"')
    create_meta_table='CREATE TABLE "Range_Meta_Table"("NumTables" integer,  "FragWidth" double precision  )'
    cur.execute(create_meta_table)
    meta_query= 'INSERT INTO "Range_Meta_Table" VALUES (%s, %s)'
    cur.execute(meta_query,(N,frag_width))
    
        
    conn.commit()
    conn.close()
    print("success")

