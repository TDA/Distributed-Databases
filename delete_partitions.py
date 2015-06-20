import psycopg2
def Delete_partitions():
    conn = psycopg2.connect(database="testdb", user="postgres", password="user", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    check='SELECT table_name FROM information_schema.tables WHERE table_schema=\'public\' ORDER BY table_name'
    cur.execute(check)
    rows=cur.fetchall()
    rr=0
    rng=0
    for row in rows:
        #print(row)
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
        last_frag_start=max_rating-frag_width
        print("No of tables is ",no_tables)
        print("Range is ",frag_width)
        
        while(frag_start<=last_frag_start):
            next_frag_start=frag_start+frag_width
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
    conn.close()
     
        
