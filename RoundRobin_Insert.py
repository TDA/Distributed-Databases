import psycopg2
def RoundRobin_Insert(table_name,uid,mid,rating):
    max_rating=5
    if(rating<0 or rating>max_rating):
        print("Invalid rating")
        exit()
    conn = psycopg2.connect(database="testdb", user="postgres", password="user", host="127.0.0.1", port="5432")
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
    conn.close()
    print("success")

