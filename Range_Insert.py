import psycopg2
def Range_Insert(table_name,uid,mid,rating):
    max_rating=5
    if(rating<0 or rating>max_rating):
        print("Invalid rating")
        exit()
    conn = psycopg2.connect(database="testdb", user="postgres", password="user", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    query_string='SELECT * FROM "Range_Meta_Table"'
    cur.execute(query_string)    
    row=cur.fetchall();
    
    frag_start=0
    frag_width=row[0][1]
    no_tables=row[0][0]
    last_frag_start=max_rating-frag_width
    print("No of tables is ",no_tables)
    print("Range is ",frag_width)
    print("Last frag is ",last_frag_start)
    if(rating==0):
        frag_start=0
        next_frag_start=frag_start+frag_width
    else:
        while(frag_start<=last_frag_start):
            next_frag_start=frag_start+frag_width
            if(rating>frag_start and rating<=next_frag_start):
                break;
            frag_start=next_frag_start
        
    new_table_name='table'+str(frag_start)+'to'+str(next_frag_start)
    print("Table to be inserted into is ",new_table_name)
    query = 'INSERT INTO "'+new_table_name+'" VALUES (%s, %s, %s)'
    cur.execute(query,(uid,mid,rating))
    print('-----------------DONE------------------')
        
    conn.commit()
    conn.close()
    print("success")

