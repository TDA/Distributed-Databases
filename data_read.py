import os
import psycopg2
def Load_Ratings(file_path):
    file_path=os.path.abspath(file_path)
    
    file_name=os.path.basename(file_path)
    file_dir=os.path.dirname(file_path)
    os.chdir(file_dir)
    with open(file_name,'r') as file:
        with open('temp.dat','w') as file_temp:
            for each_line in file:
                (uid,mid,rat,ts)=each_line.split('::')
                file_temp.write(uid+","+mid+","+rat+"\n")

    conn = psycopg2.connect(database="testdb", user="postgres", password="user", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    query_string='COPY "ratings" ("UserID", "MovieID", "Rating") FROM \''+file_path+'\\temp.dat\' WITH (FORMAT CSV)'
    cur.execute(query_string)
    
    conn.commit()
    conn.close()
    print("success")

