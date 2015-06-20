The loadratings takes in the ratings file and parses it to create a temp file 
that contains only the userid,itemid, and rating. This file is then copied using 
the COPY statement in postgres through python. This is relatively faster than 
Inserting each row through execute(or executemany).

The partitions are then created by Range/Roundrobin techniques and loaded into the db.
We also create meta tables to hold details about partitions.(no of tables and next insertion)
The Insertions work similarly by querying the meta tables and inserting appropriately.

Deletepartitions deletes all tables that were created by us.

Kindly note that the code has been tested only on windows 7 with 32bit Python3.4. 
I have added the shebang to the file but have not tested on linux due to time constraints.

