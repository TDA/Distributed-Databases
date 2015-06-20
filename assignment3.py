import psycopg2
import os
import threading

DATABASE_NAME = 'dds_assgn1'
glob_count=0
#create the thread class that we can use for the parallelism
class myThread (threading.Thread):
    def __init__(self, threadID, startValue, endValue, columnName, inputTable,outputTable):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.startValue = startValue
        self.endValue = endValue
        self.columnName = columnName
        self.counter = 0
        self.tuples = []
        self.orderedTuples = []
        self.inputTable= inputTable
        self.outputTable = outputTable
    def run(self):
        print("Starting " + self.name)
        # Get lock to synchronize threads
        self.tuples=retrieveRecords(self.columnName,self.startValue,self.endValue, self.inputTable, conn)
        self.counter=len(self.tuples)
        #this part should be synchronized to have the proper numbers in the tupleorder
        threadLock.acquire()
        #print_time(self.name, self.counter, 3)
        global glob_count
        for t in self.tuples:
            glob_count+=1
            count=(glob_count)
            temp=(t,count)
            self.orderedTuples.append(temp)
        # Free lock to release next thread
        threadLock.release()
        print(str(self.name) + "value is "+ str(self.counter))
        storeRecords(self.name,self.orderedTuples,self.outputTable,conn)

def getopenconnection(user='postgres', password='1234', dbname='dds_assgn3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

#retrieve the data in parallel
def retrieveRecords(colName,startValue,endValue, inputTable, conn=getopenconnection()):
    #print(colName,startValue,endValue,conn)
    cur=conn.cursor()
    query='SELECT * FROM "'+str(inputTable)+'" WHERE "'+str(colName)+'" >= '+str(startValue)+' AND "'+str(colName)+'" < '+str(endValue) + ' ORDER BY "'+str(colName) + '" ASC'
    #print(query)
    cur.execute(query)
    rows=cur.fetchall()
    return rows

#store the data back in parallel, adding an additional column.
def storeRecords(name,tuples,outputTable,conn=getopenconnection()):
    for t in tuples:
        temp=(t[0][0],t[0][1],t[0][2],t[1])
        #print(temp)
        cur=conn.cursor()
        query= 'INSERT INTO "%s" VALUES%s'%((str(outputTable),str(temp)))
        print(query)
        cur.execute(query)
        conn.commit()
        #print(name)

def createOutputTable(tableName):
    query='''CREATE TABLE IF NOT EXISTS "%s" (UserID bigint NOT NULL,
       MovieID INTEGER NOT NULL,
       Rating REAL NOT NULL ,
       TupleOrder INTEGER NOT  NULL )'''
    conn=getopenconnection()
    cur=conn.cursor()
    cur.execute(query%(str(tableName)))
    conn.commit()
    conn.close()

def ParallelSort(inputTable,columnName,outputTable,conn):
    createOutputTable(outputTable)
    #create the threads
    query='''SELECT MAX("%s") FROM %s'''%(columnName,inputTable)
    cur=conn.cursor()
    cur.execute(query)
    maxValue=cur.fetchall()
    print(maxValue[0][0])
    rangeVal=maxValue[0][0]/5
    threads=[]
    for i in range(0,4):
        thread=myThread(i+1,i*rangeVal , (i+1)*rangeVal, columnName, inputTable,outputTable)
        thread.start()
        threads.append(thread)
    thread5 = myThread(5, 4*rangeVal,maxValue[0][0]+1 , columnName, inputTable,outputTable)
    thread5.start()
    threads.append(thread5)
    for t in threads:
        t.join()
    print ("Exiting Main Thread")



class myJoinThread (threading.Thread):
    def __init__(self, threadID, startValue, endValue, columnName1, columnName2, inputTable1,inputTable2,outputTable,listofcolumns,listofcolpos):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.startValue = startValue
        self.endValue = endValue
        self.columnName1 = columnName1
        self.columnName2 = columnName2
        self.listofcolumns = listofcolumns
        self.listofcolpos = listofcolpos
        self.tuples1 = []
        self.tuples2 = []
        self.orderedTuples = []
        self.inputTable1= inputTable1
        self.inputTable2= inputTable2
        self.outputTable = outputTable
    def run(self):
        conn=getopenconnection()
        cur=conn.cursor()
        print("Starting " + self.name)
        # Get lock to synchronize threads
        self.tuples1=retrieveRecords(self.columnName1,self.startValue,self.endValue, self.inputTable1, conn)
        self.tuples2=retrieveRecords(self.columnName2,self.startValue,self.endValue, self.inputTable2, conn)
        pos1=getPos(self.columnName1,self.inputTable1)
        pos2=getPos(self.columnName2,self.inputTable2)
        #print(pos1,pos2)
        positionsFirstTable=self.listofcolpos[0]
        positionsSecondTable=self.listofcolpos[1]
        #print(positionsFirstTable)
        #print(positionsSecondTable)
        string=""
        newTuple=()
        ListTuples=[]
        if(len(self.tuples2)>0 and len(self.tuples1)>0):
            for s in self.tuples1:
                for t in self.tuples2:
                    #print(t)
                    if s[pos1-1] in t:
                        newTuple=tuple(s[x-1] for x in positionsFirstTable)
                        newTuple2=tuple(t[y-1] for y in positionsSecondTable)
                        newTuple3=newTuple+newTuple2
                        ListTuples.append(newTuple3)
                            #string+=(str(s[x-1]))
                        #for y in positionsSecondTable:
                         #   string+=(str(t[y-1]))
        if(len(ListTuples)>0):
            #print(ListTuples,"\n")
            for tuple2 in ListTuples:
                query='''INSERT INTO "%s" VALUES%s'''%(self.outputTable,tuple2)
                print(query)
                cur.execute(query)
                conn.commit()
                        #print("yess")
                    #if(s[pos1-1] == t[pos2]-1):
                     #   print("found")
                    #else:
                     #   print("not found")
        print(str(self.name))


def getPos(columnName,tableName):
    query='''SELECT * FROM information_schema.columns WHERE table_name = '%s' AND COLUMN_NAME = '%s' '''%(tableName,columnName)
    conn=getopenconnection()
    cur=conn.cursor()
    cur.execute(query)
    rows=cur.fetchall()
    for row in rows:
        #print(row)
        columnPos=row[4]

    return columnPos

def createJoinOutputTable(iptable1,iptable2,optable,conn):
    #This function creates a new table combining the schema of the existing two tables(removing common columns).
    cur=conn.cursor()
    query='''SELECT * FROM information_schema.columns WHERE table_name = '%s' '''%iptable1
    newList=[]
    newColumnList=[]
    colPos=[]
    colPos2=[]
    cur.execute(query)
    rows=cur.fetchall()

    for row in rows:
        #print(row)
        columnName=row[3]
        columnType=row[7]
        columnPos=row[4]
        newList.append(str(columnName+' '+columnType))
        newColumnList.append(columnName)
        colPos.append(columnPos)
    query='''SELECT * FROM information_schema.columns WHERE table_name = '%s' '''%iptable2
    cur.execute(query)
    rows=cur.fetchall()
    for row in rows:
        columnName=row[3]
        columnType=row[7]
        columnPos=row[4]
        if(str(columnName+' '+columnType) not in newList):
            newList.append(str(columnName+' '+columnType))
            newColumnList.append(columnName)
            colPos2.append(columnPos)
    newColumnPos=[colPos,colPos2]
    str1=""
    str1=', \n'.join(newList)

    tableCreateQuery='''CREATE TABLE IF NOT EXISTS  "%s" (%s) '''%(optable,str1)
    #print(tableCreateQuery)
    cur.execute(tableCreateQuery)
    conn.commit()
    return newColumnList,newColumnPos

def ParallelJoin(inputTable1,inputTable2,Table1JoinColumn,Table2JoinColumn,outputTable,conn):
    Columns=createJoinOutputTable(inputTable1,inputTable2,outputTable,conn)
    listOfColumns=Columns[0]
    listOfColPos=Columns[1]
    #create the threads
    print(listOfColumns)
    #print(listOfColPos)
    query='''SELECT MAX("%s") FROM %s'''%(Table1JoinColumn,inputTable1)
    cur=conn.cursor()
    cur.execute(query)
    maxValue1=cur.fetchall()
    #print(maxValue1[0][0])
    rangeVal1=maxValue1[0][0]
    #query='''SELECT MAX("%s") FROM %s'''%(Table2JoinColumn,inputTable2)
    #cur.execute(query)
    #maxValue2=cur.fetchall()
    #print(maxValue2[0][0])
    #rangeVal2=maxValue2[0][0]
    threads=[]
    for i in range(0,4):
        thread=myJoinThread(i+1,i*rangeVal1 , (i+1)*rangeVal1, Table1JoinColumn,Table2JoinColumn,inputTable1, inputTable2,outputTable,listOfColumns,listOfColPos)
        thread.start()
        threads.append(thread)
    thread5 = myJoinThread(5, 4*rangeVal1,maxValue1[0][0]+1 , Table1JoinColumn,Table2JoinColumn,inputTable1, inputTable2,outputTable,listOfColumns,listOfColPos)
    thread5.start()
    threads.append(thread5)
    for t in threads:
        t.join()
    print ("Exiting Main Thread")

conn=getopenconnection()
threadLock = threading.Lock()
#ParallelSort("iptable","UserID","optable",getopenconnection())
#ParallelJoin("iptable","iptable2","UserID","UserID","optableJoin",getopenconnection())
