To run the DB2 client from command line execute the command as follows
goto the main BG project directory and execute

ant clean
ant compile
ant dbcompile-bg2

cd build/

;assuming the Db2client has been unzipped into the BG/db/db2 folder

java -cp bg.jar:../db/db2/lib/* com.yahoo.ycsb.Client -schema 
-db db2DataStore.DB2Client -p db.user=db2inst2  -p db.passwd=password 
-p db.url=jdbc:db2://localhost:50000/test -p db.driver=com.ibm.db2.jcc.DB2Driver