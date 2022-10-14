from cmath import log
import logging
import mysql.connector

class mySQLDB:
    def __init__(self, logger, sqlConn):
        self.logger = logger
        self.sqlConn = sqlConn

    def connect(self, username, password, host, db):
        try:
            cnx = mysql.connector.connect(user=username, password=password, host='localhost', database= db)
            self.sqlConn = cnx
            print(cnx)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            cnx.close()
    
    def createTable(self, tableName, tableDesc):
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

        cursor.close()
        cnx.close()


if __name__=='__main__':
    mydb = mySQLDB(None, None)
    mydb.connect('root', 'carol1999', 'localhost', 'messages')
    TABLES = {}
    TABLES['employees'] = (
        "CREATE TABLE 'message' ("
        "  'messageID' varchar(25) NOT NULL,"
        "  'sentTo' varchar(25) NOT NULL,"
        "  'content' varchar(100) NOT NULL,"
        "  'createdAt' date NOT NULL,"
        "  'receivedAt' date NOT NULL,"
        "  PRIMARY KEY ('messageID')"
        ") ENGINE=InnoDB")
