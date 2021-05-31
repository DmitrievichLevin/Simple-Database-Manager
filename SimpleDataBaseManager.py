import mysql.connector
import time

#queryObject is created from parsed JSON data
#It contains 8 str values corresponding to each column in table1


class myDataBase:
    def __init__(self):
        self.dB = mysql.connector.connect(
        host = "endpoint",
        user = "username",
        password = "password",
        database = "databasename"
        )

        self.mycursor = self.dB.cursor(buffered = True)

        self.mycursor.execute("SELECT * FROM table_name1")

        myresult = self.mycursor.fetchall()
        
    def insertArticle(self, queryObject):
            self.queryObject = queryObject
           

            insert_stmt = (
                "INSERT INTO table_name1 (column1, column2, colum3, column4, column5, column6, column7, column8) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            )
            
            data = (self.queryObject.column_1, self.queryObject.column_2, 
                    self.queryObject.column_3, self.queryObject.column_4,
                    self.queryObject.column_5, self.queryObject.column_6, 
                    self.queryObject.column_7, self.queryObject.column_8
                   )
           
            if self.checkArticles(self.queryObject) == False:

                self.mycursor.execute(insert_stmt, data)
                self.dB.commit()
                self.dB.close()

            else:
                print("Updating table_name1...")
                self.updateArticle(self.queryObject)

    
    #checks for duplicates to maintain data integrity
    def checkArticles(self, queryObject):
        self.queryObject = queryObject
        
        #In my Database entries with different values for column1
        #Can contain the same entry for column2
        #But must remain parellel 
        check_stmt = (
            "SELECT column_name FROM table_name1 "
            "WHERE column1 = %s AND column2 = %s"
        )

        data = {self.queryObject.column_1,
                self.queryObject.column_2
               }
        
        
        self.mycursor.execute(check_stmt, (data,))
        
        if self.mycursor.fetchone() == None:

            return False
        
        else: 
            print("duplicate entry")
            return True

    
    #If a duplicate entry is found columns are updated to reflect real-time values
    def updateArticle(self, queryObject):
        self.queryObject = queryObject

        update_stmt = ("UPDATE table_name1 SET column3 = %s, column4 = %s, " + 
                       "column5 = %s, column6 = %s, column7 = %s, " +
                       "column8 = %s WHERE column1 = %s AND column2 = %s;")

        data = (self.queryObject.column_3, self.queryObject.column_4, self.queryObject.column_5,
               self.queryObject.column_5, self.queryObject.column_6, self.queryObject.column_7,
               self.queryObject.column_1, self.queryObject.column_2)

        self.mycursor.execute(update_stmt, data)
        self.dB.commit()
        self.dB.close()
       
    #To ensure no old data is left and also
    #manage memory usage
    #month old data will be deleted
    #column5 is a UNIX time value
    def deleteMonthOld(self):
        curEpoch = int(time.time())
        monthAgo = curEpoch - 2629743

        delete_stmt = ("DELETE FROM table_name1 WHERE column5 <= %s;")
        
        data_cond = monthAgo
        self.mycursor.execute(delete_stmt, (data_cond,))
        self.dB.commit()
        self.dB.close()
        print("Month old data deleted")


    #table0 contains Unique keys needed to make queries
    #where the data from table1 will be compiled for use
    #in the remaining columns during runtime
    #we need those keys to run in a query loop to fill table1
    def get_tickers(self):

        get_stmt = "SELECT column1 FROM table_name0"

        self.mycursor.execute(get_stmt)
        
        row = [item[0] for item in self.mycursor.fetchall()]

        return row
    
        self.dB.commit()
        self.dB.close()

        

