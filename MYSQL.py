from logger import *
import mysql.connector as connection
import pandas as pd

class sql_Database:
    def __init__(self, host, username, password):
        lp.info("Sql_database class invoked")
        try:
            self.host = host
            self.username = username
            self.password = password
            lp.info("The MySQL DB connection parameters initialized")
        except Exception as e:
            return "__init__: Failed to initialize \n" + str(e)
            lp.error("__init__: Failed to initialize \n" + str(e))

    def database_creation(self):
        lp.info("Database creation function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            query = "Create database if not exists dataset"
            query1 = "show databases"
            cursor = mydb.cursor()
            cursor.execute(query)
            cursor.execute(query1)
            output = cursor.fetchall()
            print("Database created successfully  ", output)
            lp.info("Database created successfully  " + str(output))

        except Exception as e:
            return "(database_creation): Failed to create sql database \n" + str(e)
            lp.error("(database_creation): Failed to create sql database \n" + str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")

    def table_creation(self):
        lp.info("Table creation function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            cursor = mydb.cursor()
            query = "create table if not exists dataset.attribute(Dress_ID int ," \
                    "Style varchar(100) , " \
                    "Price varchar(100) , " \
                    "Rating float(20,10) , " \
                    "Size varchar(100) , " \
                    "Season varchar(100) ," \
                    "NeckLine varchar(100) ," \
                    "SleeveLength varchar(100) ," \
                    "waiseline varchar(100) ," \
                    "Material varchar(100) ," \
                    "FabricType varchar(100) ," \
                    "Decoration varchar(100) ," \
                    "PatternType varchar(100) ," \
                    "Recommendation int )"
            query1 =  "show tables from dataset"
            cursor.execute(query)
            cursor.execute(query1)
            output = cursor.fetchall()
            print("Tables created successfully", output)
            lp.info("Tables created successfully"+ str(output))

        except Exception as e:
            return "(table_creation): Failed to create table \n"+ str(e)
            lp.info("(table_creation): Failed to create table \n" + str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")

    def Attribute_bulk_insert(self):
        lp.info("Attribute bulk insert function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            cursor = mydb.cursor()
            df = pd.read_excel(r"C:\Users\Babu Jayaraman\Music\FSDS_Live_classes_Tasks\Database_Task\Attribute DataSet.xlsx")
            df.fillna('NULL', inplace=True)
            try:
                for (row, rs) in df.iterrows():
                    Dress_ID = str(int(rs[0]))
                    Style = rs[1]
                    Price = rs[2]
                    Rating = str(float(rs[3]))
                    Size = rs[4]
                    Season = rs[5]
                    NeckLine = rs[6]
                    SleeveLength = rs[7]
                    waiseline = rs[8]
                    Material = rs[9]
                    FabricType = rs[10]
                    Decoration = rs[11]
                    PatternType = rs[12]
                    Recommendation = str(int(rs[13]))
                    asdf = (Dress_ID + "," + "'{}'" + "," + "'{}'" + "," + Rating + "," + "'{}'" + "," + "'{}'" + "," + "'{}'" + "," + "'{}'" + "," + "'{}'" + "," + "'{}'" + "," + "'{}'" + "," + "'{}'" + "," + "'{}'" + "," + Recommendation).format(
                        Style, Price, Size, Season, NeckLine, SleeveLength, waiseline, Material, FabricType, Decoration,PatternType)
                    query = "insert into dataset.attribute values({})".format(asdf)
                    cursor.execute(query)
                mydb.commit()
                print("All attribute data inserted")
                lp.info("All attribute data inserted")
            except Exception as e:
                print("Error in inserting attribute bulk data \n" + str(e))
                lp.error("Error in inserting attribute bulk data \n" + str(e))
        except Exception as e:
            return "(Attribute_bulk_upload): Failed to bulk upload Attribute data to attribute table \n"+ str(e)
            lp.error("(Attribute_bulk_upload): Failed to bulk upload Attribute data to attribute table \n"+ str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")

    def read_dataset(self):
        lp.info("Read dataset function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            result = pd.read_sql('select * from dataset.attribute', mydb)
            print(result)
            lp.info("Attribute dataset is: \n" + str(result))

            result1 = pd.read_sql('select * from dataset.sales', mydb)
            print(result1)
            lp.info("sales dataset is: \n" + str(result1))
            print("Attribute and sales Dataset read completed successfully")
            lp.info("Attribute and sales Dataset read completed successfully")

        except Exception as e:
            return "(read_dataset): Failed to read the attribute and sales datasets in pandas dataframe \n"+ str(e)
            lp.error("(read_dataset): Failed to read the attribute and sales datasets in pandas dataframe \n"+ str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")


    def drop_database(self):
        lp.info("Drop database function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            cursor = mydb.cursor()
            cursor.execute("drop database dataset")
            print("Database dropped successfully")
            lp.info("Database dropped successfully")

        except Exception as e:
            return "(drop_database): Failed dropping database \n" + str(e)
            lp.info("(drop_database): Failed dropping database \n" + str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")


    def insert_record(self):
        lp.info("insert data function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            cursor = mydb.cursor()
            try:
                query = "insert into dataset.attribute values(10222851,'Sexy','Low'," \
                        "4.6,'M','Summer','o-neck','sleevless','empire','null','chiffon','ruffles','animal',1)"
                cursor.execute(query)
                mydb.commit()
                print("one record inserted")
                lp.info("one record inserted")
            except Exception as e:
                print("Error in inserting one record \n" + str(e))
                lp.error("Error in inserting one record \n" + str(e))
        except Exception as e:
            return "(insert_record): Failed to insert one record to attribute table \n" + str(e)
            lp.error("(insert_record): Failed to insert one record to attribute table \n" + str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")

    def update_record(self):
        lp.info("updating records function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            cursor = mydb.cursor()
            try:
                query = "update dataset.attribute set Recommendation = 1 where Season = 'Summer'"
                cursor.execute(query)
                mydb.commit()
                print("Records updated")
                lp.info("Records updated")
            except Exception as e:
                print("Error in updating records \n" + str(e))
                lp.error("Error in updating records \n" + str(e))
        except Exception as e:
            return "(update_record): Failed to updating records to attribute table \n" + str(e)
            lp.error("(update_record): Failed to updating records to attribute table \n" + str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")

    def delete_record(self):
        lp.info("delete data function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            cursor = mydb.cursor()
            try:
                query = 'DELETE from dataset.attribute WHERE Season="Summer"'
                cursor.execute(query)
                mydb.commit()
                print("Records deleted")
                lp.info("Records deleted")
            except Exception as e:
                print("Error in deleting records \n" + str(e))
                lp.error("Error in deleting records \n" + str(e))
        except Exception as e:
            return "(delete_record): Failed to deleting records to attribute table \n" + str(e)
            lp.error("(delete_record): Failed to deleting records to attribute table \n" + str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")

    def fetch_record(self):
        lp.info("fetch data function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            cursor = mydb.cursor()
            try:
                query = "select * from dataset.attribute where Season = 'Summer'"
                cursor.execute(query)
                result = cursor.fetchall()
                print(pd.DataFrame(result))
                lp.info("Records fetched" + str(pd.DataFrame(result)))
            except Exception as e:
                print("Error in fetching records \n" + str(e))
                lp.error("Error in fetching records \n" + str(e))
        except Exception as e:
            return "(fetch_record): Failed to fetching records to attribute table \n" + str(e)
            lp.error("(fetch_record): Failed to fetching records to attribute table \n" + str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")
