import pandas as pd
import mysql.connector as connection
import pymongo
import json
from logger import *
from MYSQL import sql_Database

class mongo_DB(sql_Database):

    def json_convertion(self):
        lp.info("json convertion function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            df = pd.read_sql('select * from dataset.attribute', mydb)
            df = df.to_json('attribute.json')
            print("Datasets are converted to json format")
            lp.info("Datasets are converted to json format")

        except Exception as e:
            return "(Json_convertion): Failed to convert the datasets to json \n" + str(e)
            lp.error("(Json_convertion): Failed to convert the datasets to json \n" + str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")

    def bulk_insert1(self):
        lp.info("Bulk insert function from class invoked")
        try:
            with open('attribute.json', 'r') as f:
                data = json.load(f)
            lp.info("Json data loaded successfully")
        except Exception as e:
            print("attribute.json files not found \n" + str(e))
            lp.error("attribute.json files not found \n" + str(e))
        try:
            client = pymongo.MongoClient("mongodb+srv://root:redhat@cluster0.dpqh6.mongodb.net/?retryWrites=true&w=majority")
            db = client.test
            db1 = client['dataset']
            attribute = db1['attribute']
            attribute.insert_many([data])
            print("Attribute data inserted into MongoDB")
            lp.info("Attribute data inserted into MongoDB")
        except Exception as e:
            return "(bulk_insert): Failed to bulk insert of data to mongoDB \n" + str(e)
            lp.error("(bulk_insert): Failed to bulk insert of data to mongoDB \n" + str(e))
        finally:
            client.close()
            return "MongoDB Database connection closed"
            lp.info("MongoDB Database connection closed")

    def insert_record1(self):
        lp.info("insert record function from class invoked")
        try:
            client = pymongo.MongoClient("mongodb+srv://root:redhat@cluster0.dpqh6.mongodb.net/?retryWrites=true&w=majority")
            db = client.test
            db1 = client['dataset']
            attribute = db1['attribute']
            data = [
                {
                    "id": "0001",
                    "type": "donut",
                    "name": "Cake",
                    "ppu": 0.55,
                    "batters":
                        {
                            "batter":
                                [
                                    {"id": "1001", "type": "Regular"},
                                    {"id": "1002", "type": "Chocolate"},
                                    {"id": "1003", "type": "Blueberry"},
                                    {"id": "1004", "type": "Devil's Food"}
                                ]
                        },
                    "topping":
                        [
                            {"id": "5001", "type": "None"},
                            {"id": "5002", "type": "Glazed"},
                            {"id": "5005", "type": "Sugar"},
                            {"id": "5007", "type": "Powdered Sugar"},
                            {"id": "5006", "type": "Chocolate with Sprinkles"},
                            {"id": "5003", "type": "Chocolate"},
                            {"id": "5004", "type": "Maple"}
                        ]
                },
                {
                    "id": "0002",
                    "type": "donut",
                    "name": "Raised",
                    "ppu": 0.55,
                    "batters":
                        {
                            "batter":
                                [
                                    {"id": "1001", "type": "Regular"}
                                ]
                        },
                    "topping":
                        [
                            {"id": "5001", "type": "None"},
                            {"id": "5002", "type": "Glazed"},
                            {"id": "5005", "type": "Sugar"},
                            {"id": "5003", "type": "Chocolate"},
                            {"id": "5004", "type": "Maple"}
                        ]
                },
                {
                    "id": "0003",
                    "type": "donut",
                    "name": "Old Fashioned",
                    "ppu": 0.55,
                    "batters":
                        {
                            "batter":
                                [
                                    {"id": "1001", "type": "Regular"},
                                    {"id": "1002", "type": "Chocolate"}
                                ]
                        },
                    "topping":
                        [
                            {"id": "5001", "type": "None"},
                            {"id": "5002", "type": "Glazed"},
                            {"id": "5003", "type": "Chocolate"},
                            {"id": "5004", "type": "Maple"}
                        ]
                }
            ]
            attribute.insert_many(data)
            result = pd.DataFrame(attribute.find_one())
            print("record inserted into MongoDB is \n", result)
            lp.info("record inserted into MongoDB is \n" + str(result))
        except Exception as e:
            return "(insert_record): Failed to insert record to mongoDB \n" + str(e)
            lp.error("(insert_record): Failed to insert record to mongoDB \n" + str(e))
        finally:
            client.close()
            return "MongoDB Database connection closed"
            lp.info("MongoDB Database connection closed")

    def fetch_record1(self):
        lp.info("insert record function from class invoked")
        try:
            client = pymongo.MongoClient("mongodb+srv://root:redhat@cluster0.dpqh6.mongodb.net/?retryWrites=true&w=majority")
            db = client.test
            db1 = client['dataset']
            attribute = db1['attribute']
            for i in attribute.find({"ppu" : {'$gt' : 0.1}}):
                print("record fetched into MongoDB is \n", i)
                lp.info("record fetched into MongoDB is \n" + str(i))
        except Exception as e:
            return "(fetch_record): Failed to fetch record to mongoDB \n" + str(e)
            lp.error("(fetch_record): Failed to fetch record to mongoDB \n" + str(e))
        finally:
            client.close()
            return "MongoDB Database connection closed"
            lp.info("MongoDB Database connection closed")

    def delete_record1(self):
        lp.info("insert record function from class invoked")
        try:
            client = pymongo.MongoClient("mongodb+srv://root:redhat@cluster0.dpqh6.mongodb.net/?retryWrites=true&w=majority")
            db = client.test
            db1 = client['dataset']
            attribute = db1['attribute']
            attribute.delete_many({"name":"Cake"})
            print("record deleted into MongoDB")
            lp.info("record deleted into MongoDB")
        except Exception as e:
            return "(delete_record1): Failed to fetch record to mongoDB \n" + str(e)
            lp.error("(delete_record1): Failed to fetch record to mongoDB \n" + str(e))
        finally:
            client.close()
            return "MongoDB Database connection closed"
            lp.info("MongoDB Database connection closed")

    def update_record1(self):
        lp.info("insert record function from class invoked")
        try:
            client = pymongo.MongoClient("mongodb+srv://root:redhat@cluster0.dpqh6.mongodb.net/?retryWrites=true&w=majority")
            db = client.test
            db1 = client['dataset']
            attribute = db1['attribute']
            attribute.update_many({'type':'donut'}, { '$set': {'type':'spring_roll'}} )
            print("record updated into MongoDB")
            lp.info("record updated into MongoDB")
        except Exception as e:
            return "(update_record1): Failed to update record to mongoDB \n" + str(e)
            lp.error("(update_record1): Failed to update record to mongoDB \n" + str(e))
        finally:
            client.close()
            return "MongoDB Database connection closed"
            lp.info("MongoDB Database connection closed")


    def drop_collection(self):
        lp.info("Drop database collection function from class invoked")
        try:
            client = pymongo.MongoClient("mongodb+srv://root:redhat@cluster0.dpqh6.mongodb.net/?retryWrites=true&w=majority")
            db = client.test
            db1 = client['dataset']
            attribute = db1['attribute']
            attribute.drop()
            print("Database collections dropped successfully")
            lp.info("Database collections dropped successfully")
        except Exception as e:
            return "(drop_collection): Failed to drop the mongoDB collection \n" + str(e)
            lp.error("(drop_collection): Failed to drop the mongoDB collection \n" + str(e))
        finally:
            client.close()
            return "MongoDB Database connection closed"
            lp.info("MongoDB Database connection closed")
