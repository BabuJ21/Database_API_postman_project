from MYSQL import *
from MongoDB import *
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/mysql_database/insert',methods=['GET','POST'])
def insert_record():
    if(request.method == 'POST'):
        a = request.json['host']
        b = request.json['user']
        c = request.json['password']
        asdf = sql_Database(a,b,c)
        asdf.database_creation()
        asdf.table_creation()
        asdf.Attribute_bulk_insert()
        asdf.insert_record()
        return "record inserted successfully"

@app.route('/mysql_database/update',methods=['GET','POST'])
def update_record():
    if(request.method == 'POST'):
        a = request.json['host']
        b = request.json['user']
        c = request.json['password']
        s = sql_Database(a,b,c)
        s.update_record()
        return "record updated successfully"

@app.route('/mysql_database/fetch',methods=['GET','POST'])
def fetch_record():
    if(request.method == 'POST'):
        a = request.json['host']
        b = request.json['user']
        c = request.json['password']
        s = sql_Database(a,b,c)
        s.fetch_record()
        return "records fetched successfully"

@app.route('/mysql_database/delete',methods=['GET','POST'])
def delete_record():
    if(request.method == 'POST'):
        a = request.json['host']
        b = request.json['user']
        c = request.json['password']
        s = sql_Database(a,b,c)
        s.delete_record()
        return "record deleted successfully"


@app.route('/MongoDB_database/insert',methods=['GET','POST'])
def insert_record1():
    if(request.method == 'POST'):
        a = request.json['host']
        b = request.json['user']
        c = request.json['password']
        s = mongo_DB(a,b,c)
        s.json_convertion()
        s.bulk_insert1()
        s.insert_record1()
        return "record inserted successfully"

@app.route('/MongoDB_database/fetch',methods=['GET','POST'])
def fetch_record1():
    if(request.method == 'POST'):
        a = request.json['host']
        b = request.json['user']
        c = request.json['password']
        s = mongo_DB(a,b,c)
        s.fetch_record1()
        return "record fetched successfully"

@app.route('/MongoDB_database/update',methods=['GET','POST'])
def update_record1():
    if(request.method == 'POST'):
        a = request.json['host']
        b = request.json['user']
        c = request.json['password']
        s = mongo_DB(a,b,c)
        s.update_record1()
        return "record updated successfully"

@app.route('/MongoDB_database/delete',methods=['GET','POST'])
def delete_record1():
    if(request.method == 'POST'):
        a = request.json['host']
        b = request.json['user']
        c = request.json['password']
        s = mongo_DB(a,b,c)
        s.delete_record1()
        return "record deleted successfully"

if __name__ == '__main__':
    app.run()