import mysql.connector as connection

mydb = connection.connect(host='localhost', user='root', passwd='redhat')
query = "Create database if not exists DS_data"
query1 = "use DS_data"
query2 = "show databases"
cursor = mydb.cursor()
cursor.execute(query)
cursor.execute(query1)
cursor.execute(query2)
print(cursor.fetchall())



# Below commands are used to generate the skeleton of the Fitbit data.csv

#csvsql --dialect mysql --snifflimit 100000 "Attribute DataSet.xlsx"
#csvsql --db mysql://root:redhat@localhost:3306/DS_data --insert "Attribute_dataset.csv"