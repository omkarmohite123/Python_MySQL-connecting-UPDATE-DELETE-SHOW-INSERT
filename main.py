#install mysql connector
#install connectorDB
"""
#1)Connecting Database and Creating Table::
import mysql.connector
mydb=mysql.connector.connect(host='localhost',
                             user='root',
                             password='',
                             database='test.db')
if my.db.is_connected():
    print("connected")
mydb.close()
mycursor=mydb.cursor()
mycursor.execute("CREATE TABLE users(id INT, FirstName VARCHAR(255), Lastname VARCHAR(255), Pin INT, state VARCHAR(255), Latitude INT, Longitude INT)"

"""




"""
#2)inserting Database and adding rows::

import mysql.connector
mydb=mysql.connector.connect(host='localhost',
                             user='root',
                             password='',
                             database='test.db')
mycursor=mydb.cursor()
sql=("INSERT INTO users (id, FirstName, Lastname, Pin, state, Latitude, Longitude)\
                 VALUES(%s,%s,%s,%s,%s,%s,%s)")
val=[(2,"shreyas","mohite",234567,"kerala",789456,321654),
     (3,"ronit","patile",345678,"delhi",486512,759426),
     (4,"amit","pawar",678444,"vadi",777512,756426),
     (5,"ron","patile",349978,"delhi",477512,700426),
     (6,"ajit","savant",389678,"rahi",489912,750026)]
mycursor.executemany(sql, val)
mydb.commit()
print(mycursor.rowcount,"Record inserted")
"""



"""
#3)write a python programto select Firstname and state from users table::

import mysql.connector
mydb=mysql.connector.connect(host='localhost',
                             user='root',
                             password='',
                             database='test.db')
mycursor=mydb.cursor()
a=mycursor.execute("SELECT FirstName,state FROM users")
myres=mycursor.fetchall()
for x in myres:
    print(x)
"""




"""
#4)update state value in users table::

import mysql.connector
mydb=mysql.connector.connect(host='localhost',
                             user='root',
                             password='',
                             database='test.db')
mycursor=mydb.cursor()
sql=("UPDATE users SET state='punjab' WHERE FirstName='ronit'")
mycursor.execute(sql)
mydb.commit()
print(mycursor.rowcount,"record updated")
"""




"""
#5)Delete any row from users table::

import mysql.connector
mydb=mysql.connector.connect(host='localhost',
                             user='root',
                             password='',
                             database='test.db')
mycursor=mydb.cursor()
sql=("DELETE FROM users WHERE FirstName='ronit'")
mycursor.execute(sql)
mydb.commit()
print(mycursor.rowcount,"Record delete")
"""





"""
#6)find data from id and Name::


import mysql.connector
mydb=mysql.connector.connect(host='localhost',
                             user='root',
                             password='',
                             database='test.db')
mycursor=mydb.cursor()
sql=("SELECT id,FirstName FROM users")
mycursor.execute(sql)
myres=mycursor.fetchall()
for x in myres:
    print(x)
"""





"""
#7)Get data from users table using id value:::


sql = "SELECT * FROM users WHERE id = '2'"
mycursor.execute(sql)

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
"""





"""
#8)ordering using name:::


import mysql.connector
mydb=mysql.connector.connect(host='localhost',
                             user='root',
                             password='',
                             database='test.db')
mycursor=mydb.cursor()
sql="SELECT * FROM users ORDER BY FirstName"
mycursor.execute(sql)
myresult = mycursor.fetchall()
for x in myresult:
  print(x)
"""





"""
#9)limiting users data:::

import mysql.connector
mydb=mysql.connector.connect(host='localhost',
                             user='root',
                             password='',
                             database='test.db')
mycursor=mydb.cursor()
mycursor.execute("SELECT * FROM users LIMIT 5")
myresult = mycursor.fetchall()

for x in myresult:
  print(x)
"""




"""
#10)create coustmers table:::


import mysql.connector
mydb=mysql.connector.connect(host='localhost',
                             user='root',
                             password='',
                             database='test.db')
mycursor=mydb.cursor()
mycursor.execute("CREATE TABLE customers (id INT,name VARCHAR(255), state VARCHAR(255))")

"""




"""
#11)inserting some values in coustmer table:::


import mysql.connector
mydb=mysql.connector.connect(host='localhost',
                             user='root',
                             password='',
                             database='test.db')
mycursor=mydb.cursor()
sql=("INSERT INTO customers (id, name, state)\
                 VALUES(%s,%s,%s)")
val=[(1,"rajendra","mumbai"),
     (2,"asif","pune"),
     (3,"shweta","sangli")]

mycursor.executemany(sql, val)
mydb.commit()
print(mycursor.rowcount,"Record inserted")
"""




"""
#12)using join function to join users and coustemr tables1:::


import mysql.connector
mydb=mysql.connector.connect(host='localhost',
                             user='root',
                             password='',
                             database='test.db')
mycursor=mydb.cursor()
sql = "SELECT \
  users.FirstName AS name \
  customers.state As state \
  FROM users \
  INNER JOIN customers ON users.id = customers.id"
mycursor.execute(sql)

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
"""




"""
#13) view users table and customers table:::



import mysql.connector
mydb=mysql.connector.connect(host='localhost',
                             user='root',
                             password='',
                             database='test.db')
mycursor=mydb.cursor()
mycursor = mydb.cursor()

mycursor.execute("SHOW TABLES")

for x in mycursor:
  print(x)
"""