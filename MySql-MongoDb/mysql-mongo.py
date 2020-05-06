'''
Mysql to MongoDB tranfers of retail tables
'''
import mysql.connector
import pymongo as mongo

#Mysql Connections
try:
    cnx = mysql.connector.connect(user='indira', password='admin',
                              host='localhost',
                              database='retail_db')

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
cursor = cnx.cursor(buffered=True)
 
#List all tables in the current Database
cursor.execute('SHOW TABLES')
 
print('\nTables in Retail_db\n')
for a in cursor:
    print("".join(a))
     
#read all tables from retail_db
     
#Departments table read
 
query = 'select * from departments'
departments=cursor.execute(query)
 
print('\nDepartments data transferring\n')
  
#Mongo Connections

print('Mongo Connected!!')
myclient = mongo.MongoClient('mongodb://localhost:27017/')

#Test mongo connection
print('List to MongoDB databases\n')
print(myclient.list_database_names())

#Departments Transfer
mydb = myclient["retail_db"]
table = mydb["departments"]

for i in cursor:
    print(i[0],i[1])
    table.insert_one({'department_no':i[0],'department_name':i[1]})
 

#Categories Table Read
  
query = 'select * from categories'
categories=cursor.execute(query)
print('\nCategories data transferring\n')
      
#Categories Transfer
  
  
mydb = myclient["retail_db"]
table = mydb["categories"]
  
for i in cursor:
    table.insert_one({'category_id':i[0],'category_department_id':i[1],'category_name':i[2]})
  
print('\nCategories data transferred to MongoDB\n')
      
#Products Table
  
query = 'select * from products'
products = cursor.execute(query)
   
print('\nProducts data transferring\n')

      
#Products Transfer   
  

mydb = myclient["retail_db"]
table = mydb["products"]
for i in cursor:
    table.insert_one({'product_id':i[0],'product_category_id':i[1],'product_name':i[2],'product_description':i[3],'product_price':i[4],'product_image':i[5]})   
      
print('\nProducts data transferred to MongoDB\n')
       
#Order_items Table
query = 'select * from order_items'
order_items = cursor.execute(query)
  
print('\nOrder_items data transferring\n')

  
#Order_items Transfer
  
  
mydb = myclient["retail_db"]
table = mydb["order_items"]
  
for i in cursor:
     table.insert_one({'order_item_id':i[0],'order_item_order_id':i[1],'order_item_product_id':i[2],'order_item_quantity':i[3],'order_item_subtotal':i[4],'order_item_product_price':i[5]})
       
print('\nOrder_items data transferred to MongoDB\n') 
  
#Orders Table
query = 'select * from orders'
orders = cursor.execute(query)
print('\nOrders data transferring\n')
   
#Orders Transfer
  
  
mydb = myclient["retail_db"]
table = mydb["orders"]
  
for i in cursor:
    table.insert_one({'order_id':i[0],'order_date':i[1],'order_customer_id':i[2],'order_status':i[3]})
  
print('\nOrders data transferred to MongoDB\n')

#Customers Table

query = 'select * from customers'
customers = cursor.execute(query)

print('\nCustomers data transfering\n')

#Customers Transfer
  
  
mydb = myclient["retail_db"]
table = mydb["customers"]
  
for i in cursor:
    table.insert_one({'customer_id':i[0],'customer_fname':i[1],'customer_lname':i[2],'customer_email':i[3],'customer_password':i[4],'customer_street':i[5],'customer_city':i[6],'customer_state':i[7],'customer_zipcode':i[8]})
    
print('\nCustomers data transferred')  

#Test mongo connection
print(myclient.list_database_names())
cnx.close()
myclient.close()