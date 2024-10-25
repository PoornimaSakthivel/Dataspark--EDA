
import pandas as pd
import numpy as np
from datetime import datetime
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine


path='C:\\Users\\POORNIMA\\Desktop\\POORNIMA\GUVI\\DataSpark\\'

#Database Connection function
def verify_connection(host, user, password,dbname) -> None:
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database = dbname
        )

        if connection.is_connected():
            print(f"Successfully connected to MySQL server at {host} with user {user}")
            connection.close()
        else:
            print("Connection failed")

    except Error as e:
        print(f"Error: {e}")

def insertdata(dbname,df):
    df.to_sql(dbname,engine,schema=None, 
    if_exists='replace', 
    index=True, # It means index of DataFrame will save. Set False to ignore the index of DataFrame.
    index_label=None, # Depend on index. 
    chunksize=None, # Just means chunksize. If DataFrame is big will need this parameter.
    dtype=None, # # Set the columns type of sql table.
    method=None, )

def loadingcsv(name,csvfilename):
    df=pd.read_csv(path+ csvfilename, encoding='ISO-8859-1')
    print("*"*70)
    print(name)
    print("*"*70)
    print(df.dtypes)
    return df

def covertingtonumeric(cost,df):
    
    df[cost]=df[cost].replace({'\$':"","\d ":"",",":""},regex=True)
                               
    df[cost]=pd.to_numeric(df[cost])
    print(df.dtypes)
    print(df[cost])


def convertdate(field,df):
    df[field] = pd.to_datetime(df[field], format='%m/%d/%Y', errors='coerce')
    df[field] = df[field].fillna(pd.to_datetime(df[field], format='%d-%m-%Y', errors='coerce'))
    #df[field]=df[field].replace({'/':"","\d ":"",",":""},regex=True)
                               
    #df[field]=pd.to_numeric(df[field])
    print(df.dtypes)

    print(df[field])


def calculateage(df):

    today = datetime.today()
    
    
    ages=[]
    for index, row in df.iterrows():
        birthdate = row['Birthday']
        if pd.notnull(birthdate):  # Check if birthdate is not null
            age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))
            ages.append(age)
        else:
            ages.append(None)  # Append None if birthdate is null
            
    dfcustomers['age'] = ages  # Assign the calculated ages to the 'age' column
        
def convertdatatype(df,columnname):
    df[columnname] = df[columnname].astype(int)       
    
#Customers Data Cleaning
dfcustomers=loadingcsv("CustomersDataTypes",'Customers.csv')
#print(dfcustomers)
convertdate('Birthday',dfcustomers)
calculateage(dfcustomers)
#calculateage('Birthday',dfcustomers)
#dfcustomers['age'] = dfcustomers.apply(calculateage,axis=1)
#print(dfcustomers['age'])
print(dfcustomers.dtypes)
print(dfcustomers.head())
missing_values = dfcustomers.isna().sum()
print(missing_values)
dfcustomers.info()
dfcustomers_cleaned = dfcustomers.dropna()
print(dfcustomers_cleaned)
missing_values = dfcustomers_cleaned.isna().sum()
print(missing_values)
#Loading into DB
DATABASE_URL='mysql+pymysql://root:Arudhra%401201@localhost/datasparkdata'
engine = create_engine(DATABASE_URL, echo=True,pool_size=10, max_overflow=20)
print(engine)
insertdata('customers',dfcustomers_cleaned)

#Sales Data Cleaning
dfsales=loadingcsv("SalesDataTypes",'Sales.csv')
print(dfsales)
missing_values = dfsales.isna().sum()
print(missing_values)
dfsales.info()
print(dfsales.dtypes)
convertdate('Order Date',dfsales)
dfsales_cleaned = dfsales.dropna(axis=1)
dfsales_cleaned.info()
print(dfsales_cleaned.dtypes)
print(dfsales_cleaned.sample(5))
#Loading into DB
DATABASE_URL='mysql+pymysql://root:Arudhra%401201@localhost/datasparkdata'
engine = create_engine(DATABASE_URL, echo=True,pool_size=10, max_overflow=20)
print(engine)
insertdata('sales',dfsales_cleaned)

#Stores Data Cleaning
dfstores=loadingcsv("StoresDataTypes",'Stores.csv')
print(dfstores)
missing_values = dfstores.isna().sum()
print(missing_values)
dfstores.info()
print(dfstores.dtypes)
convertdate('Open Date',dfstores)
dfstores_cleaned = dfstores.dropna()
dfstores_cleaned.info()
print(dfstores_cleaned.dtypes)
print(dfstores_cleaned.sample(5))
#Loading into DB
DATABASE_URL='mysql+pymysql://root:Arudhra%401201@localhost/datasparkdata'
engine = create_engine(DATABASE_URL, echo=True,pool_size=10, max_overflow=20)
print(engine)
insertdata('stores',dfstores_cleaned)

#Products Data Cleaning
dfproducts=loadingcsv("ProductsDataTypes",'Products.csv')
print(dfproducts)
missing_values = dfproducts.isna().sum()
print(missing_values)
dfproducts.info()
covertingtonumeric('Unit Cost USD',dfproducts)
covertingtonumeric('Unit Price USD',dfproducts)
missing_values = dfproducts.isna().sum()
print(missing_values)
dfproducts.info()
print(dfproducts.dtypes)
print(dfproducts["Unit Price USD"])
print(dfproducts.sample(5))
#Loading into DB
DATABASE_URL='mysql+pymysql://root:Arudhra%401201@localhost/datasparkdata'
engine = create_engine(DATABASE_URL, echo=True,pool_size=10, max_overflow=20)
print(engine)
insertdata('products',dfproducts)


#Merging Sales and Products dataset by productkey
dfsalesproducts = pd.merge(dfsales_cleaned, dfproducts, on='ProductKey', how='outer') 

print(dfsalesproducts.sample(10))
missing_values = dfsalesproducts.isna().sum()
print(missing_values)
dfsalesproducts.info()
print(dfsalesproducts.dtypes)
dfsalesproducts_cleaned = dfsalesproducts.dropna()
#convert float to int
convertdatatype(dfsalesproducts_cleaned,'Order Number')
convertdatatype(dfsalesproducts_cleaned,'Line Item')
convertdatatype(dfsalesproducts_cleaned,'CustomerKey')
convertdatatype(dfsalesproducts_cleaned,'StoreKey')
convertdatatype(dfsalesproducts_cleaned,'Quantity')
print(dfsalesproducts_cleaned.dtypes)
missing_values = dfsalesproducts_cleaned.isna().sum()
print(missing_values)
dfsalesproducts_cleaned.info()
#Loading into DB
DATABASE_URL='mysql+pymysql://root:Arudhra%401201@localhost/datasparkdata'
engine = create_engine(DATABASE_URL, echo=True,pool_size=10, max_overflow=20)
print(engine)
insertdata('salesproducts',dfsalesproducts_cleaned)

#Merging Sales and Customers dataset by customerkey
dfsalescustomers = pd.merge(dfsales_cleaned, dfcustomers_cleaned, on='CustomerKey', how='outer') 

print(dfsalescustomers.sample(10))
missing_values = dfsalescustomers.isna().sum()
print(missing_values)
dfsalescustomers.info()

print(dfsalescustomers.dtypes)
dfsalescustomers_cleaned = dfsalescustomers.dropna()
#convert float to int
convertdatatype(dfsalescustomers_cleaned,'Order Number')
convertdatatype(dfsalescustomers_cleaned,'Line Item')
convertdatatype(dfsalescustomers_cleaned,'CustomerKey')
convertdatatype(dfsalescustomers_cleaned,'StoreKey')
convertdatatype(dfsalescustomers_cleaned,'ProductKey')
convertdatatype(dfsalescustomers_cleaned,'Quantity')
convertdatatype(dfsalescustomers_cleaned,'age')
print(dfsalescustomers_cleaned.dtypes)
missing_values = dfsalescustomers_cleaned.isna().sum()
print(missing_values)
dfsalescustomers_cleaned.info()
#Loading into DB
DATABASE_URL='mysql+pymysql://root:Arudhra%401201@localhost/datasparkdata'
engine = create_engine(DATABASE_URL, echo=True,pool_size=10, max_overflow=20)
print(engine)
insertdata('salescustomers',dfsalescustomers_cleaned)


#Merging Sales and Stores dataset by StoreKey
dfsalesstores = pd.merge(dfsales_cleaned, dfstores_cleaned, on='StoreKey', how='outer') 

print(dfsalesstores.sample(10))
missing_values = dfsalesstores.isna().sum()
print(missing_values)
dfsalesstores.info()

print(dfsalesstores.dtypes)
dfsalesstores_cleaned = dfsalesstores.dropna()
#convert float to int
convertdatatype(dfsalesstores_cleaned,'Order Number')
convertdatatype(dfsalesstores_cleaned,'Line Item')
convertdatatype(dfsalesstores_cleaned,'CustomerKey')
convertdatatype(dfsalesstores_cleaned,'ProductKey')
convertdatatype(dfsalesstores_cleaned,'Quantity')
print(dfsalesstores_cleaned.dtypes)
missing_values = dfsalesstores_cleaned.isna().sum()
print(missing_values)
dfsalesstores_cleaned.info()
#Loading into DB
DATABASE_URL='mysql+pymysql://root:Arudhra%401201@localhost/datasparkdata'
engine = create_engine(DATABASE_URL, echo=True,pool_size=10, max_overflow=20)
print(engine)
insertdata('salesstores',dfsalesstores_cleaned)



