from impala.dbapi import connect
from impala.util import as_pandas

conn = connect(host='10.10.14.18',
               port=10000,
               auth_mechanism='LDAP',
               user='liuyuan',
               password='1234@abc')

cursor = conn.cursor()
cursor.execute("select * from graph.zto_store_infor")
data = as_pandas(cursor)
print(data.head())





