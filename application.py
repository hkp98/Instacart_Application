from collections import OrderedDict
from re import split
import json

from flask import Flask, render_template, request
import time
import pymysql
import psycopg2
import pyodbc


application = Flask(__name__, template_folder='templates')
application.config['SECRET_KEY'] = 'some_random_secret'


# app.config['MYSQL_HOST'] = 'database4.ciyb91h8aude.us-east-2.rds.amazonaws.com'
# app.config['MYSQL_HOST'] = 'instacartdb.cyxeteubefl2.us-east-2.rds.amazonaws.com'
# app.config['MYSQL_USER'] = 'admin'
# app.config['MYSQL_PASSWORD'] = '12345678'
# app.config['MYSQL_DB'] = 'database4'
# app.config['MYSQL_DB'] = 'instacart'

@application.route('/showpdf',methods=['get','post'])
def showpdf():
    return render_template('showpdf.html',l1=l1)

l1,l2=[],[]

'''@app.route('/index')
def index1():
    return render_template('index.html')'''

@application.route('/', methods=['GET', 'POST'])
def index():
    if request.method == "POST":
        details = request.form
        if not details['query']:
            return render_template('index.html', headings=None, query=None, database=None, data="", error="True", time=0)

        query = details['query']
        type(query)
        database = details['database']
        if database == "mysql":
            dbsql = pymysql.connect(host="instacartdb.cyxeteubefl2.us-east-2.rds.amazonaws.com",
                     user="admin", password="12345678", database="instacart")

            start_time = time.time()
            #cur = dbsql.cursor()
            try:

                cur = dbsql.cursor()
                cur.execute(query)
                dbsql.commit()

            except pymysql.Error as err:
                #print("Please rewrite the query")
                cur.close()
                return render_template('index.html', headings=None, head=None,query=None,err=err, database=None, data="", error="True",time=0)
            if cur.description:
                headings = [i[0] for i in cur.description]

            else:
                headings=None
            # print(headings)
            data = cur.fetchall()
            #data=pd.DataFrame(data)
            #print(data.head)
            #data=data.to_dataframe()
            head=cur.rowcount
            execution_time = time.time()-start_time
            global l1
            l2=[query,database,head,execution_time]
            l1.append(l2)
            l1.reverse()
            # print(data)
            cur.close()
        elif database == "redshift":
            dbredshift = psycopg2.connect(dbname= 'instacartdb', host='redshift-instacart-cluster.cj9vlu8msgc0.us-east-2.redshift.amazonaws.com', port= '5439', user= 'admin', password= 'Abcd1234')
            data = None
            start_time = time.time()

            try:
                cur = dbredshift.cursor()

                cur.execute(query)
                dbredshift.commit()
            except Exception as err:
                cur.close()
                return render_template('index.html', headings=None, query=None, database=None,err=err, data="", error="True",time=0)
            if cur.description:
                headings = [i[0] for i in cur.description]
                data = cur.fetchall()

                #data=data.to_dataframe()
                #print(data)
            else:
                headings=None
            # print(headings)

            head=cur.rowcount
            execution_time = time.time()-start_time
            l2 = [query, database, head, execution_time]
            l1.append(l2)
            l1.reverse()
            cur.close()
        elif database=="mongodb":
            dbname='instacart'
            port=27017
            server='18.221.227.218'
            con = pyodbc.connect('DRIVER={Devart ODBC Driver for MongoDB};'
                                  'Server=' + server + ';Port=' + str(port) +
                                  ';Database=' + dbname)
            start_time = time.time()
            data=None
            try:
                cur = con.cursor()
                cur.execute(query)
                con.commit()
            except Exception as err:
                cur.close()
                return render_template('index.html', headings=None, query=None, database=None,err=err, data="", error="True",time=0)
            if cur.description:
                headings = [i[0] for i in cur.description]
                data = cur.fetchall()
                head=len(data)
            else:
                head=cur.rowcount
                headings=None
                #print(cur.description)

            #head = len(data)
            #print(head)

            #head = cur.rowcount
            execution_time = time.time() - start_time
            l2 = [query, database, head, execution_time]
            l1.append(l2)
            l1.reverse()
            cur.close()
            #data = cur.fetchall()
            #print(res)
            '''drill = PyDrill(host='localhost', port=8047)
            drill.storage_enable('mongo')
            start_time = time.time()
            #print("Active",drill.is_active())
            try:
                data = drill.query(query)
                df = pd.DataFrame(list(data))
                c = df.index
                df.index+=1

                head = len(c)
                #df=data.to_dataframe()
                #print(df)
            #head=data.count(True)


                df=df.to_html(escape=False)



                execution_time = time.time() - start_time
                l2 = [query, database,head, execution_time]
                l1.append(l2)
                l1.reverse()
            except Exception as err:
                #drill.close()
                return render_template('index.html', headings=None, query=None, database=None,err=err, data="", error="True",time=0)


            return render_template('index.html',head=head,query=query,database=database,error="False",mongo=df,time=execution_time)'''



        return render_template('index.html', headings=headings, query=query,head=head, database=database, data=data, error="False", time=execution_time)
    return render_template('index.html', headings=None, query=None, head=None,database=None, data=None, error="None", time=0)

if __name__ == '__main__':
    application.run(debug=True)
