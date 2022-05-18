import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

# Use the application default credentials
cred = credentials.Certificate("airflow/dags/cred.json")
firebase_admin.initialize_app(cred)

db = firestore.client()

from random import randint
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

def _parseHttp():
    doc_ref = db.collection(u'httpDateSize')
    numberOfLines = 0
    numberOfGet = 0
    numberOfPost = 0
    f = open("airflow/dags/http.log", "r")
    w = open("airflow/dags/result.txt", "w")
    for x in f:
        for index, word in enumerate(x.split()):
            if word == '200':
                print(x)
                if(x.split()[index-3]) == '"GET':
                    
                    w.write("\n"+"GET"+"\n")
                    w.write(x.split()[index-5]+"\n")
                    w.write(x.split()[index+1]+"\n")
                    # doc_ref.add({
                    # u'type': u'GET',
                    # u'date': x.split()[index-5],
                    # u'size': x.split()[index+1],
                    # })
                    numberOfGet += 1
                if(x.split()[index-3]) == '"POST':
                    w.write("POST"+"\n")
                    print(x.split()[index-5])
                    w.write(x.split()[index-5]+"\n")
                    print(x.split()[index+1])
                    w.write(x.split()[index+1]+"\n")

                    numberOfPost += 1
                numberOfLines += 1


    w.write('Total 200 requests={}\n'.format(numberOfLines))
    w.write('GET={}\n'.format(numberOfGet))
    w.write('POST={}\n'.format(numberOfPost))

    print('Total 200 requests={}'.format(numberOfLines))
    print('GET={}'.format(numberOfGet))
    print('POST={}'.format(numberOfPost))

    f.close()
    w.close()


with DAG("httpChartDag", start_date=datetime(2022, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    parseHttp = PythonOperator(
        task_id="parseHttp",
        python_callable=_parseHttp
    )