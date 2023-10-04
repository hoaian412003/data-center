from datetime import date, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago

from airflow.providers.mongo.hooks.mongo import MongoHook, MongoClient
from flask import jsonify

# DAG creation
dag = DAG(
    "mongodb_hadoop",
    description="Move all data in mongodb base on your configuration to hadoop (HDFS) system DAG",
    start_date=days_ago(2),
    default_args={
        "conn_id": "PoolsWallet",
        "mongo_db": "poolwallet",
        "mongo_collection": "accesslogs",
        "batch_size": 100,  # Default number of records will send to hdfs in one batch
    },
)

hook = MongoHook(conn_id=dag.default_args["conn_id"])
collection = hook.get_collection(
    mongo_collection=dag.default_args["mongo_collection"],
    mongo_db=dag.default_args["mongo_db"],
)


def getTotalRecords():
    total = collection.count_documents(filter={})
    return total


def fetchBatchRecord(batchNumber: int):
    batchSize = dag.default_args["batch_size"]
    records = collection.find().skip(batchNumber * batchSize).limit(batchSize)
    return records


def fetchAllRecords():
    total = getTotalRecords()
    batchCount = int(total / dag.default_args["batch_size"]) + 1
    totalRecordProcessed = 0

    print("Total of batch / record is: ", batchCount, total)

    for bathNumber in range(batchCount):
        print("Processing bath #", bathNumber)
        records = fetchBatchRecord(bathNumber)
        result = []
        for record in records:
            record["_id"] = str(record["_id"])
            result.append(record)
            totalRecordProcessed = totalRecordProcessed + 1
        print("Found ", len(result), " records")

    return totalRecordProcessed


def putDataToHadoop():
    pass


t1 = PythonOperator(
    task_id="fetch",
    dag=dag,
    python_callable=fetchAllRecords,
)
