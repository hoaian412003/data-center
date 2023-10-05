from datetime import date, timedelta
import json

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago

from airflow.providers.mongo.hooks.mongo import MongoHook, MongoClient
from hdfs import InsecureClient
from airflow.models.param import Param


# DAG creation
dag = DAG(
    "mongodb_hadoop",
    description="Move all data in mongodb base on your configuration to hadoop (HDFS) system DAG",
    start_date=days_ago(2),
    params={
        "conn_id": Param("PoolsWallet", type="string"),
        "mongo_db": Param("poolwallet", type="string"),
        "mongo_collection": Param("banners", type="string"),
        "batch_size": Param(100, type="integer"),
        "hdfs_uri": Param("http://namenode:9870", type="string"),
    },
    render_template_as_native_obj=True,
)


def task(**context):
    args = context["params"]

    hook = MongoHook(conn_id=args["conn_id"])
    collection = hook.get_collection(
        mongo_collection=args["mongo_collection"],
        mongo_db=args["mongo_db"],
    )
    hdfsClient = InsecureClient(args["hdfs_uri"], "root")

    def getTotalRecords():
        total = collection.count_documents(filter={})
        return total

    def fetchBatchRecord(batchNumber: int):
        batchSize = args["batch_size"]
        records = collection.find().skip(batchNumber * batchSize).limit(batchSize)
        return records

    def fetchAllRecords():
        total = getTotalRecords()
        batchCount = int(total / args["batch_size"]) + 1
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
            putDataToHadoop(result, args["mongo_collection"] + "/block_" + str(bathNumber))
            print("Found ", len(result), " records")

        return totalRecordProcessed

    def putDataToHadoop(data, name_file: str):
        jsonData = json.dumps(data, default=str)
        with hdfsClient.write(name_file) as writer:
            writer.write(jsonData)
        return jsonData

    # Main Runner
    fetchAllRecords()


t1 = PythonOperator(
    task_id="fetch",
    dag=dag,
    python_callable=task,
)
