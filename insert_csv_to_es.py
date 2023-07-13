import csv, sys
import os
import sys

import pandas
import datetime
from elasticsearch import helpers, Elasticsearch


print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ STARTING @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")

es = Elasticsearch(host = "localhost", port = 9200)

# Extract
df = pandas.read_csv("data.csv", sep=";")

# Force the following field to be a String
df = df.astype({"Horodate": str})
df2_dic = df.to_dict('records')

def generator(df2_dic):
    '''Create json format before inserting data to ES'''
    print("Launching generator")
    for c, line in enumerate(df2_dic):
        yield {
            '_index': 'index-target',
            '_type':'_doc',
            '_id':line.get("id", None),
            '_source':{
                'Identifiant':line.get("Identifiant", None),
                'Horodate':line.get("Status", None),
                'Value':line.get("Origine", None)

            }
        }


print("Starting bulk")

# Load
try:
    res = helpers.bulk(es, generator(df2_dic))
    print("Data was inserted into ES")
    print("Response: ", res)
except Exception as e:
    print("Bulk Insertion did not work")
    print(e)
    pass



print("######################################  END JOB  ######################################")

