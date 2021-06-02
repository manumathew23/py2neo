from pymongo import MongoClient
client=MongoClient("mongodb://")
db = client.career_progression
col = db.duplet_collection_all_synon

count = col.count()

j = 0
i = 1000

while j<= count:
    select_records = col.find({}).limit(i).skip(j)
    j += i
    if j+1000 >= count:
        i = count - i

    session.write_transaction(push_data_to_graphdb, select_records)


def push_data_to_graphdb(tx, select_records):
    for path in select_records:
        duplet = path.get('duplets')
        tx.run("MERGE (a:node {name: $start}) MERGE (b: node{name: $end}) MERGE  (a)-[:to{count: $count}]->(b)", start=duplet[0], end=duplet[1], count=path.get('count'))


    for path in select_records:
        duplet = path.get('duplets')
        tx.run("MERGE (a:node {name: $start}) MERGE (b: node{name: $end}) MERGE  (a)-[:to{count: $count}]->(b)",
               start=duplet[0], end=duplet[1], count=path.get('count'))



client = MongoClient(
    "mongodb://")
db = client.career_progression
col = db.duplet_collection_all_synon

db_instance = Neo4jGraphDataBase()
db_driver = db_instance.driver
session = db_driver.session()

# count = col.count()
count = 500

j = 0
i = 100

while j <= count:
    select_records = col.find({}).limit(i).skip(j)
    j += i
    if j + 1000 >= count:
        i = count - i

    session.write_transaction(push_data_to_graphdb, select_records)
