from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://10.31.1.110:7687", auth=("neo4j", "1qaz2wsx"))


def find_friend(tx, phone):
    for record in tx.run(
            "MATCH (a:TELEPHONE) WHERE a.tel = '" + phone + "' RETURN a.idcard AS idcard, a.name AS name, a.tel AS tel"):
        print(str(record['idcard']) + "-----" + str(record['name']) + "-----" + str(record['tel']) + "-----")


session = driver.session()
session.read_transaction(find_friend, "13415207927")


# 官方示例
# def add_friend(tx, name, friend_name):
#     tx.run("MERGE (a:Person {name: $name}) "
#            "MERGE (a)-[:KNOWS]->(friend:Person {name: $friend_name})",
#            name=name, friend_name=friend_name)
#
# def print_friends(tx, name):
#     for record in tx.run("MATCH (a:Person)-[:KNOWS]->(friend) WHERE a.name = $name "
#                          "RETURN friend.name ORDER BY friend.name", name=name):
#         print(record["friend.name"])
#
# with driver.session() as session:
#     session.write_transaction(add_friend, "Arthur", "Guinevere")
#     session.write_transaction(add_friend, "Arthur", "Lancelot")
#     session.write_transaction(add_friend, "Arthur", "Merlin")
#     session.read_transaction(print_friends, "Arthur")`1`


def testGraph(tx):
    for record in tx.run("CALL algo.graph.load('my-graph3','MATCH (t:TELEPHONE{tel:\"15838515866\"})-[r]-(m:TELEPHONE) "
                         "return id(m) as id ','MATCH (t1)-[r]-(t2) "
                         "return id(t1) as source,id(t2) as target',{graph: 'cypher'})"):
        print(str(record['id']) + "-----" + str(record['source']))


session.read_transaction(testGraph)


# pageRank
def algoPageRank(tx):
    for record in tx.run(
            "CALL algo.pageRank.stream('Page', 'LINKS', {iterations:20, dampingFactor:0.85}) YIELD nodeId, "
            "score RETURN algo.asNode(nodeId).name AS page,score ORDER BY score DESC"):
        print(str(record['page']) + "-----" + str(record['score']))


# session.read_transaction(algoPageRank)

# APOC
def apocRun(tx):
    for record in tx.run("CALL apoc.index.nodes('Airport','name:inter*') YIELD node AS airport, weight "
                         "RETURN airport.name, weight LIMIT 10"):
        print(str(record['airport.name']) + "-----" + str(record['weight']))
# session.read_transaction(apocRun)
