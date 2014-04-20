__author__ = 'Saksham'

import py2neo
from py2neo import neo4j, node, rel, cypher
import time


def main():
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
    #graph_db.clear()

    graph_db.delete_index(neo4j.Node, "People_TMP")
    graph_db.delete_index(neo4j.Node, "Comments_TMP")
    #graph_db.delete_index(neo4j.Node, "Reply_Edges_TMP")

    people = graph_db.get_or_create_index(neo4j.Node, "People_TMP")
    comments = graph_db.get_or_create_index(neo4j.Node, "Comments_TMP")
    comment_reply_edges = graph_db.get_or_create_index(neo4j.Relationship, "Reply_Edges_TMP")

    people_nodes = 0

    batch = neo4j.WriteBatch(graph_db)


    with open('../data/person_knows_person.csv') as res:
        content = res.read()
        lines = content.split('\n')
        lines = [x for x in lines if x is not '']
        lines = lines[1:]


    tmp_people = set()
    start_time = time.clock()
    ctr = 0
    print "Started KNOWS edges . . .\n"
    for line in lines:
        parts = line.split('|')
        first = int(parts[0])
        second = int(parts[1])

        batch.get_or_create_in_index(neo4j.Node, "People_TMP", "id", first, {"id": first, "type": "Person"})
        batch.get_or_create_in_index(neo4j.Node, "People_TMP", "id", second, {"id": second, "type": "Person"})

        ctr += 1
        if ctr > 10000:
            xx = batch.submit()
            batch.clear()
            while len(xx) > 0:
                v1 = xx.pop(0)
                v2 = xx.pop(0)
                batch.append_cypher("START n=node(" + str(v1._id) + "), t=node(" + str(v2._id) + ") CREATE (n)-[:KNOWS]->(t)")
            batch.run()
            batch.clear()
            ctr = 0
            print "10,000 batch done..."

    xx = batch.submit()
    batch.clear()
    while len(xx) > 0:
        v1 = xx.pop(0)
        v2 = xx.pop(0)
        batch.append_cypher("START n=node(" + str(v1._id) + "), t=node(" + str(v2._id) + ") CREATE (n)-[:KNOWS]->(t)")
    batch.run()
    batch.clear()
    end_time = time.clock()
    print "Created KNOWS edges in %s seconds...\n" % str(end_time - start_time)

    # #######################################################################
    #
    with open('../data/comment_hasCreator_person.csv') as res:
        content = res.read()
        lines = content.split('\n')
        lines = [x for x in lines if x is not '']
        lines = lines[1:]

    start_time = time.clock()
    print "Started COMMENTED edges . . .\n"
    ctr = 0
    for line in lines:
        parts = line.split('|')
        cid = int(parts[0])
        pid = int(parts[1])

        batch.get_or_create_in_index(neo4j.Node, "People_TMP", "id", pid, {"id": pid, "type": "Person"})
        batch.get_or_create_in_index(neo4j.Node, "Comments_TMP", "id", cid, {"id": cid, "type": "Comment"})
        ctr += 1
        if ctr > 10000:
            xx = batch.submit()
            batch.clear()
            while len(xx) > 0:
                v1 = xx.pop(0)
                v2 = xx.pop(0)
                batch.append_cypher("START n=node(" + str(v1._id) + "), t=node(" + str(v2._id) + ") CREATE (n)-[:COMMENTED]->(t)")
            batch.run()
            batch.clear()
            ctr = 0
            print "Done 10,000 batch..."
    xx = batch.submit()
    batch.clear()
    while len(xx) > 0:
        v1 = xx.pop(0)
        v2 = xx.pop(0)
        batch.append_cypher("START n=node(" + str(v1._id) + "), t=node(" + str(v2._id) + ") CREATE (n)-[:COMMENTED]->(t)")
    batch.run()
    batch.clear()
    end_time = time.clock()
    print "Created COMMENTED edges in %s seconds . . ." % str(end_time-start_time)

    #######################################################################

    with open('../data/comment_replyOf_comment.csv') as res:
        content = res.read()
        lines = content.split('\n')
        lines = [x for x in lines if x is not '']
        lines = lines[1:]

    start_time = time.clock()
    print "Started REPLY_OF edges . . .\n"
    ctr = 0
    for line in lines:
        parts = line.split('|')
        reply = int(parts[0])
        original = int(parts[1])
        batch.get_or_create_in_index(neo4j.Node, "Comments_TMP", "id", reply, {"id": reply, "type": "Comment"})
        batch.get_or_create_in_index(neo4j.Node, "Comments_TMP", "id", original, {"id": original, "type": "Comment"})
        ctr += 1
        if ctr > 10000:
            xx = batch.submit()
            batch.clear()
            while len(xx) > 0:
                v1 = xx.pop(0)
                v2 = xx.pop(0)
                #batch.create(rel(v1, "REPLY_OF", v2))
                batch.append_cypher("START n=node(" + str(v1._id) + "), t=node(" + str(v2._id) + ") CREATE (n)-[:REPLY_OF]->(t)")
            batch.run()
            batch.clear()
            ctr = 0
            print "Done 10,000 batch..."
    xx = batch.submit()
    batch.clear()
    while len(xx) > 0:
        v1 = xx.pop(0)
        v2 = xx.pop(0)
        batch.append_cypher("START n=node(" + str(v1._id) + "), t=node(" + str(v2._id) + ") CREATE (n)-[:REPLY_OF]->(t)")
    batch.run()
    batch.clear()
    end_time = time.clock()
    print "Created REPLY_OF edges in %s seconds. . ." % str(end_time-start_time)

    pass

if __name__ == '__main__':
    main()
