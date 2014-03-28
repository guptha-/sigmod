__author__ = 'Saksham'

import py2neo
from py2neo import neo4j, node, rel, cypher


def main():
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
    #graph_db.clear()

    people = graph_db.get_or_create_index(neo4j.Node, "People")
    comments = graph_db.get_or_create_index(neo4j.Node, "Comments")
    comment_reply_edges = graph_db.get_or_create_index(neo4j.Relationship, "Reply_Edges")

    people_nodes = 0

    batch = neo4j.WriteBatch(graph_db)

    with open('../data/person_knows_person.csv') as res:
        content = res.read()
        lines = content.split('\n')
        lines = [x for x in lines if x is not '']
        lines = lines[1:]

    uniq_pids = {}
    for line in lines:
        parts = line.split('|')
        first = int(parts[0])
        second = int(parts[1])

        if not uniq_pids.has_key(first):
            uniq_pids[first] = True
        if not uniq_pids.has_key(second):
            uniq_pids[second] = True

    print "Started people nodes . . .\n"
    for key in uniq_pids:
        batch.get_or_create_in_index(neo4j.Node, "People", "id", key, {"id": key, "type": "Person"})

    del uniq_pids
    batch.run()
    batch.clear()
    print "Created people nodes . . .\n"

    print "Started KNOWS edges . . .\n"
    for line in lines:
        parts = line.split('|')
        first = int(parts[0])
        second = int(parts[1])

        a = people.get("id", first)
        b = people.get("id", second)

        batch.create(rel(a[0], "KNOWS", b[0]))

    batch.run()
    batch.clear()
    print "Created KNOWS edges . . .\n"

    # #######################################################################
    #
    uniq_comments = {}
    with open('../data/comment_hasCreator_person.csv') as res:
        content = res.read()
        lines = content.split('\n')
        lines = [x for x in lines if x is not '']
        lines = lines[1:]
    for line in lines:
        parts = line.split('|')
        cid = int(parts[0])
        pid = int(parts[1])

        if not uniq_comments.has_key(cid):
            uniq_comments[cid] = pid
    #
    print "Started comment nodes . . .\n"
    ctr = 0
    for key in uniq_comments:
        batch.get_or_create_in_index(neo4j.Node, "Comments", "id", key, {"id": key, "type": "Comment"})
        ctr += 1
        if ctr >= 10000:
            batch.run()
            batch.clear()
            print "Done 10K batch . . ."
            ctr = 0

    batch.run()
    batch.clear()
    print "Created comment nodes . . .\n"

    print "Started COMMENTED edges . . .\n"
    ctr = 0
    for cid, pid in uniq_comments.items():
        a = comments.get("id", cid)
        b = people.get("id", pid)

        if b.__len__() == 0:
            batch.get_or_create_in_index(neo4j.Node, "People", "id", pid, {"id": pid, "type": "Person"})
            batch.run()
            batch.clear()
            ctr = 0
            b = people.get("id", pid)

        if a.__len__() == 0:
            batch.get_or_create_in_index(neo4j.Node, "Comments", "id", cid, {"id": cid, "type": "Comment"})
            batch.run()
            batch.clear()
            ctr = 0
            a = comments.get("id", cid)

        batch.create(rel(b[0], "COMMENTED", a[0]))
        ctr += 1
        if ctr >= 10000:
            batch.run()
            batch.clear()
            print "Done 10K batch . . ."
            ctr = 0

    del uniq_comments
    batch.run()
    batch.clear()
    print "Created COMMENTED edges . . ."

    #######################################################################

    with open('../data/comment_replyOf_comment.csv') as res:
        content = res.read()
        lines = content.split('\n')
        lines = [x for x in lines if x is not '']
        lines = lines[1:]

    print "Started REPLY_OF edges . . .\n"
    ctr = 0
    for line in lines:
        parts = line.split('|')
        reply = int(parts[0])
        original = int(parts[1])

        a = comments.get("id", reply)
        b = comments.get("id", original)

        if a.__len__() == 0:
            batch.get_or_create_in_index(neo4j.Node, "Comments", "id", reply, {"id": reply, "type": "Comment"})
            batch.run()
            batch.clear()
            ctr = 0
            a = comments.get("id", reply)
        if b.__len__() == 0:
            batch.get_or_create_in_index(neo4j.Node, "Comments", "id", original, {"id": original, "type": "Comment"})
            batch.run()
            batch.clear()
            ctr = 0
            b = comments.get("id", original)

        batch.create(rel(a[0], "REPLY_OF", b[0]))
        ctr += 1
        if ctr > 5000:
            batch.run()
            batch.clear()
            print "Comepled 5K batch . . ."
            ctr = 0

    batch.run()
    batch.clear()
    print "Created REPLY_OF edges . . ."

    pass

if __name__ == '__main__':
    main()
