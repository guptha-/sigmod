__author__ = 'Saksham'


from py2neo import neo4j, rel


def main(querydir):

    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

    people = graph_db.get_or_create_index(neo4j.Node, "People")

    interest_tags = graph_db.get_or_create_index(neo4j.Node, "Interest")

    forums = graph_db.get_or_create_index(neo4j.Node, "Forum")

    batch = neo4j.WriteBatch(graph_db)

    batch2 = neo4j.ReadBatch(graph_db)

    ctr = 0
    res = open("../data/" + querydir + "/forum.csv", "r")
    flag = True
    for line in res:
        line = line.rstrip('\n')
        if flag or line is '':
            flag = False
            continue
        parts = line.split('|')
        forum_id = int(parts[0])
        forum_title = parts[1]
        forum_title = ''.join([i if ord(i) < 128 else '' for i in forum_title])
        batch.get_or_create_in_index(neo4j.Node, "Forum", "id", forum_id, {"id": forum_id, "title": forum_title})
        ctr += 1
        if ctr > 10000:
            ctr = 0
            batch.submit()
            batch.clear()
            print "Batch done"

    res.close()
    batch.submit()
    batch.clear()
    print "Done forum load"

    #create tag->forum edges
    ctr = 0
    res = open("../data/" + querydir + "/forum_hasTag_tag.csv", "r")
    flag = True
    for line in res:
        line = line.rstrip('\n')
        if flag or line is '':
            flag = False
            continue
        parts = line.split('|')
        forum_id = int(parts[0])
        interest_tag_id = int(parts[1])
        batch2.get_indexed_nodes(forums, "id", forum_id)
        batch2.get_indexed_nodes(interest_tags, "id", interest_tag_id)
        ctr += 1
        if ctr > 10000:
            ctr = 0
            edges = batch2.submit()
            batch2.clear()
            while i < len(edges):
                batch.create(rel(edges[i+1][0], "IS_PRESENT_IN", edges[i][0]))
                i += 2
            batch.submit()
            batch.clear()
            print "Batch done"
    res.close()
    edges = batch2.submit()
    batch2.clear()

    i = 0
    while i < len(edges):
        batch.create(rel(edges[i+1][0], "IS_PRESENT_IN", edges[i][0]))
        i += 2
    batch.submit()
    batch.clear()
    print "Done is present in load"

    # create people->forum edges
    person_id_node_map = {}
    forum_id_node_map = {}

    # with open('../data/' + querydir + '/forum_hasMember_person.csv') as res:
    #     content = res.read()
    #     lines = content.split('\n')
    #     lines = [x for x in lines if x is not '']
    #     lines = lines[1:]
    #
    # for line in lines:

    # readbatch = neo4j.ReadBatch(graph_db)
    # ctr = 0
    # res = open("../data/" + querydir + "/forum_hasMember_person.csv", "r")
    # flag = True
    # for line in res:
    #     line = line.rstrip('\n')
    #     if flag or line is '':
    #         flag = False
    #         continue
    #     parts = line.split('|')
    #     person_id = int(parts[1])
    #     forum_id = int(parts[0])
    #     # if not forum_id_node_map.has_key(forum_id):
    #     #     forum_node = forums.get("id", forum_id)
    #     #     forum_id_node_map[forum_id] = forum_node[0]
    #     # if not person_id_node_map.has_key(person_id):
    #     #     person_node = people.get("id", person_id)
    #     #     person_id_node_map[person_id] = person_node[0]
    #     readbatch.get_indexed_nodes("People", "id", person_id)
    #     readbatch.get_indexed_nodes("Forum", "id", forum_id)
    #     ctr += 1
    #     if ctr > 10000:
    #         responses = readbatch.submit()
    #         readbatch.clear()
    #         ctr = 0
    #         x = 0
    #         while x < responses.__len__():
    #             batch.create(rel(responses[x][0], "IS_MEMBER_OF", responses[x + 1][0]))
    #             x += 2
    #         batch.submit()
    #         batch.clear()
    #         print "Batch done"
    #
    # res.close()
    # responses = readbatch.submit()
    # readbatch.clear()
    #
    # x = 0
    # while x < responses.__len__():
    #     batch.create(rel(responses[x][0], "IS_MEMBER_OF", responses[x + 1][0]))
    #     x += 2
    # batch.submit()
    # batch.clear()
    #
    # # for line in lines:
    # #     parts = line.split('|')
    # #     person_id = int(parts[1])
    # #     forum_id = int(parts[0])
    # #     batch.create(rel(person_id_node_map[person_id], "IS_MEMBER_OF", forum_id_node_map[forum_id]))
    # #     ctr += 1
    # #     if ctr > 1000:
    # #         batch.run()
    # #         batch.clear()
    # #         ctr = 0
    # # batch.run()
    # # batch.clear()
    # print "Done forum has member load"

    pass


if __name__ == '__main__':
    main("1k")
