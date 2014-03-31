__author__ = 'Saksham'


from py2neo import neo4j, rel

def main():

    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

    people = graph_db.get_or_create_index(neo4j.Node, "People")

    interest_tags = graph_db.get_or_create_index(neo4j.Node, "Interest")

    forums = graph_db.get_or_create_index(neo4j.Node, "Forum")

    batch = neo4j.WriteBatch(graph_db)

    batch2 = neo4j.ReadBatch(graph_db)

    with open('../data/forum.csv') as res:
        content = res.read()
        lines = content.split('\n')
        lines = [x for x in lines if x is not '']
        lines = lines[1:]

    for line in lines:
        parts = line.split('|')
        forum_id = int(parts[0])
        forum_title = parts[1]
        forum_title = ''.join([i if ord(i) < 128 else '' for i in forum_title])
        batch.get_or_create_in_index(neo4j.Node, "Forum", "id", forum_id, {"id": forum_id, "title": forum_title})

    batch.submit()
    batch.clear()

    #create tag->forum edges
    with open('../data/forum_hasTag_tag.csv') as res:
        content = res.read()
        lines = content.split('\n')
        lines = [x for x in lines if x is not '']
        lines = lines[1:]

    for line in lines:
        parts = line.split('|')
        forum_id = int(parts[0])
        interest_tag_id = int(parts[1])
        batch2.get_indexed_nodes(forums, "id", forum_id)
        batch2.get_indexed_nodes(interest_tags, "id", interest_tag_id)
    edges = batch2.submit()
    batch2.clear()

    i = 0
    while i < len(edges):
        batch.create(rel(edges[i+1][0], "IS_PRESENT_IN", edges[i][0]))
        i += 2
    batch.run()
    batch.clear()

    # create people->forum edges
    with open('../data/forum_hasMember_person.csv') as res:
        content = res.read()
        lines = content.split('\n')
        lines = [x for x in lines if x is not '']
        lines = lines[1:]

    person_id_node_map = {}
    forum_id_node_map = {}
    for line in lines:
        parts = line.split('|')
        person_id = int(parts[1])
        forum_id = int(parts[0])
        if not forum_id_node_map.has_key(forum_id):
            forum_node = forums.get("id", forum_id)
            forum_id_node_map[forum_id] = forum_node[0]
        if not person_id_node_map.has_key(person_id):
            person_node = people.get("id", person_id)
            person_id_node_map[person_id] = person_node[0]

    ctr = 0
    for line in lines:
        parts = line.split('|')
        person_id = int(parts[1])
        forum_id = int(parts[0])
        batch.create(rel(person_id_node_map[person_id], "IS_MEMBER_OF", forum_id_node_map[forum_id]))
        ctr += 1
        if ctr > 1000:
            batch.run()
            batch.clear()
            ctr = 0
    batch.run()
    batch.clear()

    pass


if __name__ == '__main__':
    main()
