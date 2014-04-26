__author__ = 'Saksham'

from py2neo import neo4j, node, rel, cypher
import datetime


def main():
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

    people = graph_db.get_or_create_index(neo4j.Node, "People")

    places = graph_db.get_or_create_index(neo4j.Node, "Place")

    batch = neo4j.WriteBatch(graph_db)

    res = open("../data/place.csv", "r")
    flag = True
    for line in res:
        line = line.rstrip('\n')
        if flag or line is '':
            flag = False
            continue
        parts = line.split('|')
        place_id = int(parts[0])
        place_name = parts[1]
        place_type = parts[3]

        place_name = ''.join([i if ord(i) < 128 else '' for i in place_name])

        # indexing only on the id
        batch.get_or_create_in_index(neo4j.Node, "Place", "id", place_id,
                                     {"id": place_id, "name": place_name, "type": place_type})

    res.close()
    xx = batch.submit()
    batch.clear()

    while len(xx) > 0:
        v = xx.pop(0)
        batch.add_indexed_node("Place", "name", str(v["name"]), v)
    batch.submit()
    batch.clear()

    ###########################################################################
    readbatch = neo4j.ReadBatch(graph_db)
    res = open("../data/place_isPartOf_place.csv", "r")
    flag = True
    for line in res:
        line = line.rstrip('\n')
        if flag or line is '':
            flag = False
            continue
        parts = line.split('|')
        smaller_place_id = int(parts[0])
        larger_place_id = int(parts[1])

        readbatch.get_indexed_nodes("Place", "id", smaller_place_id)
        readbatch.get_indexed_nodes("Place", "id", larger_place_id)

    res.close()
    responses = readbatch.submit()
    readbatch.clear()

    x = 0
    while x < responses.__len__():
        batch.create(rel(responses[x][0], "PART_OF", responses[x + 1][0]))
        x += 2

    batch.submit()
    batch.clear()

    #####################################################################################
    res = open("../data/person_isLocatedIn_place.csv", "r")
    flag = True
    for line in res:
        line = line.rstrip('\n')
        if flag or line is '':
            flag = False
            continue
        parts = line.split('|')
        person_id = int(parts[0])
        place_id = int(parts[1])
        readbatch.get_indexed_nodes("People", "id", person_id)
        readbatch.get_indexed_nodes("Place", "id", place_id)

    res.close()
    responses = readbatch.submit()
    readbatch.clear()

    x = 0
    while x < responses.__len__():
        batch.create(rel(responses[x][0], "LIVES_IN", responses[x + 1][0]))
        x += 2

    batch.submit()
    batch.clear()

    ####################################################################################

    person_study_org_map = {}
    person_work_org_map = {}
    org_place_map = {}

    res = open("../data/organisation_isLocatedIn_place.csv", "r")
    flag = True
    for line in res:
        line = line.rstrip('\n')
        if flag or line is '':
            flag = False
            continue
        parts = line.split('|')
        org_id = int(parts[0])
        place_id = int(parts[1])
        org_place_map[org_id] = place_id

    res = open("../data/person_studyAt_organisation.csv", "r")
    flag = True
    for line in res:
        line = line.rstrip('\n')
        if flag or line is '':
            flag = False
            continue
        parts = line.split('|')
        person_id = int(parts[0])
        study_org_id = int(parts[1])
        person_study_org_map[person_id] = study_org_id

    # Have to use a separate loop here because of duplicates (I think!)
    for person, study_org in person_study_org_map.items():
        study_org_place = org_place_map[study_org]
        readbatch.get_indexed_nodes("People", "id", person)
        readbatch.get_indexed_nodes("Place", "id", study_org_place)

    responses = readbatch.submit()
    readbatch.clear()

    x = 0
    while x < responses.__len__():
        batch.create(rel(responses[x][0], "STUDIED_IN", responses[x + 1][0]))
        x += 2

    batch.submit()
    batch.clear()

    res = open("../data/person_workAt_organisation.csv", "r")
    flag = True
    for line in res:
        line = line.rstrip('\n')
        if flag or line is '':
            flag = False
            continue
        parts = line.split('|')
        person_id = int(parts[0])
        work_org_id = int(parts[1])
        person_work_org_map[person_id] = work_org_id

    # Have to use a separate loop here because of duplicates (I think!)
    for person, work_org in person_work_org_map.items():
        work_org_place = org_place_map[work_org]
        readbatch.get_indexed_nodes("People", "id", person)
        readbatch.get_indexed_nodes("Place", "id", work_org_place)

    responses = readbatch.submit()
    readbatch.clear()

    x = 0
    while x < responses.__len__():
        batch.create(rel(responses[x][0], "WORKED_IN", responses[x + 1][0]))
        x += 2

    batch.submit()
    batch.clear()

if __name__ == '__main__':
    main()