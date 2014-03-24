__author__ = 'Saksham'

from py2neo import neo4j, node, rel, cypher
import datetime


def main():
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

    people = graph_db.get_or_create_index(neo4j.Node, "People")

    places = graph_db.get_or_create_index(neo4j.Node, "Place")

    batch = neo4j.WriteBatch(graph_db)

    with open('place.csv') as res:
        content = res.read()
        lines = content.split('\n')
        lines = [x for x in lines if x is not '']
        lines = lines[1:]

    #creating people nodes, indexed on their name
    for line in lines:
        parts = line.split('|')
        place_id = int(parts[0])
        place_name = parts[1]
        place_type = parts[3]

        place_name = ''.join([i if ord(i) < 128 else '' for i in place_name])

        # indexing only on the id
        batch.get_or_create_in_index(neo4j.Node, "Place", "id", place_id,
                                     {"id": place_id, "name": place_name, "type": place_type})
    batch.submit()
    batch.clear()

    # adding index on name of the place also (since in the actual query, name, not id would be given)
    for line in lines:
        parts = line.split('|')
        place_id = int(parts[0])
        place_name = parts[1]

        place_name = ''.join([i if ord(i) < 128 else '' for i in place_name])

        node_ref = places.get("id", place_id)
        batch.add_indexed_node("Place", "name", place_name, node_ref[0])
    batch.submit()
    batch.clear()

    ################################################################################

    with open('place_isPartOf_place.csv') as res:
        content = res.read()
        lines = content.split('\n')
        lines = [x for x in lines if x is not '']
        lines = lines[1:]

    # create "place is part of" edges
    for line in lines:
        parts = line.split('|')
        smaller_place_id = int(parts[0])
        larger_place_id = int(parts[1])

        small_place_node = places.get("id", smaller_place_id)
        large_place_node = places.get("id", larger_place_id)
        batch.create(rel(small_place_node[0], "PART_OF", large_place_node[0]))
    batch.submit()
    batch.clear()

    #####################################################################################

    with open('person_isLocatedIn_place.csv') as res:
        content = res.read()
        lines = content.split('\n')
        lines = [x for x in lines if x is not '']
        lines = lines[1:]

    for line in lines:
        parts = line.split('|')
        person_id = int(parts[0])
        place_id = int(parts[1])

        person_node = people.get("id", person_id)
        place_node = places.get("id", place_id)
        batch.create(rel(person_node[0], "LIVES_IN", place_node[0]))
    batch.submit()
    batch.clear()

    ####################################################################################

    person_study_org_map = {}
    person_work_org_map = {}
    org_place_map = {}

    with open('person_studyAt_organisation.csv') as res:
        content = res.read()
        lines = content.split('\n')
        lines = [x for x in lines if x is not '']
        lines = lines[1:]

    for line in lines:
        parts = line.split('|')
        person_id = int(parts[0])
        study_org_id = int(parts[1])
        person_study_org_map[person_id] = study_org_id

    with open('person_workAt_organisation.csv') as res:
        content = res.read()
        lines = content.split('\n')
        lines = [x for x in lines if x is not '']
        lines = lines[1:]

    for line in lines:
        parts = line.split('|')
        person_id = int(parts[0])
        work_org_id = int(parts[1])
        person_work_org_map[person_id] = work_org_id

    with open('organisation_isLocatedIn_place.csv') as res:
        content = res.read()
        lines = content.split('\n')
        lines = [x for x in lines if x is not '']
        lines = lines[1:]

    for line in lines:
        parts = line.split('|')
        org_id = int(parts[0])
        place_id = int(parts[1])
        org_place_map[org_id] = place_id

######################################################################

    for person, study_org in person_study_org_map.items():
        person_node_ref = people.get("id", person)
        study_org_place = org_place_map[study_org]
        place_node_ref = places.get("id", study_org_place)
        batch.create(rel(person_node_ref[0], "STUDIED_IN", place_node_ref[0]))
    batch.submit()
    batch.clear()

    for person, work_org in person_work_org_map.items():
        person_node_ref = people.get("id", person)
        work_org_place = org_place_map[work_org]
        place_node_ref = places.get("id", work_org_place)
        batch.create(rel(person_node_ref[0], "WORKED_IN", place_node_ref[0]))
    batch.submit()
    batch.clear()




if __name__ == '__main__':
    main()