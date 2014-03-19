__author__ = 'Saksham'

from py2neo import neo4j, node, rel, cypher
import datetime
from datetime import date


def main():
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

    people = graph_db.get_or_create_index(neo4j.Node, "People")

    interest_tags = graph_db.get_or_create_index(neo4j.Node, "Interest")

    birthdays = graph_db.get_or_create_index(neo4j.Node, "Birthdays")

    batch = neo4j.WriteBatch(graph_db)

    with open('person.csv') as res:
        content = res.read()
        lines = content.split('\n')
        lines = [x for x in lines if x is not '']
        lines = lines[1:]

    with open('person_hasInterest_tag.csv') as res:
        content = res.read()
        lines1 = content.split('\n')
        lines1 = [x for x in lines1 if x is not '']
        lines1 = lines1[1:]

    unique_interest_tags = {}
    for line in lines1:
        parts = line.split('|')
        pid = int(parts[0])
        interest_id = int(parts[1])
        if not unique_interest_tags.has_key(interest_id):
            persons = [pid]
            unique_interest_tags[interest_id] = persons
        else:
            unique_interest_tags[interest_id].append(pid)

    # create tag nodes
    for tag, persons in unique_interest_tags.items():
        batch.get_or_create_in_index(neo4j.Node, "Interest", "id", tag, {"id": tag, "type": "Tag"})
    batch.submit()
    batch.clear()

    add_names_of_interest_tags_to_nodes(batch, interest_tags)

    # edit person nodes to add birth-dates
    for line in lines:
        parts = line.split('|')
        id = int(parts[0])
        date = parts[4].split('-')

        year = int(date[0])
        month = int(date[1])
        day = int(date[2])

        absolute_day = datetime.date(year, month, day).toordinal()

        node_ref = people.get("id", id)

        if node_ref.__len__() == 0:
            pass
            batch.get_or_create_in_index(neo4j.Node, "People", "id", id, {"id": id, "type": "Person"})
            batch.run()
            batch.clear()
            node_ref = people.get("id", id)

        batch.set_property(node_ref[0], "birthday", absolute_day)
        batch.add_indexed_node("Birthdays", "birthday", absolute_day, node_ref[0])

    batch.submit()
    batch.clear()

    for tag, persons in unique_interest_tags.items():
        for person in persons:
            person_node_ref = people.get("id", person)
            tag_node_ref = interest_tags.get("id", tag)
            batch.create(rel(person_node_ref[0], "HAS_INTEREST", tag_node_ref[0]))
    batch.submit()
    batch.clear()

def add_names_of_interest_tags_to_nodes(batch, interest_tags):

    with open('tag.csv') as res:
        content = res.read()
        lines = content.split('\n')
        lines = [x for x in lines if x is not '']
        lines = lines[1:]

    for line in lines:
        parts = line.split('|')
        id = int(parts[0])
        name = parts[1]
        name = ''.join([i if ord(i) < 128 else '' for i in name])
        i_node = interest_tags.get("id", id)

        if i_node.__len__() > 0:
            batch.set_property(i_node[0], "name", name)

    batch.submit()
    batch.clear()

if __name__ == '__main__':
    main()
