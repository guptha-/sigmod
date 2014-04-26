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

    res = open("../data/person.csv", "r")
    flag = True
    for line in res:
        line = line.rstrip('\n')
        if flag or line is '':
            flag = False
            continue

        # edit person nodes to add birth-dates
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
            batch.submit()
            batch.clear()
            node_ref = people.get("id", id)

        batch.set_property(node_ref[0], "birthday", absolute_day)
        batch.add_indexed_node("Birthdays", "birthday", absolute_day, node_ref[0])

    batch.submit()
    batch.clear()

    unique_interest_tags = {}
    res = open("../data/person_hasInterest_tag.csv", "r")
    flag = True
    for line in res:
        line = line.rstrip('\n')
        if flag or line is '':
            flag = False
            continue
        parts = line.split('|')
        pid = int(parts[0])
        interest_id = int(parts[1])
        if not unique_interest_tags.has_key(interest_id):
            persons = [pid]
            unique_interest_tags[interest_id] = persons
            batch.get_or_create_in_index(neo4j.Node, "Interest", "id", interest_id, {"id": interest_id, "type": "Tag"})
        else:
            unique_interest_tags[interest_id].append(pid)

    xx = batch.submit()
    batch.clear()

    readbatch = neo4j.ReadBatch(graph_db)
    while len(xx) > 0:
        v = xx.pop(0)
        persons = unique_interest_tags[v["id"]]
        for person in persons:
            readbatch.get_indexed_nodes("People", "id", person)

        responses = readbatch.submit()
        readbatch.clear()

        for x in range(0, responses.__len__()):
            batch.create(rel(responses[x][0], "HAS_INTEREST", v))
    batch.submit()
    batch.clear()

    add_names_of_interest_tags_to_nodes(batch, interest_tags)


def add_names_of_interest_tags_to_nodes(batch, interest_tags):
    res = open("../data/tag.csv", "r")
    flag = True
    for line in res:
        line = line.rstrip('\n')
        if flag or line is '':
            flag = False
            continue
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
