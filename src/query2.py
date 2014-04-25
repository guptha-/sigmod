__author__ = 'Saksham'

import sys
import py2neo
from operator import itemgetter
from py2neo import neo4j, node, rel, cypher
import datetime, time


def main(d_str, k_str):
    global range_of_tag
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

    people = graph_db.get_or_create_index(neo4j.Node, "People")

    interest_tags = graph_db.get_or_create_index(neo4j.Node, "Interest")

    birthdays = graph_db.get_or_create_index(neo4j.Node, "Birthdays")

    start_time = time.clock()
    d = int(d_str)
    k = int(k_str)
    # get hold of all nodes with birthday > d
    q1 = "birthday:[" + str(d) + " TO 735311]"
    valid_birthday_nodes = list(birthdays.query(q1))
    valid_birthday_nodes_set = set()
    for vbd in valid_birthday_nodes:
        valid_birthday_nodes_set.add(vbd)

    # get hold of all interest nodes
    q2 = "START i=node:Interest('*:*') return distinct i"
    res2 = list(neo4j.CypherQuery(graph_db, q2).execute())

    ranges_for_interests = []
    for node_interest in res2:
        interest_node = node_interest.i
        connected_components = func(interest_node, valid_birthday_nodes_set, graph_db)
        range_of_tag = find_largest_connected_component(connected_components)
        ranges_for_interests.append((-1*range_of_tag, interest_node["name"]))

    desc_sorted_ranges = sorted(ranges_for_interests, key=itemgetter(0, 1))

    end_time = time.clock()
    print "Time takes %s seconds..." % str(end_time - start_time)
    print_str = ""
    for i in range(0, k):
        print_str += "=> %s-%d \n" % (desc_sorted_ranges[i][1], -1*desc_sorted_ranges[i][0])
    print print_str

    pass


def func(interest_node, valid_birthday_nodes, graph_db):

    # get all people with connection to this interest node
    interest_node_id = interest_node["id"]
    q1 = "START i=node(" + str(interest_node._id) + ") MATCH (n)-[r:HAS_INTEREST]->(i) return n"
    #q1 = "START i=node:Interest('id:" + str(interest_node_id) + "') MATCH (n)-[r:HAS_INTEREST]->(i) return n"
    res1 = list(neo4j.CypherQuery(graph_db, q1).execute())

    # add these people to a set
    people_with_same_interest = set()
    for people_node in res1:
        person = people_node.n
        people_with_same_interest.add(person)

    # intersect these with nodes satisfying the birthday condition
    final_people_nodes = valid_birthday_nodes & people_with_same_interest

    connected_components = {}
    covered_people = set()
    for curr_person in final_people_nodes:
        if curr_person not in covered_people:
            connected_set = bfs_per_node(curr_person, final_people_nodes, graph_db)
            connected_components[curr_person] = connected_set
            for elem in connected_set:
                covered_people.add(elem)

    return connected_components


def bfs_per_node(start_person, final_people_nodes, graph_db):
    connected_component_queue = list()
    traversed_nodes = set()

    connected_component_queue.append(start_person)
    traversed_nodes.add(start_person)

    while len(connected_component_queue) > 0:
        next_node = connected_component_queue.pop(0)
        #node_id = next_node["id"]
        #q = "START n=node:People('id:" + str(node_id) + "')" \
        #    "MATCH (n)-[r:KNOWS]->(t) RETURN t"
        q = "START n=node(" + str(next_node._id) + ") MATCH (n)-[r:KNOWS]->(t) RETURN t"
        res = list(neo4j.CypherQuery(graph_db, q).execute())
        all_neighbors = set()
        for entry in res:
            all_neighbors.add(entry.t)
        new_neighbors = (all_neighbors & final_people_nodes) - traversed_nodes
        for neighbor in new_neighbors:
            connected_component_queue.append(neighbor)
            traversed_nodes.add(neighbor)

    return list(traversed_nodes)


def find_largest_connected_component(connected_components):

    max_size = 0
    for k, v in connected_components.items():
        if len(v) > max_size:
            max_size = len(v)
    return max_size



if __name__ == '__main__':
    main(datetime.date(1981, 3, 10).toordinal(), 4)
