__author__ = 'Saksham'

import py2neo
import time
from collections import OrderedDict
from operator import itemgetter
from py2neo import neo4j, node, rel, cypher


def main():
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

    people = graph_db.get_or_create_index(neo4j.Node, "People")

    interest_tags = graph_db.get_or_create_index(neo4j.Node, "Interest")

    places = graph_db.get_or_create_index(neo4j.Node, "Place")

    hops = 4
    k = 5
    p = 'Chengdu'

    # get all nodes that have anything to do with Asia
    place_node = places.get("name", p)
    place_type = place_node[0]["type"]
    place_id = place_node[0]["id"]

    place_satisfying_people = set()

    if place_type == 'city':
        place_satisfying_people = Locate_city_people(place_id, graph_db)

    elif place_type == 'country':
        place_satisfying_people = Locate_country_people(place_id, graph_db)

    elif place_type == 'continent':
        place_satisfying_people = Locate_continent_people(place_id, graph_db)

    person_interests_map = Get_interests_of_place_satisfiers(place_satisfying_people, graph_db)

    #Remove_people_with_no_common_interests(place_satisfying_people, person_interests_map)
    #Remove_people_not_in_proximity(place_satisfying_people, person_interests_map, graph_db)


    desc_sorted_interests = Rank_people_by_intersecting_interests(place_satisfying_people, person_interests_map)

    final_couples = []
    ctr = 0
    iterator = 0
    while ctr < k:
        node1_id = desc_sorted_interests[iterator][0]
        node2_id = desc_sorted_interests[iterator][1]
        common_tags = -1*desc_sorted_interests[iterator][2]
        if path_exists(node1_id, node2_id, hops, graph_db):
            final_couples.append((node1_id, node2_id, common_tags))
            ctr += 1
        iterator += 1
    pass

    edge_dict = {}
    for person in place_satisfying_people:
        (k, v) = bfs_per_node(person, place_satisfying_people, person_interests_map, hops, graph_db).items()[0]
        edge_dict[k] = v

    desc_sorted = sorted(edge_dict, key=lambda x: edge_dict[x], reverse=True)

    pass

    return place_satisfying_people

def path_exists(node1_id, node2_id, hops, graph_db):

    q1 = "START n=node:People('id:" + str(node1_id) + "'), t=node:People('id:" + str(
        node2_id) + "') MATCH p=shortestPath((n)-[:KNOWS*.." + str(hops) + "]->(t)) RETURN p"

    res = list(neo4j.CypherQuery(graph_db, q1).execute())

    if len(res) > 0:
        return True
    return False


def Rank_people_by_intersecting_interests(place_satisfying_people, person_interests_map):

    end_nodes_interesecting_interests_map = []

    node_id_map = {}
    for person in place_satisfying_people:
        node_id_map[person] = person["id"]
    place_satisfying_people_list = sorted(node_id_map, key=lambda x: node_id_map[x])

    for i in range(0, len(place_satisfying_people_list)):
        p1 = place_satisfying_people_list[i]
        for j in range(i + 1, len(place_satisfying_people_list)):
            p2 = place_satisfying_people_list[j]
            p1_interests = person_interests_map[p1]
            p2_interests = person_interests_map[p2]
            intersecting_interest_count = len(p1_interests & p2_interests)
            end_nodes_interesecting_interests_map.append((p1["id"], p2["id"], -1*intersecting_interest_count))

    desc_sorted_interests = sorted(end_nodes_interesecting_interests_map, key=itemgetter(2, 0, 1))

    return desc_sorted_interests


def Remove_people_with_no_common_interests(place_satisfying_people, person_interests_map):
    interest_tags = {}

    for person, interests in person_interests_map.items():
        for interest in interests:
            if not interest_tags.has_key(interest):
                interest_tags[interest] = [person]
            else:
                interest_tags[interest].append(person)

    for interest, folks in interest_tags.items():
        if len(folks) < 2:
            person = folks[0]
            if person_interests_map.has_key(person):
                person_interests = person_interests_map[person]
                flag = False
                for intr in person_interests:
                    if len(interest_tags[intr]) > 1:
                        flag = True
                if not flag:
                    if person in place_satisfying_people:
                        place_satisfying_people.remove(person)
                        del person_interests_map[person]

    return


def Remove_people_not_in_proximity(place_satisfying_people, person_interests_map, graph_db):
    lonely_people = set()

    start_time = time.time()

    for person in place_satisfying_people:
        q1 = "START n=node:People('id:" + str(person["id"]) + "') MATCH (n)-[:KNOWS*..2]->(t) return t"
        friend_range_full = list(neo4j.CypherQuery(graph_db, q1).execute())
        friend_range = set()
        for friend in friend_range_full:
            friend_range.add(friend.t)
        useful_friends = friend_range & place_satisfying_people
        if len(useful_friends) == 0:
            lonely_people.add(person)

    diff = time.time() - start_time

    for person in lonely_people:
        place_satisfying_people.remove(person)
        del person_interests_map[person]

    return


def Get_interests_of_place_satisfiers(place_satisfying_people, graph_db):
    person_interests_map = {}

    for person in place_satisfying_people:
        person_id = person["id"]
        q1 = "START n=node:People('id:" + str(person_id) + "') MATCH (n)-[:HAS_INTEREST]->(i) RETURN i"
        interests_full = list(neo4j.CypherQuery(graph_db, q1).execute())
        interests = set()
        for interest in interests_full:
            interests.add(interest.i)
        person_interests_map[person] = interests

    return person_interests_map


def bfs_per_node(start_person, place_satisfying_people, person_interests_map, hops, graph_db):
    queue = [start_person]
    traversed_nodes = set()
    traversed_nodes.add(start_person)
    node_hop_map = {}
    node_hop_map[start_person] = 0

    max_interest_match_count = 0
    max_interest_match_node = None

    while len(queue) > 0:
        next_node = queue.pop(0)
        node_id = next_node["id"]

        if (next_node != start_person) and (next_node in place_satisfying_people):
            start_person_interests = person_interests_map[start_person]
            next_node_interests = person_interests_map[next_node]
            common_interests = start_person_interests & next_node_interests
            if len(common_interests) > max_interest_match_count:
                max_interest_match_count = len(common_interests)
                max_interest_match_node = next_node

        next_node_hops_from_start = node_hop_map[next_node]

        if next_node_hops_from_start < hops:
            q = "START n=node:People('id:" + str(node_id) + "')" \
                                                            "MATCH (n)-[r:KNOWS]->(t) RETURN t"
            res = list(neo4j.CypherQuery(graph_db, q).execute())
            all_neighbors = set()
            for entry in res:
                all_neighbors.add(entry.t)
            new_neighbors = (all_neighbors - traversed_nodes)

            for neighbor in new_neighbors:
                node_hop_map[neighbor] = node_hop_map[next_node] + 1
                queue.append(neighbor)
                traversed_nodes.add(neighbor)

    edge_hash = {(start_person, max_interest_match_node): max_interest_match_count}

    return edge_hash


def Locate_continent_people(continent_id, graph_db):
    place_satisfying_people = set()

    # add people directly associated with continent
    place_satisfying_people.update(Locate_people_given_place_id(continent_id, graph_db))

    q1 = "START n=node:Place('id:" + str(continent_id) + "') MATCH (t)-[:PART_OF]->(n) return t"
    countries_full = list(neo4j.CypherQuery(graph_db, q1).execute())
    countries = []
    for country in countries_full:
        countries.append(country.t)

    for country in countries:
        country_id = country["id"]
        place_satisfying_people.update(Locate_country_people(country_id, graph_db))

    return place_satisfying_people


def Locate_country_people(country_id, graph_db):
    place_satisfying_people = set()

    # add people directly associated with country
    place_satisfying_people.update(Locate_people_given_place_id(country_id, graph_db))

    q1 = "START n=node:Place('id:" + str(country_id) + "') MATCH (t)-[:PART_OF]->(n) return t"
    cities_full = list(neo4j.CypherQuery(graph_db, q1).execute())
    cities = []
    for city in cities_full:
        cities.append(city.t)

    for city in cities:
        city_id = city["id"]
        place_satisfying_people.update(Locate_city_people(city_id, graph_db))

    return place_satisfying_people


def Locate_city_people(city_id, graph_db):
    place_satisfying_people = Locate_people_given_place_id(city_id, graph_db)
    return place_satisfying_people


def Locate_people_given_place_id(place_id, graph_db):
    place_satisfying_people = set()

    q1 = "START n=node:Place('id:" + str(place_id) + "') MATCH (t)-[:LIVES_IN]->(n) return t"
    living_people = list(neo4j.CypherQuery(graph_db, q1).execute())
    for folk in living_people:
        place_satisfying_people.add(folk.t)

    q2 = "START n=node:Place('id:" + str(place_id) + "') MATCH (t)-[:STUDIED_IN]->(n) return t"
    studying_people = list(neo4j.CypherQuery(graph_db, q2).execute())
    for folk in studying_people:
        place_satisfying_people.add(folk.t)

    q3 = "START n=node:Place('id:" + str(place_id) + "') MATCH (t)-[:WORKED_IN]->(n) return t"
    working_people = list(neo4j.CypherQuery(graph_db, q3).execute())
    for folk in working_people:
        place_satisfying_people.add(folk.t)

    return place_satisfying_people


if __name__ == '__main__':
    main()