__author__ = 'Saksham'

from operator import itemgetter
from py2neo import neo4j
import time

def main(k_str, hops_str, p):
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

    people = graph_db.get_or_create_index(neo4j.Node, "People")

    interest_tags = graph_db.get_or_create_index(neo4j.Node, "Interest")

    places = graph_db.get_or_create_index(neo4j.Node, "Place")

    k = int(k_str)
    hops = int(hops_str)

    # get all nodes that have anything to do with Asia
    start_time = time.clock()
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

    desc_sorted_interests = Rank_people_by_intersecting_interests(place_satisfying_people, person_interests_map)

    final_couples = []
    ctr = 0
    iterator = 0
    while ctr < k:
        if (iterator >= desc_sorted_interests.__len__()):
            break
        node1_id = desc_sorted_interests[iterator][0]
        node2_id = desc_sorted_interests[iterator][1]
        common_tags = -1*desc_sorted_interests[iterator][2]
        if path_exists(node1_id, node2_id, hops, graph_db):
            final_couples.append((node1_id, node2_id, common_tags))
            ctr += 1
        iterator += 1
    end_time = time.clock()
    print "Time taken %s seconds..." % str(end_time-start_time)
    if final_couples.__len__() == 0:
        print "There are no pairs with a path between them"
    for (n1, n2, common_tags) in final_couples:
        print "%s|%s have %d common interests" % (n1, n2, common_tags)

    print "\n"
    pass


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
    main(3, 2,'Asia')