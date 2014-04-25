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

    start_time = time.clock()
    # get all nodes that have anything to do with Asia
    place_node = places.get("name", p)
    place_type = place_node[0]["type"]
    #place_id = place_node[0]["id"]

    place_satisfying_people = set()

    if place_type == 'city':
        place_satisfying_people = Locate_city_people(place_node[0], graph_db)

    elif place_type == 'country':
        place_satisfying_people = Locate_country_people(place_node[0], graph_db)

    elif place_type == 'continent':
        place_satisfying_people = Locate_continent_people(place_node[0], graph_db)

    person_interests_map = Get_interests_of_place_satisfiers(place_satisfying_people, graph_db)

    if place_type == 'continent':
        desc_sorted_interests = do_different(person_interests_map)
    else:
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


def path_exists(node1_id, node2_id, hops, graph_db):

    q1 = "START n=node:People('id:" + str(node1_id) + "'), t=node:People('id:" + str(
        node2_id) + "') MATCH p=shortestPath((n)-[:KNOWS*.." + str(hops) + "]->(t)) RETURN p"

    res = list(neo4j.CypherQuery(graph_db, q1).execute())

    if len(res) > 0:
        return True
    return False


def Rank_people_by_intersecting_interests(place_satisfying_people, person_interests_map):

    end_nodes_interesecting_interests_map = set()

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
            end_nodes_interesecting_interests_map.add((p1["id"], p2["id"], -1*intersecting_interest_count))

    end_nodes_interesecting_interests_list = list(end_nodes_interesecting_interests_map)
    desc_sorted_interests = sorted(end_nodes_interesecting_interests_list, key=itemgetter(2, 0, 1))

    return desc_sorted_interests

def do_different(person_interests_map):

    score_hash = {}
    interest_person_map = Get_reverse_map(person_interests_map)

    for (interest, people) in interest_person_map.items():
        people_list = list(people)
        for i in range(0, len(people_list)):
            px = people_list[i]["id"]
            for j in range(i+1, len(people_list)):
                py = people_list[j]["id"]
                if px > py:
                    if not score_hash.has_key((py, px)):
                        score_hash[(py, px)] = -1
                    else:
                        score_hash[(py, px)] += -1
                else:
                    if not score_hash.has_key((px, py)):
                        score_hash[(px, py)] = -1
                    else:
                        score_hash[(px, py)] += -1

    tuple_list = []
    for (k, v) in score_hash.items():
        tuple_list.append((k[0], k[1], v))
    sorted_score_hash = sorted(tuple_list, key=itemgetter(2, 0, 1))
    return sorted_score_hash

def Get_reverse_map(person_interests_map):

    interest_person_map = {}

    for (person, interests) in person_interests_map.items():
        interest_list = list(interests)
        for interest in interest_list:
            if not interest_person_map.has_key(interest):
                people = set()
                people.add(person)
                interest_person_map[interest] = people
            else:
                interest_person_map[interest].add(person)
    return interest_person_map

def Get_interests_of_place_satisfiers(place_satisfying_people, graph_db):
    person_interests_map = {}

    for person in place_satisfying_people:
        q1 = "START n=node(" + str(person._id) + ") MATCH (n)-[:HAS_INTEREST]->(i) RETURN i"
        interests_full = list(neo4j.CypherQuery(graph_db, q1).execute())
        interests = set()
        for interest in interests_full:
            interests.add(interest.i)
        person_interests_map[person] = interests
    return person_interests_map


def Locate_continent_people(continent_node, graph_db):
    place_satisfying_people = set()

    # add people directly associated with continent
    place_satisfying_people.update(Locate_people_given_place_id(continent_node, graph_db))

    q1 = "START n=node(" + str(continent_node._id) + ") MATCH (t)-[:PART_OF]->(n) return t"
    countries_full = list(neo4j.CypherQuery(graph_db, q1).execute())
    countries = []
    for country in countries_full:
        countries.append(country.t)

    for country in countries:
        place_satisfying_people.update(Locate_country_people(country, graph_db))

    return place_satisfying_people


def Locate_country_people(country_node, graph_db):
    place_satisfying_people = set()

    # add people directly associated with country
    place_satisfying_people.update(Locate_people_given_place_id(country_node, graph_db))

    q1 = "START n=node(" + str(country_node._id) + ") MATCH (t)-[:PART_OF]->(n) return t"
    cities_full = list(neo4j.CypherQuery(graph_db, q1).execute())
    cities = []
    for city in cities_full:
        cities.append(city.t)

    for city in cities:
        place_satisfying_people.update(Locate_city_people(city, graph_db))

    return place_satisfying_people


def Locate_city_people(city_node, graph_db):
    place_satisfying_people = Locate_people_given_place_id(city_node, graph_db)
    return place_satisfying_people


def Locate_people_given_place_id(place_node, graph_db):

    rd_batch = neo4j.ReadBatch(graph_db)
    place_satisfying_people = set()

    q1 = "START n=node(" + str(place_node._id) + ") MATCH (t)-[:LIVES_IN]->(n) return t"
    rd_batch.append_cypher(q1)

    q2 = "START n=node(" + str(place_node._id) + ") MATCH (t)-[:STUDIED_IN]->(n) return t"
    rd_batch.append_cypher(q2)

    q3 = "START n=node(" + str(place_node._id) + ") MATCH (t)-[:WORKED_IN]->(n) return t"
    rd_batch.append_cypher(q3)

    results = rd_batch.submit()
    rd_batch.clear()

    while len(results) > 0:
        res = results.pop(0)
        if res is not None:
            if type(res) is list:
                for x in res:
                    place_satisfying_people.add(x.t)
            elif type(res) is neo4j.Node:
                    place_satisfying_people.add(res)
            else:
                pass

    return place_satisfying_people


if __name__ == '__main__':
    main(7, 6,'Ankara')