__author__ = 'Saksham'

from py2neo import neo4j


def main():
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
    people = graph_db.get_or_create_index(neo4j.Node, "People")
    interest_tags = graph_db.get_or_create_index(neo4j.Node, "Interest")
    forums = graph_db.get_or_create_index(neo4j.Node, "Forum")

    k = 3
    interest_tag_name = 'Bill_Clinton'

    # get the tag node
    q1 = "START n=node:Interest('*:*') where n.name='" + interest_tag_name + "' RETURN n"
    tag_node_list = list(neo4j.CypherQuery(graph_db, q1).execute())
    tag_node = tag_node_list[0].n

    # get all forums with this interest tag
    q2 = "START n=node:Interest('id:" + str(tag_node["id"]) + "') MATCH (n)-[:IS_PRESENT_IN]->(t) RETURN t"
    forum_node_list = list(neo4j.CypherQuery(graph_db, q2).execute())
    valid_forum_nodes = []
    for node in forum_node_list:
        valid_forum_nodes.append(node.t)

    # get people who are members of these forums
    valid_people_nodes = set()
    for node in valid_forum_nodes:
        q3 = "START n=node:Forum('id:" + str(node["id"]) + "') MATCH (t)-[:IS_MEMBER_OF]->(n) RETURN t"
        people_node_list = list(neo4j.CypherQuery(graph_db, q3).execute())
        for val in people_node_list:
            valid_people_nodes.add(val.t)

    n = len(valid_people_nodes)

    closeness_centralities = {}
    for person in valid_people_nodes:
        closness_value = Get_Closeness_Centrality(person, valid_people_nodes, graph_db)
        closeness_centralities[person] = closness_value


def Get_Closeness_Centrality(person_node, valid_people_nodes, graph_db):
    valid_reachable_nodes = bfs(person_node, valid_people_nodes, graph_db)
    s_p = 0
    for (node, dist) in valid_reachable_nodes.items():
        s_p += dist
    n = len(valid_people_nodes)
    r_p = len(valid_reachable_nodes)
    if s_p == 0:
        return 0.0
    closeness_centrality = ((r_p - 1) * (r_p - 1)) / ((n - 1) * s_p).__float__()
    return closeness_centrality


def bfs(person_node, valid_people_nodes, graph_db):
    queue = list()
    valid_reachable_nodes = {}
    queue.append(person_node)
    valid_reachable_nodes[person_node] = 0

    while len(queue) > 0:
        next_node = queue.pop(0)  #remove 1st element
        q1 = "START n=node:People('id:" + str(next_node["id"]) + "') MATCH (n)-[:KNOWS]->(t) RETURN t"
        people_node_list = list(neo4j.CypherQuery(graph_db, q1).execute())
        neighbors = set()
        for p in people_node_list:
            neighbors.add(p.t)
        valid_neighbors = valid_people_nodes & neighbors
        for entry in valid_neighbors:
            if not valid_reachable_nodes.has_key(entry):
                queue.append(entry)
                valid_reachable_nodes[entry] = valid_reachable_nodes[next_node] + 1
    return valid_reachable_nodes


if __name__ == '__main__':
    main()
