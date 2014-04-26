__author__ = 'Saksham'

from py2neo import neo4j
from operator import itemgetter
import networkx as nx


def main(k_str, interest_tag_name):
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
    people = graph_db.get_or_create_index(neo4j.Node, "People")
    interest_tags = graph_db.get_or_create_index(neo4j.Node, "Interest")
    forums = graph_db.get_or_create_index(neo4j.Node, "Forum")

    batch = neo4j.ReadBatch(graph_db)

    k = int(k_str)

    # get the tag node
    q1 = "START n=node:Interest('*:*') where n.name='" + interest_tag_name + "' RETURN n"
    tag_node_list = list(neo4j.CypherQuery(graph_db, q1).execute())
    tag_node = tag_node_list[0].n

    # get all forums with this interest tag

    #q2 = "START n=node:Interest('id:" + str(tag_node["id"]) + "') MATCH (n)-[:IS_PRESENT_IN]->(t) RETURN t"
    q2 = "START n=node(" + str(tag_node._id) + ") MATCH (n)-[:IS_PRESENT_IN]->(t) RETURN t"
    forum_node_list = list(neo4j.CypherQuery(graph_db, q2).execute())
    valid_forum_nodes = []
    for node in forum_node_list:
        valid_forum_nodes.append(node.t)

    # get people who are members of these forums
    valid_people_nodes = set()
    for node in valid_forum_nodes:
        #q3 = "START n=node:Forum('id:" + str(node["id"]) + "') MATCH (t)-[:IS_MEMBER_OF]->(n) RETURN t"
        q3 = "START n=node(" + str(node._id) + ") MATCH (t)-[:IS_MEMBER_OF]->(n) RETURN t"
        batch.append_cypher(q3)
        #people_node_list = list(neo4j.CypherQuery(graph_db, q3).execute())
    xx = batch.submit()
    batch.clear()
    while len(xx) > 0:
        val = xx.pop(0)
        if val is not None:
            if not type(val) is neo4j.Node:
                for v in val:
                    valid_people_nodes.add(v.t)
            elif type(val) is neo4j.Node:
                valid_people_nodes.add(val)
    #for val in people_node_list:
     #   valid_people_nodes.add(val.t)

    n = len(valid_people_nodes)

    valid_people_nodes_list = list(valid_people_nodes)
    node_index_map = {}
    ctr = 0
    for entry in valid_people_nodes_list:
        node_index_map[entry] = ctr
        ctr += 1

    for person in valid_people_nodes:
        #q4 = "START n=node:People('id:" + str(person["id"]) + "') MATCH (n)-[r:KNOWS]->(t) RETURN t"
        q4 = "START n=node(" + str(person._id) + ") MATCH (n)-[r:KNOWS]->(t) RETURN t"
        batch.append_cypher(q4)
    xx = batch.submit()
    batch.clear()

    G = nx.Graph()

    for i in range(0, n):
        curr_node = valid_people_nodes_list[i]
        neighbor_node_list = xx[i]
        if isinstance(neighbor_node_list, neo4j.Node):
            neighbor_node = neighbor_node_list
            if neighbor_node in valid_people_nodes:
                G.add_edge(node_index_map[curr_node], node_index_map[neighbor_node])
        else:
            for j in range(0, len(neighbor_node_list)):
                neighbor_node = neighbor_node_list[j].t
                if neighbor_node in valid_people_nodes:
                    G.add_edge(node_index_map[curr_node], node_index_map[neighbor_node])

    path_lens = nx.all_pairs_dijkstra_path_length(G)

    node_centrality_map = {}
    for (k1, v) in path_lens.items():
        s_p = 0
        r_p = len(v)
        for (x, neighbor_dist) in v.items():
            s_p += neighbor_dist
        actual_node = valid_people_nodes_list[k1]
        centrality = ((r_p-1)*(r_p-1))/((n-1)*s_p).__float__()
        node_centrality_map[actual_node["id"]] = -1*centrality

    sorted_node_centrality = sorted(node_centrality_map.items(), key=itemgetter(1, 0))

    for i in range(0, k):
        print "%s-%s\n" % (str(sorted_node_centrality[i][0]), str(-1*sorted_node_centrality[i][1]))

    pass


if __name__ == '__main__':
    main('7', 'Tony Blair')