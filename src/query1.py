__author__ = 'Saksham'

from py2neo import neo4j
import time


def main(start_id_str, end_id_str, k_str):
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

    people = graph_db.get_or_create_index(neo4j.Node, "People")
    comments = graph_db.get_or_create_index(neo4j.Node, "Comments")

    start_id = int(start_id_str)
    end_id = int(end_id_str)
    k = int(k_str)
    people_nodes = 998

    start_time = time.clock()
    if k == -1:
         q = "START n=node:People('id:" + str(start_id) + "'), t=node:People('id:" + str(
         end_id) + "') MATCH p=shortestPath((n)-[:KNOWS*]->(t)) RETURN p"
         path = neo4j.CypherQuery(graph_db, q).execute()
         if len(path) > 0:
            node_in_path = path[0].p.nodes
            path_string = ""
            for n in node_in_path:
                 path_string += str(n["id"]) + "->"
            act_string = path_string[:-2]
            print act_string + "\n"
         else:
             print "None\n"
         pass

    else:
        q1 = "START n=node:People('id:" + str(start_id) + "') return n"
        res1 = neo4j.CypherQuery(graph_db, q1).execute()
        start_node = res1[0].n
        q2 = "START n=node:People('id:" + str(end_id) + "') return n"
        res2 = neo4j.CypherQuery(graph_db, q2).execute()
        end_node = res2[0].n
        path_final = bfs(start_node,end_node,graph_db,k)

        print_string = ""
        if path_final is not None:
            for node_in_path in path_final:
                print_string += str(node_in_path["id"]) + "->"
            act_string = print_string[:-2]
            print act_string + "\n"
        else:
            print "None\n"
        pass
    end_time = time.clock()
    print "Time taken %s seconds..." % str(end_time - start_time)

def bfs(start_node, end_node, graph_db, k):
    queue = list()
    paths = {}
    queue.append(start_node)
    paths[start_node] = [start_node]
    while len(queue) > 0:
        next_node = queue.pop(0)  #remove 1st element
        if next_node["id"] == end_node["id"]:
            return paths[next_node]
        #node_id = next_node["id"]
        q = "START n=node(" + str(next_node._id) + ") " \
            "MATCH (n)-[r:KNOWS]->(t) RETURN t"
        res = neo4j.CypherQuery(graph_db, q).execute()
        all_neighbors = list(res)
        for n_node in all_neighbors:
            neighbor = n_node.t
            if not paths.has_key(neighbor):
                if Is_Frequent_Communication_Edge(next_node, neighbor, graph_db, k):
                    queue.append(neighbor)
                    paths[neighbor] = list(paths[next_node])
                    paths[neighbor].append(neighbor)
                else:
                    pass
            else:
                pass
    return None   # no frequent communication path exists

def Is_Frequent_Communication_Edge(node1, node2, graph_db, k):

    q1 = "START n=node(" + str(node1._id) + "), t=node(" + str(node2._id) + ") MATCH " \
         "(n)-[:COMMENTED]->(x)-[:REPLY_OF]->(y)<-[:COMMENTED]-(t) return count(x)"
    node1_comments = neo4j.CypherQuery(graph_db, q1).execute()
    ctr1 = list(node1_comments)[0][0]

    if ctr1 <= k:
        return False

    q2 = "START n=node(" + str(node2._id) + "), t=node(" + str(node1._id) + ") MATCH " \
         "(n)-[:COMMENTED]->(x)-[:REPLY_OF]->(y)<-[:COMMENTED]-(t) return count(x)"
    node2_comments = neo4j.CypherQuery(graph_db, q2).execute()
    ctr2 = list(node2_comments)[0][0]

    if ctr1 > k and ctr2 > k:
        return True
    return False

if __name__ == '__main__':
    main(155, 355, -1)
