__author__ = 'Saksham'

import py2neo
from py2neo import neo4j, node, rel, cypher


def main(start_id_str, end_id_str, k_str):
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

    people = graph_db.get_or_create_index(neo4j.Node, "People")
    comments = graph_db.get_or_create_index(neo4j.Node, "Comments")

    start_id = int(start_id_str)
    end_id = int(end_id_str)
    k = int(k_str)
    people_nodes = 998

    q1 = "START n=node:People('id:" + str(start_id) + "') return n"
    res1 = neo4j.CypherQuery(graph_db, q1).execute()
    start_node = res1[0].n
    q2 = "START n=node:People('id:" + str(end_id) + "') return n"
    res2 = neo4j.CypherQuery(graph_db, q2).execute()
    end_node = res2[0].n
    path_final = bfs(start_node,end_node,graph_db,k)

    x = "MATCH (start {id:" + str(start_id) + ",type:\"Person\"}), (end {id:" + str(
       end_id) + ",type:\"Person\"}), p=shortestPath((start)-[:KNOWS*]-(end)) RETURN p"

    print_string = ""
    if path_final != None:
        for node_in_path in path_final:
            print_string += str(node_in_path["id"]) + "->"
        act_string = print_string[:-2]
        print act_string + "\n"
    else:
        print "None\n"
    pass

def bfs(start_node, end_node, graph_db, k):
    #Is_Frequent_Communication_Edge(end_node,start_node,graph_db,k)
    queue = list()
    paths = {}
    queue.append(start_node)
    paths[start_node] = [start_node]
    while len(queue) > 0:
        next_node = queue.pop(0)  #remove 1st element
        if next_node["id"] == end_node["id"]:
            return paths[next_node]
        node_id = next_node["id"]
        q = "START n=node:People('id:" + str(node_id) + "')" \
             "MATCH (n)-[r:KNOWS*1..1]->(t) RETURN t"
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
                    #paths[neighbor] = list(paths[next_node])
            else:
                pass
    return None   # no frequent communication path exists

def Is_Frequent_Communication_Edge(node1, node2, graph_db, k):

    q1 = "START n=node:People('id:" + str(node1["id"]) + "'), t=node:People('id:" + str(node2["id"]) + "') " \
         "MATCH (n)-[:COMMENTED]->(x)-[:REPLY_OF]->(y)<-[:COMMENTED]-(t) return count(x)"
    node1_comments = neo4j.CypherQuery(graph_db, q1).execute()
    ctr1 = list(node1_comments)[0][0]

    if ctr1 <= k:
        return False

    q2 = "START n=node:People('id:" + str(node2["id"]) + "'), t=node:People('id:" + str(node1["id"]) + "') " \
         "MATCH (n)-[:COMMENTED]->(x)-[:REPLY_OF]->(y)<-[:COMMENTED]-(t) return count(x)"
    node2_comments = neo4j.CypherQuery(graph_db, q2).execute()
    ctr2 = list(node2_comments)[0][0]

    if ctr1 > k and ctr2 > k:
        return True
    return False

    # ctr1 = 0
    # for comment in node1_comments:
    #     orig_comment_node_relations = list(graph_db.match(start_node=comment[0], rel_type="REPLY_OF"))
    #     if len(orig_comment_node_relations) > 0:
    #             orig_comment_node = orig_comment_node_relations[0].end_node
    #             owner_original_comment = list(graph_db.match(end_node=orig_comment_node, rel_type="COMMENTED"))[
    #                 0].start_node
    #             if owner_original_comment == node2:
    #                 ctr1 += 1
    #     if ctr1 > k:
    #         break
    #
    #
    #
    # ctr2 = 0
    # for comment in node2_comments:
    #     orig_comment_node_relations = list(graph_db.match(start_node=comment[0], rel_type="REPLY_OF"))
    #     if len(orig_comment_node_relations) > 0:
    #             orig_comment_node = orig_comment_node_relations[0].end_node
    #             owner_original_comment = list(graph_db.match(end_node=orig_comment_node, rel_type="COMMENTED"))[
    #                 0].start_node
    #             if owner_original_comment == node1:
    #                 ctr2 += 1
    #     if ctr2 > k:
    #         break
    #
    # if ctr1 > k and ctr2 > k:
    #     return True
    # return False


# def func(graph_db, path, k):
#     all_nodes = path.p.nodes
#     intermediate_nodes = all_nodes[1:-1]
#
#     for n in intermediate_nodes:
#         ctr = 0
#         flag = True
#
#         id = n["id"]
#         type = n["type"]
#         #q1 = "MATCH (start {id:" + str(id) + ",type:\"" + str(type) + "\"})-[:COMMENTED*1]->(comment) RETURN comment"
#         q1 = "START n=node:People('id:" + str(id) + "')" \
#              "MATCH (n)-[:COMMENTED*1]-(t) RETURN t"
#         comments = neo4j.CypherQuery(graph_db, q1).execute()
#
#         for comment in comments:
#             orig_comment_node_relations = list(graph_db.match(start_node=comment[0], rel_type="REPLY_OF"))
#             if len(orig_comment_node_relations) > 0:
#                 orig_comment_node = orig_comment_node_relations[0].end_node
#                 owner_original_comment = list(graph_db.match(end_node=orig_comment_node, rel_type="COMMENTED"))[
#                     0].start_node
#                 if owner_original_comment in intermediate_nodes and owner_original_comment != n:  #all_nodes and owner_original_comment is not n:#intermediate_nodes:
#                     ctr += 1
#             if ctr > k:
#                 flag = False
#                 break
#         if flag:
#             return False  # the path would not work
#     return True  # path found


if __name__ == '__main__':
    main(814, 641, 0)
