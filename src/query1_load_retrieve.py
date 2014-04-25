__author__ = 'Saksham'

from py2neo import neo4j
import time

person_comment_map = {}
orig_reply_map = {}

def main(start_id_str, end_id_str, k_str):
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

    people = graph_db.get_or_create_index(neo4j.Node, "People")
    comments = graph_db.get_or_create_index(neo4j.Node, "Comments")

    start_id = int(start_id_str)
    end_id = int(end_id_str)
    k = int(k_str)

    start_time = time.clock()
    if k == -1:
        q = "START n=node:People_TMP('id:" + str(start_id) + "'), t=node:People_TMP('id:" + str(
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
        load_hashes()
        q1 = "START n=node:People_TMP('id:" + str(start_id) + "') return n"
        res1 = neo4j.CypherQuery(graph_db, q1).execute()
        start_node = res1[0].n
        q2 = "START n=node:People_TMP('id:" + str(end_id) + "') return n"
        res2 = neo4j.CypherQuery(graph_db, q2).execute()
        end_node = res2[0].n
        path_final = bfs(start_node, end_node, graph_db, k)

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
    return None  # no frequent communication path exists

def load_hashes():
     with open('../data/comment_hasCreator_person.csv') as res:
        lines = res.readlines()
        lines = lines[1:]
     for line in lines:
        parts = line.strip('\n').split('|')
        cid = int(parts[0])
        pid = int(parts[1])
        if not person_comment_map.has_key(pid):
            person_comment_map[pid] = [cid]
        else:
            person_comment_map[pid].append(cid)
     pass

     with open('../data/comment_replyOf_comment.csv') as res:
        lines = res.readlines()
        lines = lines[1:]
     for line in lines:
        parts = line.strip('\n').split('|')
        reply = int(parts[0])
        orig = int(parts[1])
        if not orig_reply_map.has_key(orig):
            rset = set()
            rset.add(reply)
            orig_reply_map[orig] = rset
        else:
            orig_reply_map[orig].add(reply)
     print "Loaded hashes . . ."
     pass


def Is_Frequent_Communication_Edge(node1, node2, graph_db, k):
    person1 = node1["id"]
    person2 = node2["id"]
    # fetch comment node ids for node1, from CSV
    person1_comments = set()
    person2_comments = set()

    try:
        for p in person_comment_map[person1]:
            person1_comments.add(p)
        for p in person_comment_map[person2]:
            person2_comments.add(p)
    except:
        return False
    person1_comments_list = list(person1_comments)
    person2_comments_list = list(person2_comments)

    batch = neo4j.WriteBatch(graph_db)
    if len(person1_comments) > k and len(person2_comments) > k:

        reply_of_edges = {}
        for coms in person1_comments_list:
            if orig_reply_map.has_key(coms):
                replies = orig_reply_map[coms]
                if len(replies & person2_comments) > 0:
                    elems = list(replies & person2_comments)
                    for e in elems:
                        reply_of_edges[e] = coms
        for coms in person2_comments_list:
            if orig_reply_map.has_key(coms):
                replies = orig_reply_map[coms]
                if len(replies & person1_comments) > 0:
                    elems = list(replies & person1_comments)
                    for e in elems:
                        reply_of_edges[e] = coms

        # start COMMENTED edges
        id_node_hash = {}
        person1_comments_list = list(person1_comments)
        for c in person1_comments_list:
            if reply_of_edges.has_key(c) or c in reply_of_edges.values():
                batch.get_or_create_in_index(neo4j.Node, "Comments_TMP", "id", c, {"id": c, "type": "Comment"})
        xx = batch.submit()
        batch.clear()
        while len(xx) > 0:
            c_node = xx.pop(0)
            batch.append_cypher("START n=node(" + str(node1._id) + "), t=node(" + str(c_node._id) + ") CREATE UNIQUE (n)-[:COMMENTED]->(t)")
            id_node_hash[c_node["id"]] = c_node
        batch.run()
        batch.clear()

        person2_comments_list = list(person2_comments)
        for c in person2_comments_list:
            if reply_of_edges.has_key(c) or c in reply_of_edges.values():
                batch.get_or_create_in_index(neo4j.Node, "Comments_TMP", "id", c, {"id": c, "type": "Comment"})
        xx = batch.submit()
        batch.clear()
        while len(xx) > 0:
            c_node = xx.pop(0)
            batch.append_cypher("START n=node(" + str(node2._id) + "), t=node(" + str(c_node._id) + ") CREATE UNIQUE (n)-[:COMMENTED]->(t)")
            id_node_hash[c_node["id"]] = c_node
        batch.run()
        batch.clear()
        # end COMMENTED edges

        # start REPLY_OF edges
        for (reply, orig) in reply_of_edges.items():
            n1 = id_node_hash[reply]
            n2 = id_node_hash[orig]
            batch.append_cypher("START n=node(" + str(n1._id) + "), t=node(" + str(n2._id) + ") CREATE UNIQUE (n)-[:REPLY_OF]->(t)")
        batch.run()
        batch.clear()
        # end REPLY_OF edges

        # q1 = "START n=node:People_TMP('id:" + str(person1) + "'), t=node:People_TMP('id:" + str(person2) + "') " \
        #  "MATCH (n)-[:COMMENTED]->(x)-[:REPLY_OF]->(y)<-[:COMMENTED]-(t) return count(x)"
        q1 = "START n=node(" + str(node1._id) + "), t=node(" + str(node2._id) + ") MATCH " \
             "(n)-[:COMMENTED]->(x)-[:REPLY_OF]->(y)<-[:COMMENTED]-(t) return count(x)"
        node1_comments = neo4j.CypherQuery(graph_db, q1).execute()
        ctr1 = list(node1_comments)[0][0]

        if ctr1 <= k:
            return False

        # q2 = "START n=node:People_TMP('id:" + str(person2) + "'), t=node:People_TMP('id:" + str(person1) + "') " \
        #  "MATCH (n)-[:COMMENTED]->(x)-[:REPLY_OF]->(y)<-[:COMMENTED]-(t) return count(x)"
        q2 = "START n=node(" + str(node2._id) + "), t=node(" + str(node1._id) + ") MATCH " \
             "(n)-[:COMMENTED]->(x)-[:REPLY_OF]->(y)<-[:COMMENTED]-(t) return count(x)"
        node2_comments = neo4j.CypherQuery(graph_db, q2).execute()
        ctr2 = list(node2_comments)[0][0]

        if ctr1 > k and ctr2 > k:
            return True
        return False


if __name__ == '__main__':
    main(814, 641, 0)
