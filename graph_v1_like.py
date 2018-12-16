'''
Usage: python graph.py W4111-48d757d14890.json
'''

import click
from google.cloud import bigquery

uni1 = 'sj2909' # Your uni
uni2 = 'None' # Partner's uni. If you don't have a partner, put None

# Test function
def testquery(client):
    q = """select * from `w4111-columbia.graph.tweets` limit 3"""
    job = client.query(q)

    # waits for query to execute and return
    results = job.result()
    return list(results)

# SQL query for Question 1. You must edit this funtion.
# This function should return a list of IDs and the corresponding text.
def q1(client):
    q = """select idx as id, text from `w4111-columbia.graph.tweets`
            where text LIKE '%going live%'
            and text LIKE '%www.twitch%'
        """
    job = client.query(q)
    results = job.result()
    return list(results)

# SQL query for Question 2. You must edit this funtion.
# This function should return a list of days and their corresponding average likes.
def q2(client):
    q = """ select SUBSTR(create_time,1,3) as day, avg(like_num) as avg_likes
            from `w4111-columbia.graph.tweets`
            group by day
            order by avg_likes desc
            limit 1
        """
    job = client.query(q)
    results = job.result()
    return list(results)

# SQL query for Question 3. You must edit this funtion.
# This function should return a list of source nodes and destination nodes in the graph.
def q3(client):
    q = """CREATE OR REPLACE TABLE dataset.GRAPH AS
            select distinct twitter_username as src, REGEXP_EXTRACT(text, r"@([a-zA-Z0-9-]+)") as dst
            from `w4111-columbia.graph.tweets`
            where REGEXP_EXTRACT(text, r"@([a-zA-Z0-9-]+)") IS NOT NULL
            --where REGEXP_EXTRACT(text, r"@([a-zA-Z0-9-]+)") in 
            --(select twitter_username from `w4111-columbia.graph.tweets`);
        """ # TODO: how to check valid Twitter user !!!!
    job = client.query(q)
    results = job.result()
    return list(results)

# SQL query for Question 4. You must edit this funtion.
# This function should return a list containing the twitter username of the users having the max indegree and max outdegree.
def q4(client):
    q = """select * from
            (select dst as max_indegree
            from dataset.GRAPH
            group by dst
            order by count(distinct src) desc
            limit 1) AS mi,
            (select src as max_outdegree
            from dataset.GRAPH
            group by src
            order by count(distinct dst) desc
            limit 1) AS mo
        """
    job = client.query(q)
    results = job.result()
    return list(results)

# SQL query for Question 5. You must edit this funtion.
# This function should return a list containing value of the conditional probability.
def q5(client):
    '''
    q = """
            1. avg(indegree)
            select avg(indegree) from
            (select count(distinct src) as indegree from dataset.GRAPH group by dst)
            
            2. avg(avg_likes)
            select avg(avg_likes) from
            (select avg(like_num) as avg_likes from `w4111-columbia.graph.tweets` group by twitter_username)

            3. high_indeg nodes (HI)
            select dst as node from dataset.GRAPH group by dst having count(distinct src) >= 
            (select avg(indegree) from
            (select count(distinct src) as indegree from dataset.GRAPH group by dst))

            4. low_indeg nodes (LI)
            select dst as node from dataset.GRAPH group by dst having count(distinct src) < 
            (select avg(indegree) from
            (select count(disticnt src) as indegree from dataset.GRAPH group by dst))

            5. high_avg_likes nodes (HL)
            select twitter_username as node from `w4111-columbia.graph.tweets`
            group by twitter_username having avg(like_num) >=
            (select avg(avg_likes) from
            (select avg(like_num) as avg_likes from `w4111-columbia.graph.tweets` group by twitter_username))

            6. low avg_likes nodes (LL)
            select twitter_username as node from `w4111-columbia.graph.tweets`
            group by twitter_username having avg(like_num) <
            (select avg(avg_likes) from
            (select avg(like_num) as avg_likes from `w4111-columbia.graph.tweets` group by twitter_username))

            7. popular nodes (HI & HL)
            select G.node from
            (select dst as node from dataset.GRAPH group by dst having count(distinct src) >=
            (select avg(indegree) from
            (select count(distinct src) as indegree from dataset.GRAPH group by dst))) as G,
            (select twitter_username as node from `w4111-columbia.graph.tweets`
            group by twitter_username having avg(like_num) >=
            (select avg(avg_likes) from
            (select avg(like_num) as avg_likes from `w4111-columbia.graph.tweets` group by twitter_username))) as T
            where G.node = T.node

            8. Unpopular nodes (LI & LL)
            select G.node from
            (select dst as node from dataset.GRAPH group by dst having count(distinct src) <
            (select avg(indegree) from
            (select count(distinct src) as indegree from dataset.GRAPH group by dst))) as G,
            (select twitter_username as node from `w4111-columbia.graph.tweets`
            group by twitter_username having avg(like_num) <
            (select avg(avg_likes) from
            (select avg(like_num) as avg_likes from `w4111-columbia.graph.tweets` group by twitter_username))) as T
            where G.node = T.node

            9. number of (unpop @ pop) tweets
            select count(*) as numer from dataset.GRAPH where
            src in
                (select G.node from
                (select dst as node from dataset.GRAPH group by dst having count(distinct src) <
                (select avg(indegree) from
                (select count(distinct src) as indegree from dataset.GRAPH group by dst))) as G,
                (select twitter_username as node from `w4111-columbia.graph.tweets`
                group by twitter_username having avg(like_num) <
                (select avg(avg_likes) from
                (select avg(like_num) as avg_likes from `w4111-columbia.graph.tweets` group by twitter_username))) as T
                where G.node = T.node)
            and dst in
                (select G.node from
                (select dst as node from dataset.GRAPH group by dst having count(distinct src) >=
                (select avg(indegree) from
                (select count(distinct src) as indegree from dataset.GRAPH group by dst))) as G,
                (select twitter_username as node from `w4111-columbia.graph.tweets`
                group by twitter_username having avg(like_num) >=
                (select avg(avg_likes) from
                (select avg(like_num) as avg_likes from `w4111-columbia.graph.tweets` group by twitter_username))) as T
                where G.node = T.node)

            10. number of unpop tweets
            select count(*) as denom from dataset.GRAPH where
            src in
                (select G.node from
                (select dst as node from dataset.GRAPH group by dst having count(distinct src) <
                (select avg(indegree) from
                (select count(distinct src) as indegree from dataset.GRAPH group by dst))) as G,
                (select twitter_username as node from `w4111-columbia.graph.tweets`
                group by twitter_username having avg(like_num) <
                (select avg(avg_likes) from
                (select avg(like_num) as avg_likes from `w4111-columbia.graph.tweets` group by twitter_username))) as T
                where G.node = T.node)

            11. cond prob (@pop | unpop)
            select numer/denom as popular_unpopular
            from
            (select count(*) as numer from dataset.GRAPH where
            src in
                (select G.node from
                (select dst as node from dataset.GRAPH group by dst having count(distinct src) <
                (select avg(indegree) from
                (select count(distinct src) as indegree from dataset.GRAPH group by dst))) as G,
                (select twitter_username as node from `w4111-columbia.graph.tweets`
                group by twitter_username having avg(like_num) <
                (select avg(avg_likes) from
                (select avg(like_num) as avg_likes from `w4111-columbia.graph.tweets` group by twitter_username))) as T
                where G.node = T.node)
            and dst in
                (select G.node from
                (select dst as node from dataset.GRAPH group by dst having count(distinct src) >=
                (select avg(indegree) from
                (select count(distinct src) as indegree from dataset.GRAPH group by dst))) as G,
                (select twitter_username as node from `w4111-columbia.graph.tweets`
                group by twitter_username having avg(like_num) >=
                (select avg(avg_likes) from
                (select avg(like_num) as avg_likes from `w4111-columbia.graph.tweets` group by twitter_username))) as T
                where G.node = T.node)),
            (select count(*) as denom from dataset.GRAPH where
            src in
                (select G.node from
                (select dst as node from dataset.GRAPH group by dst having count(distinct src) <
                (select avg(indegree) from
                (select count(distinct src) as indegree from dataset.GRAPH group by dst))) as G,
                (select twitter_username as node from `w4111-columbia.graph.tweets`
                group by twitter_username having avg(like_num) <
                (select avg(avg_likes) from
                (select avg(like_num) as avg_likes from `w4111-columbia.graph.tweets` group by twitter_username))) as T
                where G.node = T.node))
        """
    '''
    q = """select numer/denom as popular_unpopular
            from
            (select count(*) as numer from dataset.GRAPH where
            src in
                (select G.node from
                (select dst as node from dataset.GRAPH group by dst having count(distinct src) <
                (select avg(indegree) from
                (select count(distinct src) as indegree from dataset.GRAPH group by dst))) as G,
                (select twitter_username as node from `w4111-columbia.graph.tweets`
                group by twitter_username having avg(like_num) <
                (select avg(avg_likes) from
                (select avg(like_num) as avg_likes from `w4111-columbia.graph.tweets` group by twitter_username))) as T
                where G.node = T.node)
            and dst in
                (select G.node from
                (select dst as node from dataset.GRAPH group by dst having count(distinct src) >=
                (select avg(indegree) from
                (select count(distinct src) as indegree from dataset.GRAPH group by dst))) as G,
                (select twitter_username as node from `w4111-columbia.graph.tweets`
                group by twitter_username having avg(like_num) >=
                (select avg(avg_likes) from
                (select avg(like_num) as avg_likes from `w4111-columbia.graph.tweets` group by twitter_username))) as T
                where G.node = T.node)),
            (select count(*) as denom from dataset.GRAPH where
            src in
                (select G.node from
                (select dst as node from dataset.GRAPH group by dst having count(distinct src) <
                (select avg(indegree) from
                (select count(distinct src) as indegree from dataset.GRAPH group by dst))) as G,
                (select twitter_username as node from `w4111-columbia.graph.tweets`
                group by twitter_username having avg(like_num) <
                (select avg(avg_likes) from
                (select avg(like_num) as avg_likes from `w4111-columbia.graph.tweets` group by twitter_username))) as T
                where G.node = T.node))
        """
    job = client.query(q)
    results = job.result()
    return list(results) 

# SQL query for Question 6. You must edit this funtion.
# This function should return a list containing the value for the number of triangles in the graph.
def q6(client):
    q = """select count(*) from
            dataset.GRAPH A, dataset.GRAPH B, dataset.GRAPH C
            where A.dst = B.src and B.dst = C.src and C.dst = A.src
            and B.src < C.src
        """

    q = """select count(*) from
            dataset.GRAPH A join dataset.GRAPH B
            on A.dst = B.src join
            dataset.GRAPH C
            on B.dst = C.src and C.dst = A.src
        """    
    job = client.query(q)
    results = job.result()
    return list(results)   

# SQL query for Question 7. You must edit this funtion.
# This function should return a list containing the twitter username and their corresponding PageRank.
def q7(client):
    q = """select node as twitter_username, pagerank as page_rank_score
            from dataset.Pagerank
            order by pagerank desc
        """    
    job = client.query(q)
    results = job.result()
    return list(results)

# SQL query for testing
def q8(client):

    q = """
        DROP TABLE IF EXISTS dataset.Pagerank
        """
    job = client.query(q)
    results = job.result()

    q = """
        CREATE OR REPLACE TABLE dataset.Pagerank (node string, rank FLOAT64, outdeg FLOAT64)
        """
    job = client.query(q)
    results = job.result()

    q = """
        UPDATE dataset.Pagerank SET outdeg = 0 where True
        """
    job = client.query(q)
    results = job.result()

    q = """
        select src as node, count(dst) as outdeg from dataset.GRAPH
        group by src
        """
    job = client.query(q)
    results = job.result()


    q = """
        INSERT INTO dataset.Pagerank(node) select * from
        (select distinct src as node from dataset.GRAPH UNION DISTINCT
        select distinct dst as node from dataset.GRAPH)
        """
    job = client.query(q)
    results = job.result()

    q = """
        UPDATE dataset.Pagerank 
        SET rank = (select 1/count(*) from
        (select distinct src as node from dataset.GRAPH UNION DISTINCT
        select distinct dst as node from dataset.GRAPH)
        )
        where True
        """
    job = client.query(q)
    results = job.result()

    q = """
        UPDATE dataset.Pagerank 
        SET rank = 0.01
        where True
        """
    job = client.query(q)
    results = job.result()

    return list(results)

# iterative PageRank algorithm.
def pagerank(client, start, n_iter):

    qq = """
        CREATE OR REPLACE TABLE dataset.Pagerank AS
        SELECT '{start}' as node, 0 as pagerank
        """.format(start=start)
    job = client.query(q)
    # Result will be empty, but calling makes the code wait for the query to complete
    job.result()

    for i in range(n_iter):
        print("Step %d..." % (i+1))
        q1 = """
        UPDATE dataset.Pagerank
        SET 
        SELECT distinct dst, {next_distance}
        FROM dataset.bfs_graph
            WHERE src IN (
                SELECT node
                FROM dataset.pagerank
                WHERE distance = {curr_distance}
                )
            AND dst NOT IN (
                SELECT node
                FROM dataset.pagerank
                )
            """.format(
                curr_distance=i,
                next_distance=i+1
            )
        job = client.query(q1)
        results = job.result()
        # print(results)



# Do not edit this function. This is for helping you develop your own iterative PageRank algorithm.
def bfs(client, start, n_iter):

    # You should replace dataset.bfs_graph with your dataset name and table name.
    q1 = """
        CREATE TABLE IF NOT EXISTS dataset.bfs_graph (src string, dst string);
        """
    q2 = """
        INSERT INTO dataset.bfs_graph(src, dst) VALUES
        ('A', 'B'),
        ('A', 'E'),
        ('B', 'C'),
        ('C', 'D'),
        ('E', 'F'),
        ('F', 'D'),
        ('A', 'F'),
        ('B', 'E'),
        ('B', 'F'),
        ('A', 'G'),
        ('B', 'G'),
        ('F', 'G'),
        ('H', 'A'),
        ('G', 'H'),
        ('H', 'C'),
        ('H', 'D'),
        ('E', 'H'),
        ('F', 'H');
        """

    job = client.query(q1)
    results = job.result()
    job = client.query(q2)
    results = job.result()

    # You should replace dataset.distances with your dataset name and table name. 
    q3 = """
        CREATE OR REPLACE TABLE dataset.distances AS
        SELECT '{start}' as node, 0 as distance
        """.format(start=start)
    job = client.query(q3)
    # Result will be empty, but calling makes the code wait for the query to complete
    job.result()

    for i in range(n_iter):
        print("Step %d..." % (i+1))
        q1 = """
        INSERT INTO dataset.distances(node, distance)
        SELECT distinct dst, {next_distance}
        FROM dataset.bfs_graph
            WHERE src IN (
                SELECT node
                FROM dataset.distances
                WHERE distance = {curr_distance}
                )
            AND dst NOT IN (
                SELECT node
                FROM dataset.distances
                )
            """.format(
                curr_distance=i,
                next_distance=i+1
            )
        job = client.query(q1)
        results = job.result()
        # print(results)


# Do not edit this function. You can use this function to see how to store tables using BigQuery.
def save_table():
    client = bigquery.Client()
    dataset_id = 'dataset'

    job_config = bigquery.QueryJobConfig()
    # Set use_legacy_sql to True to use legacy SQL syntax.
    job_config.use_legacy_sql = True
    # Set the destination table
    table_ref = client.dataset(dataset_id).table('test')
    job_config.destination = table_ref
    job_config.allow_large_results = True
    sql = """select * from [w4111-columbia.graph.tweets] limit 3"""

    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sql,
        # Location must match that of the dataset(s) referenced in the query
        # and of the destination table.
        location='US',
        job_config=job_config)  # API request - starts the query

    query_job.result()  # Waits for the query to finish
    print('Query results loaded to table {}'.format(table_ref.path))

@click.command()
@click.argument("PATHTOCRED", type=click.Path(exists=True))
def main(pathtocred):
    client = bigquery.Client.from_service_account_json(pathtocred)

    #funcs_to_test = [q1, q2, q3, q4, q5, q6, q7]
    funcs_to_test = [q8]
    #funcs_to_test = [testquery]
    for func in funcs_to_test:
        rows = func(client)
        print ("n====%s====" % func.__name__)
        print(rows)

    #bfs(client, 'A', 5)

if __name__ == "__main__":
  main()
