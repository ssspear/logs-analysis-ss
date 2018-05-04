#!/usr/bin/env python3
import psycopg2

# Queries to run in news database
queries = ["""select title, count(*) from log\
  join articles on replace(path, '/article/', '') = slug\
  group by 1\
  order by count desc\
  limit 3;""",
           """select authors.name, count(*) from log\
  join articles on replace(path, '/article/', '') = slug\
  join authors on articles.author = authors.id\
  group by 1\
  order by 2 desc;""",
           """select distinct to_char(total.day, 'Mon dd, yyyy'),\
  100*(failures.failures::numeric/total.total::numeric) as failure_rate\
  from log\
  join (\
    select date_trunc('day', time) as day, count(*) as total\
    from log\
    group by date_trunc('day', time)\
  ) total on date_trunc('day', log.time) = total.day\
  join(\
    select date_trunc('day', time) as day, count(*) as failures from log\
    where status = '404 NOT FOUND'\
    group by date_trunc('day', time)\
  ) failures on date_trunc('day', log.time) = failures.day\
  where 100*(failures.failures::numeric/total.total::numeric) > 1"""
           ]


# Connect to the database and initialize cursor
def get_cursor():
    connection = psycopg2.connect("dbname=news")
    return connection.cursor()


def get_data(cursor, queries):
    cursor.execute(queries)
    data = cursor.fetchall()
    return data


# What are the most popular three articles of all time?
def get_articles(cursor):
    query = queries[0]
    data = get_data(cursor, query)

    def f(x): return str(x[0]) + " - " + str(x[1]) + " views" + "\n"
    return "".join(map(f, data))


# Who are the most popular article authors of all time?
def get_authors(cursor):
    query = queries[1]
    data = get_data(cursor, query)

    def f(x): return str(x[0]) + " - " + str(x[1]) + " views" + "\n"
    return "".join(map(f, data))


# On which days did more than 1% of requests lead to errors?
def get_errors(cursor):
    query = queries[2]
    data = get_data(cursor, query)

    def f(x): return str(x[0]) + " - " + str(x[1]) + "% errors" + "\n"
    return "".join(map(f, data))

# Call connection to database
cursor = get_cursor()

# Fetch data
articles = get_articles(cursor)
authors = get_authors(cursor)
errors = get_errors(cursor)

# Output
print('Top 3 Articles: ')
print(articles)

print('Top Authors: ')
print(authors)

print('Days with more than 1 percent error:')
print(errors)

# Close connection
cursor.close()
