from flask import Flask, Response, request, jsonify
from neo4j import GraphDatabase
import json
# from bson import json_util

app=Flask(__name__)


database_URL = "neo4j+s://2b559b07.databases.neo4j.io:7687"
username = "neo4j"
password = "69s1_x0RpDS5MM_1GMJuzK6GSk-IGXr7QcFwOLfJvdA"  


driver = GraphDatabase.driver(database_URL, auth=(username, password))
result = driver.session().run("RETURN 1 AS result")
for record in result:
    result_value = record["result"]
print(result_value)
     
    
@app.route('/imdb', methods=['POST'])
def insertdata():
    responseBody = request.get_json()
    ids= responseBody.get('ids')
    title = responseBody.get('title')
    description = responseBody.get('description')
    year = responseBody.get('year')
    runtime = responseBody.get('runtime')
    rating = responseBody.get('rating')
    votes = responseBody.get('votes')
    revenue = responseBody.get('revenue')
    director = responseBody.get('director')
    actors = list(responseBody.get('actors'))
    genres = list(responseBody.get('genres'))
    try:
      insert_query = (
          "MERGE (m:Movie {ids:$ids,title: $title, description: $description, year: $year, runtime: $runtime, "
          "rating: $rating, votes: $votes, revenue: $revenue})"
          "MERGE (d:Person {name: $director})"
          "MERGE (d)-[:DIRECTED]->(m)"
      )

      insert_query += "WITH m "
      insert_query += "UNWIND $actors AS actor "
      insert_query += "MERGE (a:Person {name: actor}) "
      insert_query += "MERGE (a)-[:ACTED_IN]->(m) "

      insert_query += "WITH m "
      insert_query += "UNWIND $genres AS genre "
      insert_query += "MERGE (g:Genres {type: genre}) "
      insert_query += "MERGE (m)-[:IN]->(g) "

      session = driver.session()
      session.run(insert_query, {
         "ids": ids,
          "title": title,
          "description": description,
          "year": year,
          "runtime": runtime,
          "rating": rating,
          "votes": votes,
          "revenue": revenue,
          "director": director,
          "actors":actors,
          "genres":genres
      })
      response = Response("New Record added",status=201,mimetype='application/json')
      return response
    except Exception as e:
      response = Response("Insert New Record Error!!"+str(e),status=500,mimetype='application/json')
      return response 

@app.route("/")
def print():
  return "Hello"

@app.route('/imdb', methods=['GET'])
def retrieveall():
  try:
    session = driver.session()
    get_query = "MATCH (m:Movie) RETURN m.title AS title, m.description AS description, m.rating AS rating, m.year AS year,m.votes as votes, m.revenue as revenue"
    result = list(session.run(get_query))
    return [{"title": record["title"], "description": record["description"], "rating": record["rating"],"year":record["year"],"votes":record["year"],"revenue":record["revenue"]} for record in result]
  except Exception as ex:
    response = Response("Error While fetching all movies!!",status=500,mimetype='application/json')
    return response


@app.route('/imdb/<string:fname>', methods=['GET'])
def retriveOne(fname):
  try:
    session = driver.session()
    query =("MATCH (m:Movie{title: $title})"
            "OPTIONAL MATCH (m)<-[:ACTED_IN]-(a:Person)"
            "OPTIONAL MATCH (m)<-[:Directed]-(d:Person)"
            "OPTIONAL MATCH (m)-[:IN]->(g:Genres)"
            "RETURN m.ids as ids, m.title AS title, m.description AS description, m.rating AS rating,collect(DISTINCT a.name) AS actors, collect(DISTINCT d.name) AS directors, collect(DISTINCT g.type) AS genres, m.year as year, m.runtime as runtime,m.votes as votes, m.revenue as revenue"
            )
    result = list(session.run(query, {"title": fname}))
    if len(result)>0 and result[0]:
      record = result[0]
      return {"ids":record["ids"],"title": record["title"], "description": record["description"], "rating": record["rating"],"year":record["year"],"votes":record["year"],"revenue":record["revenue"],"director":record["directors"],"genres":record["genres"],"actors":record["actors"],"runtime":record["runtime"]}
    response = Response("No Movie Found",status=500,mimetype='application/json')
    return response

  except Exception as ex :
    response = Response("Error While fetching movie with title!!"+fname+" "+str(ex),status=500,mimetype='application/json')
    return response
  
@app.route('/imdb/<string:fname>', methods=['DELETE'])  
def deleteByTitle(fname):
  try:
    delete_query=("MATCH (m:Movie {title: $title})"
              "OPTIONAL MATCH (m)<-[r1:ACTED_IN]-(a:Person)"
              "OPTIONAL MATCH (m)<-[r2:DIRECTED]-(d:Person)"
              "OPTIONAL MATCH (m)-[r3:IN]->(g:Genres)"
              "FOREACH(rel IN [r1, r2, r3] | DELETE rel)"
              "DETACH DELETE m")

    session= driver.session()
    result = session.run(delete_query, {"title": fname})
    if result.consume().counters.nodes_deleted >0:
        return jsonify({"message": "Movie deleted successfully"}), 200
    else:
        return jsonify({"message": "No movie found to delete"}),404

  except Exception as ex:
    response = Response("Deleting Record Error!!"+str(ex),status=500,mimetype='application/json')
    return response

@app.route('/imdb/<string:fname>', methods=['PATCH'])
def UpdateByTitle(fname):
    title = request.json.get('title')
    description = request.json.get('description')
    rating = request.json.get('rating')
    year = request.json.get('year')
    runtime = request.json.get('runtime')
    votes = request.json.get('votes')
    revenue = request.json.get('revenue')

    updated_data =[]

    if title is not None:
        updated_data.append(f"m.title = '{title}'")

    if description is not None:
        updated_data.append(f"m.description = '{description}'")

    if rating is not None:
        updated_data.append(f"m.rating = {rating}")

    if year is not None:
        updated_data.append(f"m.year = {year}")

    if runtime is not None:
        updated_data.append(f"m.runtime = {runtime}")

    if votes is not None:
        updated_data.append(f"m.votes = {votes}")

    if revenue is not None:
        updated_data.append(f"m.revenue = {revenue}")
      
    set_fields = ", ".join(updated_data)
        
    session = driver.session()
    query = (
         f"MATCH (m:Movie {{title: $title}})"
        f"SET {set_fields}"
    )
    #year runtime votes revenue
    try:
       result = (session.run(query,{"title": fname}))
       if result.consume().counters.properties_set: 
        return jsonify({"message": "Movie updated successfully"})
       else:
        return jsonify({"message": "Movie not found"})
    except Exception as ex:
        response = Response("Updating Record Error!!"+str(ex),status=500,mimetype='application/json')
    return response

    

if __name__ == '__main__':
    app.run(host="0.0.0.0",port=5000)

