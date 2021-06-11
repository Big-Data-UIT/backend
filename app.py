from flask import Flask, request, Response, jsonify, make_response
import os
from pymongo import MongoClient
app = Flask(__name__)

DB_URI = "mongodb+srv://carie_admin:carie.admin@cluster0.fteep.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
app.config['MONGO_URI'] = DB_URI
mongoClient = MongoClient(DB_URI)
db = mongoClient.movielens


@app.route("/", methods=["GET"])
def default():
    return make_response("Big Data 2021", 200)


@app.route("/movie", methods=["GET"])
def getMovieList():
    coll = db["movies"]
    resultCursor = coll.find({}, {'_id': False})
    listResult = []
    for doc in resultCursor:
        listResult.append(doc)
    return jsonify(listResult)


if __name__ == '__main__':
    app.run(debug=True, port=8080)
