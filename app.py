from flask import Flask, request, Response, jsonify, make_response
import os
from pymongo import MongoClient
from model.ApiResponse import make_api_response
# from flask_socketio import SocketIO, emit
# from kafka import KafkaConsumer, TopicPartition

app = Flask(__name__)


DB_URI = "mongodb+srv://carie_admin:carie.admin@cluster0.fteep.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
app.config['MONGO_URI'] = DB_URI
mongoClient = MongoClient(DB_URI)
db = mongoClient.movielens
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "movies"


@ app.route("/", methods=["GET"])
def default():
    return make_response("Big Data 2021", 200)


@ app.route("/movie", methods=["GET"])
def getMovieList():
    coll = db["movies_new"]
    limit = int(request.args.get("limit")) if (
        request.args.get("limit")) else 100
    offset = int(request.args.get("offset")) if (
        request.args.get("offset")) else 0
    resultCursor = coll.find(
        {}, {'_id': False}).skip(offset).limit(limit)

    listResult = []
    for doc in resultCursor:
        listResult.append(doc)
    totalDocument = len(listResult)
    result = make_api_response(
        "OK", listResult, "Lay danh sach phim thanh cong", total=totalDocument)
    return jsonify(result)


@ app.route("/movie/ratings", methods=["GET"])
def getMovieRatings():
    coll = db["ratings"]
    movieId = request.args.get("movieId")
    params = {
        "movieId": movieId
    }
    result = list(coll.find({"movieId": movieId}, {"_id": False}))
    total = len(result)
    result = make_api_response(
        "OK", result, "Lay danh sach danh gia thanh cong", total=total)
    return jsonify(result)


# @socketio.on('connect', namespace='/kafka')
# def testConnect():
#     emit('logs', {'data': 'Connection established'})


# @socketio.on('kafkaconsumer', namespace='/kafka')
# def kafkaConsumer(msg):
#     consumer = KafkaConsumer(group_id='consumer-1',
#                              bootstrap_servers=BOOTSTRAP_SERVERS)

#     tp = TopicPartition(TOPIC_NAME, 0)

#     # subs to topic
#     consumer.assign([tp])

#     # obtain value
#     consumer.seek_to_end(tp)
#     lastOffset = consumer.position(tp)
#     consumer.seek_to_beginning(tp)
#     emit('kafkaconsumer1', {'data': ''})
#     for msg in consumer:
#         emit('kafkaconsumer', {'data': msg.value.decode('utf-8')})
#         if msg.offset == lastOffset - 1:
#             break
#     consumer.close()


if __name__ == '__main__':
    app.run(debug=True, port=8080)
