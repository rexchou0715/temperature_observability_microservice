from flask import Flask, request
import psycopg2
import json
import requests
from waitress import serve
import os

app = Flask(__name__)


def is_true(val):
    return len(val) > 0 and val.lower() == "true" or val == "1"

def linkDB():
    return psycopg2.connect(
            host="postgre-svc.default.svc.cluster.local",  # replace by IP address of PostgreSQL's container, save in Network?
            port="5432",
            user="admin",
            password="psltest",
            database="postgresdb"
    )

def getOutOfRangeFromDB(experiment_id):
    try:
        # connect to PostgreSQL DB
        conn = linkDB()
        # create a cursor
        cursor = conn.cursor()
        # query upper_threshold & lower_threshold from researcher table
        cursor.execute("SELECT upper_threshold, lower_threshold FROM researcher WHERE experiment_id=%s", [experiment_id])
        threshold = cursor.fetchone()
        upper = threshold[0]
        lower = threshold[1]
        # query data that are out of range from experiment table
        cursor.execute('SELECT timestamp_value, temperature FROM experiment WHERE experiment_id=%s AND (temperature>%s OR temperature<%s)', [experiment_id, upper, lower])
        query_result = cursor.fetchall()
        # transfer the Query result to json format
        result = []
        for row in query_result:
            obj = {
                'timestamp': row[0],
                'temperature': row[1],
            }
            result.append(obj)
        
        conn.commit()
        # close connection
        conn.close()
        
        return json.dumps(result)

    except Exception as e:
        # Print out error message
        return json.dumps([])

def getTemperatureFromDB(experiment_id, start_time, end_time):
    try:
        # connect to PostgreSQL DB
        conn = linkDB()

        # create a cursor
        cursor = conn.cursor()
        # Query
        cursor.execute("SELECT timestamp_value, temperature FROM experiment WHERE experiment_id=%s AND timestamp_value BETWEEN %s AND %s", [experiment_id, start_time, end_time])
        query_result = cursor.fetchall()
        # transfer the Query result to json format
        result = []
        for row in query_result:
            obj = {
                'timestamp': row[0],
                'temperature': row[1],
            }
            result.append(obj)
        
        conn.commit()
        # close connection
        conn.close()
        
        return json.dumps(result)
        
    except Exception as e:
        # Print out error message
        return json.dumps([])

# notification api
# def callNotify(type):
#     # connect to PostgreSQL DB
#     conn = linkDB()

#     # get the parameter from producer and set them to variable
#     experiment_id = 'test'
#     measurement_id = '1234' # getting the 'measurement_id' from the point of stage3
#     measurement_hash = 'hash' # getting the 'measurement_hash' from the point of stage3
#     # create a cursor
#     cursor = conn.cursor()
#     # Query researcher's email
#     cursor.execute("SELECT researcher_email FROM researcher WHERE experiment_id=%s", [experiment_id])
#     query_result = cursor.fetchall()
#     # set variable from Query result
#     researcher = query_result[0]
        
#     conn.commit()
#     # close connection
#     conn.close()

    # headers = {
    #     'Accept': '*/*',
    #     'Content-Type': 'application/json; charset=utf-8',
    # }

    # data = {
    #     'notification_type': type, #"OutOfRange", "Stabilized"
    #     'researcher': researcher,
    #     'measurement_id': measurement_id, 
    #     'experiment_id': experiment_id, 
    #     'cipher_data': measurement_hash
    # }  

    # response = requests.post('http://localhost:3000/api/notify', headers=headers, data=data)


@app.before_request
def fix_transfer_encoding():
    """
    Sets the "wsgi.input_terminated" environment flag, thus enabling
    Werkzeug to pass chunked requests as streams.  The gunicorn server
    should set this, but it's not yet been implemented.
    """

    transfer_encoding = request.headers.get("Transfer-Encoding", None)
    if transfer_encoding == u"chunked":
        request.environ["wsgi.input_terminated"] = True


@app.route("/out-of-range", methods=['GET'])
def outOfRange():
    if request.method == 'GET':
        experiment_id = request.args.get('experiment-id')
        return getOutOfRangeFromDB(experiment_id)

@app.route("/", methods=["GET"])
def getTemperature():
    if request.method == 'GET':
        experiment_id = request.args.get('experiment-id')
        start_time = request.args.get('start-time') 
        end_time = request.args.get('end-time')
        return getTemperatureFromDB(experiment_id, start_time, end_time)

if __name__ == '__main__':
    # app.run(port=5000, debug=True)
    serve(app, host='0.0.0.0', port=5000)
