import signal
import click 
import psycopg2
import sys
from io import BytesIO
from avro.io import DatumReader
from avro.datafile import DataFileReader
from confluent_kafka import Consumer
import requests
import time

def signal_handler(sig, frame):
    print('EXITING SAFELY!')
    exit(0)

def initiate_dict(exp_data, experiment_setting):
    experiment_id = experiment_setting["experiment"]
    if experiment_id not in exp_data:
        exp_data[experiment_id] = {}
        exp_data[experiment_id] = {"configuration": {"researcher": experiment_setting['researcher'],
                                                     "sensors": experiment_setting['sensors'],
                                                     "temperature_range": experiment_setting['temperature_range'],
                                                     "stabilization": False,
                                                     "notification": True},
                                   "experiment_data": {"num_sensors": len(experiment_setting['sensors']),
                                                    #    "timestamp": [],
                                                       "temperature": []
                                                    #    "measurement_id": []
                                                       }
                                   }
    print(f"Done INIT {experiment_id}")

def insert_experiment_data(schema_name, raw_data):
    try:
        # connect to PostgreSQL DB
        conn = psycopg2.connect(
            host="postgre-svc.default.svc.cluster.local",  # Use the container's IP Address of PostgreSQL 
            port="5432",
            user="admin",
            password="psltest",
            database="postgresdb"
        )

        # create a cursor
        cursor = conn.cursor()

        # insert data into db
        if schema_name == "experiment_configured":  
            # load data into researcher table 
            cursor.execute("INSERT INTO researcher (experiment_id, researcher_email, sensor_id, upper_threshold, lower_threshold) VALUES (%s, %s, %s, %s, %s)", (raw_data['experiment'], raw_data['researcher'], ", ".join(raw_data['sensors']), raw_data['temperature_range']['upper_threshold'], raw_data['temperature_range']['lower_threshold']))
        elif schema_name == "sensor_temperature_measured":
            # load data into experiment table
            cursor.execute("INSERT INTO experiment (experiment_id, measurement_id, timestamp_value, temperature, measurement_hash) VALUES (%s, %s, %s, %s, %s)", (raw_data[0], raw_data[1], raw_data[2], raw_data[3], raw_data[4] ))
        # commit data into db
        conn.commit()

        # close connection
        conn.close()

        # print(f"successfully load {schema_name} ")

    except Exception as e:
        # error message of failing to insert data
        print("fail to insert data:", e)

def notifiy_researcher(notification_type, researcher, measurement_id, experiment_id, measurement_hash):
    headers = {
        'accept': '*/*',
        'Content-Type': 'application/json; charset=utf-8',
    }

    data_content = {
        'notification_type': notification_type, 
        'researcher': researcher,
        'measurement_id': measurement_id, 
        'experiment_id': experiment_id, 
        'cipher_data': measurement_hash
    }
    response = requests.post('https://notifications-service.cc2023.4400app.me/api/notify?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJleHAiOjE3MDQxMjk4MTgsInN1YiI6Imdyb3VwMTAifQ.WBSSMez9eZ1hxnjvTOFrTKYLK_3UbPFC2tZ2GEo9rwK6NBThL-_aAVOvV-OcxwNJpEL-MVwvdDyNjKydVFGp3LcITlUrP2lVXqKCGfaOUQpkKHY8BkUaI5Aotzav4xR_4qJ30Z_5QSBKicPA9yd2QukyWCGUOX8pVcdd43z9Sy_9MyoeuYm5r1WdrRkqNindhj29wfxPACxWsL8nWQ9uO61ZsGlE_eUcRMSmgODWoE2QoIl2UWs-lGSP6NHNct0cSqU5SoghVW1YRltx_NvVRGQCGps1nyVft8oBPMUAsgO7HiozYu1-6soXV1lWJqlepQSqHc2Szr6CHvMGgfpL1Q', headers=headers, json=data_content)
    # print(notification_type)
    # print(experiment_id)
    # response = requests.post('http://3.248.214.252:3000/api/notify', headers=headers, json=data_content)
    print(data_content)
    print(response.content)


signal.signal(signal.SIGTERM, signal_handler)


@click.command()
@click.argument('topic')
@click.argument('group_id')
def consume(topic: str, group_id: str):
    exp_data = {} 
    counter = 0
    # start_time = time.time()
    c = Consumer({
        'bootstrap.servers': '13.49.128.80:19093',
        'group.id': group_id,
        'auto.offset.reset': 'latest' ,
        'security.protocol': 'SSL',
        'ssl.ca.location': './auth/ca.crt',
        'ssl.keystore.location': './auth/kafka.keystore.pkcs12',
        'ssl.keystore.password': 'cc2023',
        'enable.auto.commit': 'true',
        'ssl.endpoint.identification.algorithm': 'none',
    })
    c.subscribe(
        [topic],
        on_assign=lambda _, p_list: print(p_list)
    )

    while True:
        msg = c.poll(1.0) 
        # current_time = time.time()
        # elapsed_time = current_time - start_time
        # if elapsed_time >= 5:
        #     print(f"Elapsed_time: {elapsed_time}")
        #     print(f"Counter: {counter}")
        #     start_time = current_time
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        try: 
            reader = DataFileReader(BytesIO(msg.value()), DatumReader())
            schema_name = dict(msg.headers())["record_name"].decode()
            counter += 1
        except: 
            print("Failed to deserialize data")
            continue
        for data in reader:
            # print(schema_name,data)
            if schema_name == "experiment_configured":
                # if the schema name is experiment_configured, create a new key in exp_data dictionary, and insert config into db.
                initiate_dict(exp_data, data)
                insert_experiment_data(schema_name, data)
            elif schema_name == "stabilization_started":
                # if the schema name is stabilization_started, query db and send needed info to API.
                # query db and send data to API.
                # print("The temperature is stabilized!")
                continue
            elif schema_name == "sensor_temperature_measured":
                # if the schema name is sensor_temperature_measured, append data into dict, calculate average temperature, and delete related info after computation. 
                # retrieve needed information
                current_experiment_id = data['experiment']
                current_timestamp = data['timestamp']
                current_measurement_id = data['measurement_id']
                current_temperature = data['temperature']
                current_measurement_hash = data['measurement_hash']
                num_sensors = exp_data[current_experiment_id]['experiment_data']['num_sensors']
                upper_bound = exp_data[current_experiment_id]['configuration']['temperature_range']['upper_threshold']
                lower_bound = exp_data[current_experiment_id]['configuration']['temperature_range']['lower_threshold']
                # if current_timestamp not in exp_data[current_experiment_id]['experiment_data']['timestamp']:
                #     # check if the value of timestamp already exists in the dict
                #     exp_data[current_experiment_id]['experiment_data']['timestamp'].append(current_timestamp)
                # if current_measurement_id not in exp_data[data['experiment']]['experiment_data']['measurement_id']:
                #     # check if the value of measurement_id already exsts in the dict
                #     exp_data[current_experiment_id]['experiment_data']['measurement_id'].append(current_measurement_id)
                # append temperature data into the dict
                exp_data[data['experiment']]['experiment_data']['temperature'].append(data['temperature'])
                if len(exp_data[current_experiment_id]['experiment_data']['temperature']) % num_sensors == 0:
                    # if the length of temperature list is equal to num_sensors, average temperature
                    temperature = exp_data[current_experiment_id]['experiment_data']['temperature']
                    total_temperature = sum(temperature)
                    # print(f"temperature: {temperature}")
                    average_temperature = total_temperature / num_sensors
                    # print(f"measurement_id: {current_measurement_id}")
                    # print(f"average_temp: {average_temperature}")
                    if not exp_data[current_experiment_id]['configuration']['stabilization']:
                        # print("Temp is not stabilized")
                        if (average_temperature > lower_bound) & (average_temperature < upper_bound):
                            # Call API
                            current_researcher = exp_data[current_experiment_id]['configuration']['researcher']
                            try:
                                notifiy_researcher("Stabilized", current_researcher, current_measurement_id, current_experiment_id, current_measurement_hash)
                            except:
                                print("fail to send notification")
                                pass
                            print(f"Average temperature {average_temperature}! It is stabilized")
                            exp_data[current_experiment_id]['configuration']['stabilization'] = True
                    if exp_data[current_experiment_id]['configuration']['stabilization']:
                        # check boundry
                        if average_temperature < lower_bound:
                            # if temperature is lower than lower boundry, call API
                            if exp_data[current_experiment_id]['configuration']['notification'] == True:
                                # notification
                                try:
                                    current_researcher = exp_data[current_experiment_id]['configuration']['researcher']
                                    notifiy_researcher("OutOfRange", current_researcher, current_measurement_id, current_experiment_id, current_measurement_hash)
                                except:
                                    print("fail to send notifcaiton")
                                    pass
                                # print(f"average temperature {average_temperature} is lower than boundry {lower_bound}")
                                # switch off the notification
                                exp_data[current_experiment_id]['configuration']['notification'] = False
                        elif average_temperature > upper_bound:
                            # if temperature is higher than upper boundry, call API
                            if exp_data[current_experiment_id]['configuration']['notification'] == True:
                                # notification
                                try:
                                    current_researcher = exp_data[current_experiment_id]['configuration']['researcher']
                                    notifiy_researcher("OutOfRange", current_researcher, current_measurement_id, current_experiment_id, current_measurement_hash)
                                except:
                                    print("fail to send notification")
                                    pass
                                # print(f"average temperature {average_temperature} is higher than boundry {upper_bound}")
                                # switch off the notification
                                exp_data[current_experiment_id]['configuration']['notification'] = False
                        else:
                            if exp_data[current_experiment_id]['configuration']['notification'] == False:
                                # switch on the notification
                                # print("The temperature is within range")
                                exp_data[current_experiment_id]['configuration']['notification'] = True
                    # organize data related to the average temperature so that it can be inserted into db
                        insert_data = [current_experiment_id, current_measurement_id, current_timestamp, average_temperature, current_measurement_hash]
                        insert_experiment_data(schema_name, insert_data)
                    # Pop Up timestamp
                    # exp_data[current_experiment_id]['experiment_data']['timestamp'].pop(0)
                    # Pop Up measurement_id
                    # exp_data[current_experiment_id]['experiment_data']['measurement_id'].pop(0)
                    # Pop Up temperature
                    exp_data[current_experiment_id]['experiment_data']['temperature'] = []
            elif schema_name == "experiment_terminated":
                # print(exp_data) 
                print("The experiment is terminated")      
consume()
