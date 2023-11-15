import requests

def notifiy_researcher(notification_type, researcher, measurement_id, experiment_id, measurement_hash):
    headers = {
        'accept': '*/*',
        'Content-Type': 'application/json; charset=utf-8',
    }

    data1 = {
        'notification_type': notification_type, 
        'researcher': researcher,
        'measurement_id': measurement_id, 
        'experiment_id': experiment_id, 
        'cipher_data': measurement_hash
    }  

    try :
        response = requests.post('http://localhost:3000/api/notify', headers=headers, json=data1)
        print(response.content.decode())
    except :
        print("failed")
    

notifiy_researcher("OutOfRange", "d.landau@uu.nl", "1234", "5678", "D5qnEHeIrTYmLwYX.hSZNb3xxQ9MtGhRP7E52yv2seWo4tUxYe28ATJVHUi0J++SFyfq5LQc0sTmiS4ILiM0/YsPHgp5fQKuRuuHLSyLA1WR9YIRS6nYrokZ68u4OLC4j26JW/QpiGmAydGKPIvV2ImD8t1NOUrejbnp/cmbMDUKO1hbXGPfD7oTvvk6JQVBAxSPVB96jDv7C4sGTmuEDZPoIpojcTBFP2xA")