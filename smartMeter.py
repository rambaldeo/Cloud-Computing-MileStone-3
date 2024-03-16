from google.cloud import pubsub_v1
import json
import time
import random
import numpy as np
import mysql.connector

project_id = "regal-fortress-413306"
topic_id = "smart_meter"
credentials_path = "cred.json"

# Initialize MySQL connection
mysql_host = '34.118.135.16'
mysql_user = 'usr'
mysql_password = 'sofe4630u'
mysql_database = 'Readings'
mysql_connection = mysql.connector.connect(
    host=mysql_host,
    user=mysql_user,
    password=mysql_password,
    database=mysql_database
)

publisher = pubsub_v1.PublisherClient.from_service_account_file(credentials_path)
topic_path = publisher.topic_path(project_id, topic_id)

# Device normal distributions profile used to generate random data
DEVICE_PROFILES = {
    "boston": {'temp': (51.3, 17.7), 'humd': (77.4, 18.7), 'pres': (1.019, 0.091)},
    "denver": {'temp': (49.5, 19.3), 'humd': (33.0, 13.9), 'pres': (1.512, 0.341)},
    "losang": {'temp': (63.9, 11.7), 'humd': (62.8, 21.8), 'pres': (1.215, 0.201)},
}
profile_names = ["boston", "denver", "losang"]

def callback(message_future, msg):
    if message_future.exception(timeout=30):
        print("Failed to publish message.")
    else:
        print("Published message: {}".format(msg))
        # Insert record into MySQL
        cursor = mysql_connection.cursor()
        insert_query = "INSERT INTO your_table (column_name) VALUES (%s)"
        cursor.execute(insert_query, (json.dumps(msg),))
        mysql_connection.commit()
        cursor.close()

record_key = 0
while True:
    try:
        profile_name = profile_names[random.randint(0, 2)]
        profile = DEVICE_PROFILES[profile_name]
        temp = max(0, np.random.normal(profile['temp'][0], profile['temp'][1]))
        humd = max(0, min(np.random.normal(profile['humd'][0], profile['humd'][1]), 100))
        pres = max(0, np.random.normal(profile['pres'][0], profile['pres'][1]))

        msg = {
            "time": time.time(),
            "profile_name": profile_name,
            "temperature": temp,
            "humidity": humd,
            "pressure": pres,
        }

        for i in range(3):
            if random.randrange(0, 10) < 5:
                choice = random.randrange(0, 3)
                if choice == 0:
                    msg['temperature'] = None
                elif choice == 1:
                    msg['humidity'] = None
                else:
                    msg['pressure'] = None

        record_key += 1
        record_value = json.dumps(msg)

        message_future = publisher.publish(topic_path, data=record_value.encode("utf-8"))
        message_future.add_done_callback(lambda future: callback(future, msg))

        time.sleep(0.5)

    except KeyboardInterrupt:
        break

print("Messages were published to topic {}!".format(topic_path))
