from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import mysql.connector

project_id = "regal-fortress-413306"
subscription_id = "smart_meter_clean-sub"
timeout = 100.0
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

subscriber = pubsub_v1.SubscriberClient.from_service_account_file(credentials_path)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        record_value = message.data.decode("utf-8")
        print(f"Consumed record with value: {record_value}")

        # Process and save record to MySQL
        cursor = mysql_connection.cursor()
        insert_query = "INSERT INTO your_table (column_name) VALUES (%s)"
        cursor.execute(insert_query, (record_value,))
        mysql_connection.commit()
        cursor.close()

    except Exception as e:
        print(f"Error processing message: {e}")
    finally:
        message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    try:
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
