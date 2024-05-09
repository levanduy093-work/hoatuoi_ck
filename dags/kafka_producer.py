import argparse
import csv
import json
from confluent_kafka import Producer
import socket


def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Message produced to: {msg.topic()} [{msg.partition()}]")


# Function to send message to kafka from csv file
def send_message(bootstrap_server, topic, csv_file):
    conf = {
        "bootstrap.servers": bootstrap_server,
        "client.id": socket.gethostname(),
        "message.timeout.ms": 60000,
        "retries": 5,
        "retry.backoff.ms": 1000,
    }
    producer = Producer(conf)
    message_key = "hoa".encode("utf-8")
    try:
        with open(csv_file, "r", newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                message_value = json.dumps({
                    "name": row["name"],
                    "old_price": row["old_price"],
                    "price": row["price"],
                    "description": row["description"],
                    "include": row["include"],
                }).encode('utf-8')

                producer.produce(topic, key=message_key, value=message_value, callback=acked)
                producer.poll(0)

    except BufferError as e:
        print(f"Buffer Error: {str(e)}")
    except Exception as e:
        print(f"Failed to delivery message: {str(e)}")

    producer.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Single Kafka Producer script to send CSV data to Kafka.")
    parser.add_argument("--bootstrap-servers", help="Kafka Brokers as a comma-separated list", required=True)
    parser.add_argument("--topic", help="Topic to publish the message to", required=True)
    parser.add_argument("--csv-file", help="Path to the CSV file", required=True)

    args = parser.parse_args()
    send_message(args.bootstrap_servers, args.topic, args.csv_file)