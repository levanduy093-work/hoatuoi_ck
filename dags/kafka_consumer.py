import argparse
import json
import io
import pandas as pd
from confluent_kafka import Consumer, KafkaException
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from tempfile import NamedTemporaryFile


def kafka_bigquery(bootstrap_servers, topic, consumer_group):
    def create_table(client, dataset_id, table_id, schema):
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        try:
            table = client.get_table(table_ref)
            print(f"Table {table_id} already exists.")
            return table_ref
        except NotFound:
            print(f"Table {table_id} is not found. Creating table...")
            table = bigquery.Table(table_ref, schema=schema)
            table = client.create_table(table)
            print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
            return table_ref

    def get_message(bootstrap_server, topic, consumer_group):
        conf = {
            "bootstrap.servers": bootstrap_server,
            "group.id": consumer_group,
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(conf)
        consume_loop(consumer, topic)

    # def send_to_bigquery(client, table_ref, data):
    #     errors = client.insert_rows_json(table_ref, [data])
    #     if not errors:
    #         print("New rows have been added.")
    #     else:
    #         print(f"Encountered errors: {errors}")

    def send_to_bigquery(client, table_ref, data):
        # Convert the data to a pandas DataFrame
        df = pd.DataFrame([data])

        # Convert the DataFrame to a JSON string
        json_str = df.to_json(orient='records')

        # Remove the square brackets from the start and end of the JSON string
        json_str = json_str[1:-1]

        # Create a load job config
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
        )

        # Load the data from the JSON string into BigQuery
        job = client.load_table_from_file(
            io.StringIO(json_str), table_ref, job_config=job_config
        )

        # Wait for the job to complete
        job.result()

        print("New rows have been added.")

    # ck-levanduy.
    def consume_loop(consumer, topic):
        bigquery_client = bigquery.Client()
        dataset_id = "hoa_tuoi_dataset"
        table_id = "hoa_tuoi_table"
        schema = define_schema()
        table_ref = create_table(bigquery_client, dataset_id, table_id, schema)

        consumer.subscribe([topic])
        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    print(f"Erorr: {msg.error()}")
                else:
                    message_data = json.loads(msg.value().decode("utf-8"))
                    send_to_bigquery(bigquery_client, table_ref, message_data)

        except KafkaException as e:
            print(f"Kafka error: {e}")
        finally:
            consumer.close()

    def define_schema():
        return [
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("old_price", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("price", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("description", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("include", "STRING", mode="REQUIRED"),
        ]

    get_message(bootstrap_servers, topic, consumer_group)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Consumer with BigQuery Integration.")
    parser.add_argument("--bootstrap-servers", help="Kafka Brokers as a comma-separated list", required=True)
    parser.add_argument("--topic", help="Topic to subscribe to", required=True)
    parser.add_argument("--consumer-group", help="Consumer group ID", required=True)

    args = parser.parse_args()
    kafka_bigquery(args.bootstrap_servers, args.topic, args.consumer_group)
