from google.cloud import bigquery

def head_table():
    try:
        client = bigquery.Client()

        table_id = 'ck-levanduy.hoa_tuoi_dataset.hoa_tuoi_table'

        query = f"""
            SELECT *
            FROM `{table_id}`
            LIMIT 5
        """

        query_job = client.query(query)

        results = query_job.result()

        for row in results:
            print(row)

    except Exception as e:
        print(f"Error: {e}")

def count_rows():
    try:
        client = bigquery.Client()

        table_id = 'ck-levanduy.hoa_tuoi_dataset.hoa_tuoi_table'

        query = f"""
            SELECT COUNT(*) as total_rows
            FROM `{table_id}`
        """

        query_job = client.query(query)

        results = query_job.result()

        for row in results:
            print(f"Total rows in {table_id}: {row.total_rows}")

    except Exception as e:
        print(f"Error: {e}")

def price_query():
    try:
        client = bigquery.Client()

        table_id = 'ck-levanduy.hoa_tuoi_dataset.hoa_tuoi_table'

        # Select price columns where price > 300000
        query = f"""
            SELECT price
            FROM `{table_id}`
            WHERE price > 300000
        """

        query_job = client.query(query)

        results = query_job.result()

        for row in results:
            print(f"Price: {row.price}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    head_table()
    count_rows()
    price_query()