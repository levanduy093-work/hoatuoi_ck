import pandas as pd
import os


def clean_data():
    # directory = '/Users/levanduy/PycharmProjects/hoatuoi_ck/data/crawl_data/hoatuoi.csv'
    directory = '/opt/airflow/data/crawl_data/hoatuoi.csv'
    df = pd.read_csv(directory)

    # directory = '/Users/levanduy/PycharmProjects/hoatuoi_ck/data/crawl_data/hoatuoi.json'
    # directory = '/opt/airflow/data/crawl_data/hoatuoi.json'
    # df = pd.read_json(directory)

    # In ra 5 dong dau cua du lieu
    # print(df.head())

    # Loai bo ky hieu thua
    df = df.replace('\n', '', regex=True)
    # print(df)

    df['old_price'] = df['old_price'].str.replace(' đ', '')
    df['old_price'] = df['old_price'].str.replace('.', '')
    df['price'] = df['price'].str.replace(' đ', '')
    df['price'] = df['price'].str.replace('.', '')
    # Convert the 'price' and 'old_price' columns to float
    df['price'] = df['price'].astype(float)
    df['old_price'] = df['old_price'].astype(float)
    # print(df['price'])
    # print(df['old_price'])

    # Check for missing data
    missing_data = df.isnull()
    print(missing_data)

    price_avg = round(df['price'].mean(), 2)
    # print(price_avg)
    df['price'] = df['price'].fillna(price_avg)
    # print(df['price'])

    old_price_avg = round(df['old_price'].mean(), 2)
    # print(old_price_avg)
    df['old_price'] = df['old_price'].fillna(old_price_avg)
    # print(df['old_price'])

    df['description'] = df['description'].fillna('')
    # print(df['description'])

    missing_data = df.isnull()
    print(missing_data)

    # Save data
    now = pd.Timestamp.now()
    year = now.year
    month = now.month
    day = now.day

    data_dir = f'/opt/airflow/data/clean_data/{year}/{month}/{day}'
    if not os.path.exists(data_dir):
        os.makedirs(data_dir, exist_ok=True)

    # Save data to csv
    df.to_csv(f'{data_dir}/hoatuoi_cleaned.csv', index=False)

    # Save data to json
    df.to_json(f'{data_dir}/hoatuoi_cleaned.json', orient='records')


clean_data()
