from airflow.decorators import dag, task
from datetime import datetime
import requests
import csv
import urllib.parse
import logging
import os
import pandas as pd
from bs4 import BeautifulSoup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

sql_file_path = os.path.join(os.environ.get('AIRFLOW_HOME', '/usr/local/airflow'), 'include/sql/stadium_queries.sql')


@dag(
    tags=["wikipedia_flow"],
    schedule=None,
    catchup=False,
    default_args={
        "owner": "Ahmed Ayodele",
        "start_date": datetime(2024, 10, 12)
    },
    template_searchpath=['/usr/local/airflow/include/sql'],
)
def wiki_flow():

    @task
    def get_wikipedia_page(url):
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.content.decode('utf-8')

    @task
    def extract_data_from_table(html_content):
        # Parse the HTML content
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table', {'class': 'wikitable sortable sticky-header'})

        if not table:
            raise ValueError("Table with the specified class not found")

        # Initialize lists to store data
        rank = []
        stadium = []
        seating_capacity = []
        region = []
        country = []
        city = []
        images = []
        home_team = []

        # Extract rows from the table
        rows = table.find('tbody').find_all('tr')

        # Iterate over the table rows, skipping the header row
        for index, row in enumerate(rows[1:], start=2):
            try:
                logging.info(f"Processing row {index}")
                
                # Extract and append data to lists
                rank.append(row.find_all('th')[0].get_text(strip=True))
                td = row.find_all('td')
                stadium.append(td[0].get_text(strip=True))
                seating_capacity.append(td[1].get_text(strip=True).split('[')[0])
                region.append(td[2].get_text(strip=True))
                country.append(td[3].get_text(strip=True))
                city.append(td[4].get_text(strip=True))
                images.append(td[5].find('a').get('href'))
                home_team.append(td[6].get_text(strip=True))
                
                logging.info(f"Extracted data - Rank: {rank[-1]}, Stadium: {stadium[-1]}, Seating Capacity: {seating_capacity[-1]}, Region: {region[-1]}, Country: {country[-1]}, City: {city[-1]}, Image: {images[-1]}, Home Team: {home_team[-1]}")
            
            except (IndexError, AttributeError) as e:
                logging.error(f"Error processing row {index}: {e}")
                # Handle any errors that occur and append 'n/a'
                rank.append('n/a')
                stadium.append('n/a')
                seating_capacity.append('n/a')
                region.append('n/a')
                country.append('n/a')
                city.append('n/a')
                images.append('n/a')
                home_team.append('n/a')

        # URL joining for images
        web_url = "https://en.wikipedia.org/"
        image_link = [urllib.parse.urljoin(web_url, img) for img in images]

        # Pad lists to ensure they are all the same length
        def pad_list(lst, length, pad_value='n/a'):
            return lst + [pad_value] * (length - len(lst))

        max_length = max(len(rank), len(stadium), len(seating_capacity), len(region), len(country), len(city), len(images), len(home_team))

        rank = pad_list(rank, max_length)
        stadium = pad_list(stadium, max_length)
        seating_capacity = pad_list(seating_capacity, max_length)
        region = pad_list(region, max_length)
        country = pad_list(country, max_length)
        city = pad_list(city, max_length)
        image_link = pad_list(image_link, max_length)
        home_team = pad_list(home_team, max_length)

        # Create a DataFrame from the extracted data
        output = {
            "Rank": rank,
            "Stadium": stadium,
            "Seating Capacity": seating_capacity,
            "Region": region,
            "Country": country,
            "City": city,
            "Images": image_link,
            "Home Team": home_team
        }
        
        data_df = pd.DataFrame(output)
        logging.info(f"DataFrame created with shape: {data_df.shape} and type: {type(data_df)}")
        return data_df
    
    @task
    def clean_data(data_df):
        data_df['Stadium'] = data_df['Stadium'].apply(lambda x: x.replace('♦', ''))
        data_df = data_df[~data_df.apply(lambda x: x.astype(str).str.contains('n/a').any(), axis=1)]
        data_df['Images'] = data_df['Images'].apply(lambda x: 'https://en.m.wikipedia.org/wiki/File:No_image_available.svg' if x == 'n/a' else x)
        logging.info(f"Cleaned DataFrame shape: {data_df.shape}")
        return data_df

    @task
    def ensure_directory_exists():
        dir_path = os.path.join(os.environ.get('AIRFLOW_HOME', 'C:\\Users\\Ayodele\\Desktop\\airflow'), 'include/data')
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        return dir_path

    @task
    def extract_and_save_data(data_df, dir_path: str):
        if not isinstance(data_df, pd.DataFrame):
            raise ValueError("Data is not in DataFrame format")

        logging.info(f"Saving data to directory: {dir_path}")
        csv_path = os.path.join(dir_path, 'output.csv')
        data_df.to_csv(csv_path, index=False)
        return f"Data saved successfully to {csv_path}"
    
    # Task to create the stadiums table if it doesn't exist
    @task
    def create_stadiums_table():
    # This task creates the stadiums table if it doesn't exist
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        sql = """
        CREATE TABLE IF NOT EXISTS stadiums (
            rank VARCHAR(10),
            stadium VARCHAR(255),
            seating_capacity VARCHAR(20),
            region VARCHAR(100),
            country VARCHAR(100),
            city VARCHAR(100),
            image_link TEXT,
            home_team VARCHAR(255)
        );
        """
        postgres_hook.run(sql)
    
    @task
    def insert_data_into_postgres(data_df):
        # Convert DataFrame to list of tuples for Postgres insertion
        records = data_df.to_records(index=False)
        values = [tuple(row) for row in records]

        # Specify the table name
        table_name = "stadiums"
        
        # List of column names matching the DataFrame
        columns = ["rank", "stadium", "seating_capacity", "region", "country", "city", "image_link", "home_team"]

        # Use PostgresHook for bulk insertion
        hook = PostgresHook(postgres_conn_id="postgres_default")
        
        # Insert rows into the table
        hook.insert_rows(table=table_name, rows=values, target_fields=columns, commit_every=1000)


    @task
    def run_stadium_queries():
        # Establish a connection to Postgres
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        
        # Read the SQL file content
        sql_file_path = '/usr/local/airflow/include/sql/stadium_queries.sql'
        with open(sql_file_path, 'r') as file:
            sql_queries = file.read()

        # Execute the SQL commands
        #postgres_hook.run(sql_queries)
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_queries)
                results = cursor.fetchall()  

        logging.info(f"Query results: {results}")
        return results

    @task
    def save_results_to_csv(results):
        csv_path = "/usr/local/airflow/include/data/query_results.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Rank", "Stadium", "Seating Capacity", "Region", "Country", "City", "Images", "Home Team"])  # Column headers
            writer.writerows(results)
        logging.info(f"Results saved to {csv_path}")
        return csv_path 

    
    # Define task execution order
    url = "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"
    html_content = get_wikipedia_page(url)
    data_df = extract_data_from_table(html_content)
    data_df = clean_data(data_df)
    dir_path = ensure_directory_exists()
    extract_and_save_data(data_df, dir_path)

    # Set the dependencies using the wrapper function for PostgresOperator
    create_table_op = create_stadiums_table()
    query_results = run_stadium_queries()
    csv_path = save_results_to_csv(query_results)

    # Setting task dependencies
    create_table_op >> insert_data_into_postgres(data_df) >> query_results

# Instantiate the DAG
wiki_flow()
