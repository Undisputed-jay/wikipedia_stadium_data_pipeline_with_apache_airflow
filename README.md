# wikipedia_stadium_data_pipeline_with_apache_airflow
<p>This project implements an Apache Airflow DAG to scrape and process data on the largest football stadiums worldwide from Wikipedia. The pipeline extracts data from a Wikipedia page, cleans and stores it in a Postgres database, and performs SQL queries for further analysis. The key features include:</p>

<ul>
    <li>Scrapes football stadium data using <code>BeautifulSoup</code> and <code>requests</code>.</li>
    <li>Cleans and transforms the data using <code>pandas</code>.</li>
    <li>Stores the data in a Postgres database with automatic table creation.</li>
    <li>Executes SQL queries for advanced analysis and saves the results to CSV.</li>
</ul>

<p>This workflow is fully automated using Airflow and can be extended for other data sources.</p>
