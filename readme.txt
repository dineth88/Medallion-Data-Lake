Import sakila db
https://dev.mysql.com/doc/sakila/en/

mysql> SOURCE C:/temp/sakila-db/sakila-schema.sql;
mysql> SOURCE C:/temp/sakila-db/sakila-data.sql;

enable bin log

create my.ini inside mysql server 8.0/bin/

my.ini

[mysqld]
server-id=1
log_bin = mysql-bin
binlog_format = ROW
binlog_row_image = FULL

After any of these, restart the MySQL80 service (services.msc) and check:

SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';

-----------------------------------

install docker locally

https://www.docker.com/products/docker-desktop

docker compose up -d

# 3. Setup storage
python setup_minio.py

python orchestrator.py

find errors

docker logs connect 

query data lake

# 1. Show all tables across all layers
python query_datalake.py tables

# 2. Run sample queries (demonstrates Bronze, Silver, Gold queries)
python query_datalake.py samples

# 3. Describe a specific table
python query_datalake.py describe bronze_customer
python query_datalake.py describe silver_payment
python query_datalake.py describe gold_customer_summary

# 4. Interactive SQL mode
python query_datalake.py interactive

# 5. Run custom SQL query
python query_datalake.py "SELECT * FROM bronze_film LIMIT 10"
python query_datalake.py "SELECT COUNT(*) FROM silver_customer WHERE active = 1"
python query_datalake.py "SELECT * FROM gold_film_performance ORDER BY total_revenue DESC LIMIT 5"

# postgresql gold layer

# Postgres user
docker exec -it postgres-analytics psql -U analytics -d analytics

# 1. Start PostgreSQL
docker-compose up -d

# 2. Install required package
pip install psycopg2-binary

# 3. Load Gold data to PostgreSQL One time
python gold_to_postgres.py

# 4. Query fast!
python fast_query.py interactive

# Execute query
SELECT * FROM customer_summary LIMIT 5;
SELECT COUNT(*) FROM film_performance;
SELECT customer_value_tier, COUNT(*) FROM customer_summary GROUP BY customer_value_tier;

# If any erro occured rm
docker volume ls
docker volume rm sakila-data-lake_postgres_data