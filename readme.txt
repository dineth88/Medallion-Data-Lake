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