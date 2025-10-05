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