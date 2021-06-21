FROM yandex/clickhouse-server:21.6.3.14
EXPOSE 8123/tcp
COPY ./init.sql /docker-entrypoint-initdb.d/
