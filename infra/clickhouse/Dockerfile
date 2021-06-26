FROM yandex/clickhouse-server:21.3.13.9
EXPOSE 8123/tcp
COPY ./init.sql /docker-entrypoint-initdb.d/
