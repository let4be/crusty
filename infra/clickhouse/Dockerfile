FROM yandex/clickhouse-server:21.3.18.4
EXPOSE 8123/tcp
COPY ./init.sql /docker-entrypoint-initdb.d/
