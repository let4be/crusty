FROM redis:6.2.4
RUN apt-get update && apt-get -y install git build-essential cmake
RUN mkdir /app && cd /app && git clone https://github.com/RedisBloom/RedisBloom && cd RedisBloom && git submodule update --init --recursive && make
COPY redis.conf /usr/local/etc/redis/redis.conf
COPY --from=crusty_crusty:latest /usr/local/lib/libredis_queue.so /app
COPY --from=crusty_crusty:latest /usr/local/lib/libredis_calc.so /app
EXPOSE 6379/tcp
CMD [ "redis-server", "/usr/local/etc/redis/redis.conf", "--loadmodule /app/libredis_queue.so", "--loadmodule /app/libredis_calc.so", "--loadmodule /app/RedisBloom/redisbloom.so" ]
