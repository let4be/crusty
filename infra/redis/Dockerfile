FROM redis:6.2
RUN apt-get update && apt-get -y install git build-essential cmake
RUN mkdir /app && cd /app && git clone https://github.com/RedisBloom/RedisBloom && cd RedisBloom && git submodule update --init --recursive && make
COPY redis.conf /usr/local/etc/redis/redis.conf
COPY --from=crusty_crusty:latest /usr/local/lib/libqueue.so /app
EXPOSE 6379/tcp
CMD [ "redis-server", "/usr/local/etc/redis/redis.conf", "--loadmodule /app/libqueue.so", "--loadmodule /app/RedisBloom/redisbloom.so" ]
