# redis-tsdb-binding : a service to access RedisTimeSeries databases

This AGL service uses the hiredis C library to request a running Redis (https://redis.io/)
server with the RedisTimeSeries (https://oss.redislabs.com/redistimeseries/) plugin.

The server hostname and port must be specified in project/etc/redis-binding-config.json

See tests.md for an overview of the possible commands
