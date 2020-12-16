# redis-tsdb-binding : a service to access RedisTimeSeries databases

This AGL service uses the hiredis C library (https://github.com/redis/hiredis) to send requests to a 
running Redis (https://redis.io/) server instance that has the RedisTimeSeries 
(https://oss.redislabs.com/redistimeseries/) plugin.

The server hostname and port must be specified in project/etc/redis-binding-config.json
