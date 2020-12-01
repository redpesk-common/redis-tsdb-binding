# Sample tests

blabla

## Low-level verbs (direct RedisTimeSeries mapping)

* create

``` bash
afb-client-demo -H ws://localhost:1234/api?token=1 redis create '{ "key":"temperature", "retention":3000, "uncompressed":true, "labels": { "sens":"3", "asa":"44" } }'
```

* delete

``` bash
afb-client-demo -H ws://localhost:1234/api?token=1 redis del '{ "key":"temperature" }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis del '{ "key":["temperature"; "temperature2"] }'
```

* add

``` bash
afb-client-demo -H ws://localhost:1234/api?token=1 redis add '{ "key":"temperature", "timestamp":"1548149191", "value":42 , "retention":3000, "uncompressed":true, "labels": { "sens":"3", "asa":"44" } }'

afb-client-demo -H ws://localhost:1234/api?token=1 redis add '{ "key":"temperature", "timestamp":"*", "value":42 , "retention":3000, "uncompressed":true, "labels": { "sens":"3", "asa":"44" } }'
```

* range

``` bash
afb-client-demo -H ws://localhost:1234/api?token=1 redis range '{ "key":"temperature", "fromts":"1548149191", "tots":"1548149200" }'
```

* alter

``` bash
afb-client-demo -H ws://localhost:1234/api?token=1 redis alter '{ "key":"temperature", "retention":3000,  "labels": { "sens":"3", "asa":"45" } }'
```

* madd

``` bash
afb-client-demo -H ws://localhost:1234/api?token=1 redis madd '{ "key":"temperature", "timestamp":"15481491091" , "value":32 }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis madd '[{ "key":"temperature", "timestamp":"15481491091" , "value":32 }]'
afb-client-demo -H ws://localhost:1234/api?token=1 redis madd '[{ "key":"temperature", "timestamp":"*" , "value":32 }, { "key":"temperature1", "timestamp":"*" , "value":29 }]'
```

* incrby

``` bash
afb-client-demo -H ws://localhost:1234/api?token=1 redis incrby '{ "key":"temperature", "value":1, "timestamp":"*" }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis incrby '{ "key":"temperature", "value":2 , "timestamp":"*", "retention":2000 }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis incrby '{ "key":"temperature", "value":2.2, "timestamp":"*", "uncompressed":true }'
```

* decrby

``` bash
afb-client-demo -H ws://localhost:1234/api?token=1 redis decrby '{ "key":"temperature", "value":1, "timestamp":"*" }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis decrby '{ "key":"temperature", "value":2 , "timestamp":"*", "retention":2000 }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis decrby '{ "key":"temperature", "value":2.2, "timestamp":"*", "uncompressed":true }'
```

* create/deleterule

``` bash
afb-client-demo -H ws://localhost:1234/api?token=1 redis create '{ "key":"temp1" }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis create_rule '{ "sourceKey":"temperature", "destKey":"temp1", "aggregation": {"type": "avg", "bucket":500} }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis delete_rule '{ "sourceKey":"temperature", "destKey":"temp1" }
```

* mrange/mrevrange

``` bash
afb-client-demo -H ws://localhost:1234/api?token=1 redis mrange '{ "fromts":"1548149191", "tots":"1548149200" , "filter": [ "sens=3" ] }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis mrange '{ "fromts":"-", "tots":"+" , "filter": [ "sens=3" ] }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis mrange '{ "fromts":"-", "tots":"+" , "filter": [ "sens=3" ], "count":3 }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis mrange '{ "fromts":"1548149191", "tots":"1548149200", "withlabels":true, "filter": [ "sens=3" ] }'
```

* get/mget

``` bash
afb-client-demo -H ws://localhost:1234/api?token=1 redis get '{ "key":"temp1" }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis mget '{ "filter": [ "sens=3" ] }'
```

* info/query index

``` bash
afb-client-demo -H ws://localhost:1234/api?token=1 redis info '{ "key":"temperature" }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis queryindex '{ "filter": [ "sens"=3 ] }'
```

## High Level verbs (from json to redis and vice-versa )

``` bash
afb-client-demo -H ws://localhost:1234/api?token=1 redis ts_jinsert '{ "class":"sensor1", "data": { "temperature": 25.2, "table": [ 1, 2, 3 ] } }'
```

### with blobs

``` bash
afb-client-demo -H ws://localhost:1234/api?token=1 redis ts_jinsert '{ "class":"sensor2", "data": [ "cool" , "groovy", 6 , 23.5 ] }'
```

### with objects in array

``` bash
afb-client-demo -H ws://localhost:1234/api?token=1 redis ts_jinsert '{ "class":"sensor3", "data": { "table": [ {"s": 12} , {"v": 21} ] } }'
```

### Queries

``` bash
afb-client-demo -H ws://localhost:1234/api?token=1 redis ts_jget '{ "class":"sensor1" }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis ts_jget '{ "class":"sensor2" }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis ts_jget '{ "class":"sensor3" }'
```

expected output format:

``` bash
{
  "response":{
    "sensor2": [
        { "ts": 123456789, [ "cool", "groovy", 6 ] }
    ]
  }
}
```

### ... with time range

``` bash
afb-client-demo -H ws://localhost:1234/api?token=1 redis ts_mrange '{ "class":"sensor2", "fromts":"12345", "tots":"6546" }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis ts_mrange '{ "class":"sensor2", "fromts":"-", "tots":"+" }'
```

expected output format:

``` bash
{
  "response":{
    "sensor2": [
        { "ts": 123456789, [ "cool", "groovy" ] },  
        { "ts": 123413889, [ "bad", "nasty" ] },  
    ]
  }
}
```

### Deletion

``` bash
afb-client-demo -H ws://localhost:1234/api?token=1 redis ts_jdel '{ "class":"sensor1" }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis ts_jdel '{ "class":"sensor2" }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis ts_jdel '{ "class":"sensor3" }'
```
