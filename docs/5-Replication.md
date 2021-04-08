# The Replication Challenge

The API verbs described here are aimed to perform data replication from Edge to Cloud
In general, not all the Edge data are meant to be copied to the Cloud server,
data will likely to be resampled, of in some cases, only some statistics (average ...)
are of interest.

In order to do so, the redpesk redis binding leverages the capabilities of the RedisTimeSeries plugin,
that can perform resampling (aka 'aggregation rules') in the most possible efficient way.

This is that the *ts_maggregate* verb is made for, it creates an aggregation class to an alreay
existing class in a single call.

The *ts_mrange* verb gets all the data samples belonging to the given class, an its
output format is directly compatible with the *ts_minsert* verb, that pushes the collected
data to the Cloud server.

The sections below tell a little about the design choices that have been made.

## Collecting

### mrange result from redis (output of redis-cli) ----------

```redis-cli
127.0.0.1:6379> TS.MRANGE  - + FILTER class=sensor2
1) 1) "sensor2[0]"
   2) (empty array)
   3) 1) 1) (integer) 1606743420408
         2) "cool"
      2) 1) (integer) 1606743426621
         2) "cool"
      3) 1) (integer) 1606743429893
         2) "cool"
2) 1) "sensor2[1]"
   2) (empty array)
   3) 1) 1) (integer) 1606743420408
         2) "groovy"
      2) 1) (integer) 1606743426621
         2) "groovy"
      3) 1) (integer) 1606743429893
         2) "groovy"
3) 1) "sensor2[2]"
   2) (empty array)
   3) 1) 1) (integer) 1606743420408
         2) 6
      2) 1) (integer) 1606743426621
         2) 6
      3) 1) (integer) 1606743429892
         2) 6
4) 1) "sensor2[3]"
   2) (empty array)
   3) 1) 1) (integer) 1606743420407
         2) 23.5
      2) 1) (integer) 1606743426621
         2) 23.6
      3) 1) (integer) 1606743429892
         2) 23.7

```

### Format for replication

For replication, timestamps are set at the beginnig as meta-data

```cli
0)
   1606743420408
   1606743426621
   1606743429893

1) 1) "sensor2[0]"
         2) "cool"
         2) "cool"
         2) "cool"
2) 1) "sensor2[1]"
         2) "groovy"
         2) "groovy"
         2) "groovy"
3) 1) "sensor2[2]"
         2) 6
         2) 6
         2) 6
4) 1) "sensor2[3]"
         2) 23.5
         2) 23.6
         2) 23.7

```

and translated as such (waiting for the binary protocol to be available)
This is thus the expected output of *ts.mrange*:

```json
{
  "response":{
    "class":"sensor2",
    "ts": [1606743420408, 1606743426621, 1606743429893],
    "data": [
        [ "sensor2[0]", [ "cool" , "cool, "cool" ] ],  
        [ "sensor2[1]", [ "groovy", "groovy", "groovy" ] ],  
        [ "sensor2[2]", [ 6, 6, 6 ] ],  
        [ "sensor2[3]", [ 23.3, 23.6, 23.7 ] ]
    ]
  }
}
```

The advantage of such a representation is that it can directly we used for insertion
in database (column by column). This is what the ts_minsert function does.

These data can be, with some little work, represented as such for the end user:

-------------------------

```json
{
  "response":{
    "sensor2": [
        [  1606743420408,  [ "cool", "groovy", 6 , 23.5 ] ],  
        [  1606743426621,  [ "cool", "groovy", 6 , 23.6 ] ],  
        [  1606743429893,  [ "cool", "groovy", 6 , 23.7 ] ]  
    ]
  }
}
```

## Resampling

The *ts_maggregate* verb can be used to create a subclass of resampled data.

```bash
afb-client -H ws://localhost:1234/api?token=1 redis ts_maggregate '{ "class":"sensor2", "name":"avg", "aggregation": {"type": "avg", "bucket":50} }'
```

--> This creates as many subkeys as the ones of the sensor2 class
These keys names will be suffixed with "|<name>" where <name> is the given aggregation name.

Also, they inherit all the labels of the parent key, (but -not- the class label)
The class label is named "<parent_label>|<name>"

In this way, a simple "ts_mdel" call with the "<parent_label>|<name>" is enough to delete
all the subkeys of the aggregation
