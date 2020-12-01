# The Replication Challenge

## mrange result from redis (output of redis-cli) ----------

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

## Format for replication

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
    "ts": [1606743420408, 1606743426621, 1606743429893],
    "data": [ 
        [ "sensor2[0]", [ "cool" , "cool, "cool" ] ],  
        [ "sensor2[1]", [ "groovy", "groovy", "groovy" ] ],  
        [ "sensor2[2]", [ 6, 6, 6 ] ],  
        [ "sensor2[3]", [ 25.3, 23.6, 23.7 ] ]
    ]
  }
}
```

The advantage of such a representation is that it can directly we used for insertion
in database (column by column)

... which can be, with some little work, represented as such for the end user:

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
