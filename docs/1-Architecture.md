(??)

## 1. Introduction

The redis binding is a Redpesk API to access to a redis server, that has the RedisTimeSeries plugin enabled.
It provides all the standard calls of the RedisTimeSeries API (https://oss.redislabs.com/redistimeseries/commands), 
those calls are parts of the so-called low-level API.

In addition, it provides high level API verbs, that can handle sets (aka classes) of Time Series keys.

## 2. Architecture

The Redpesk redis binding relies on the [hiredis library] (https://github.com/redis/hiredis) library, which is C frontend to
a redis server.

## 3. Json flattening, and Blobs

All the high-level verbs of the redis binding take json data as a parameter
Internally, the RedisTimeSeries can only store single keys in time-indexed columns.

Thus, a simple flattening mecanism is used, to convert json object leafes to column names.

As an example, this object:

```json
{ "bar": 
  { "msg: [ "foo", "bla", "dum" ],
  { "vals" : { "x":5.6 , "y": 6.3}
}
```

Will be stored as 5 columns, whose names are respectively:

'bar.msg[0]'
'bar.msg[1]'
'bar.msg[2]'
'bar.vals.x'
'bar.vals.y'

The support of strings (and as a rule, of binary data), is not a standard feature of RedisTimeSeriesn
that only support scalar data.

As of today, there is a pending pull request to add blob support: https://github.com/RedisTimeSeries/RedisTimeSeries/pull/520
