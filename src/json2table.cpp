/*
 Copyright (C) 2021 "IoT.bzh"
 Author : Thierry Bultel <thierry.bultel@iot.bzh>

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/


#include "json2table.h"

#include <string.h>
#include <stdio.h>
#include <stdbool.h>
#include <sstream>
#include <memory>

#include "deflatten.h"

#define MAX_PATH_LEN 128

static int _json2table(json_object *obj, struct cds_list_head *list, const char *curpos)
{

    int type = json_object_get_type(obj);

    char _curpos[MAX_PATH_LEN];
    JSON_PAIR *pair = NULL;

    if (type == json_type_boolean ||
        type == json_type_double ||
        type == json_type_int ||
        type == json_type_string)
    {
        pair = (JSON_PAIR*) malloc(sizeof(JSON_PAIR));
        CDS_INIT_LIST_HEAD(&pair->node);
    }

    switch (type)
    {
    case json_type_null:
        break;
    case json_type_boolean:
        pair->type = VALUE_TYPE_DOUBLE;
        pair->d.value = json_object_get_boolean(obj);
        break;
    case json_type_double:
        pair->type = VALUE_TYPE_DOUBLE;
        pair->d.value = json_object_get_double(obj);
        break;
    case json_type_int:
        pair->type = VALUE_TYPE_DOUBLE;
        pair->d.value = json_object_get_int(obj);
        break;
    case json_type_string:
    {
        pair->type = VALUE_TYPE_BLOB;
        pair->d.s = strdup(json_object_get_string(obj));
        break;
    }
    case json_type_object:
    {
        json_object_object_foreach(obj, key, val)
        {
            sprintf(_curpos, "%s.%s", curpos, key);
            _json2table(val, list, _curpos);
        }
        break;
    }
    case json_type_array:
    {
        size_t len = json_object_array_length(obj);
        for (size_t ix = 0; ix < len; ix++)
        {
            sprintf(_curpos, "%s[%d]", curpos, (int)ix);
            json_object *_obj = json_object_array_get_idx(obj, ix);
            _json2table(_obj, list, _curpos);
        }
        break;
    }
    }

    if (pair)
    {
        pair->key = strdup(curpos);
        cds_list_add(&pair->node, list);
    }

    return 0;
}

int json2table(const char *classname, json_object *obj, struct cds_list_head *list)
{
    return _json2table(obj, list, classname);
}

static json_object * redis_value_to_json(const redisReply * elem) {
    json_object * obj = NULL;
    switch(elem->type) {
        case REDIS_REPLY_INTEGER:
            obj = json_object_new_int64(elem->integer);
            break;
        case REDIS_REPLY_STATUS: {
            char * endptr;
            double val = strtod(elem->str, &endptr);
            if (elem->str == endptr) // conversion failed
                obj = json_object_new_string(elem->str);    
            else
                obj = json_object_new_double_s(val, elem->str);
            break;
        }
        case REDIS_REPLY_STRING:
            obj = json_object_new_string(elem->str);
            break;
        default:
            break;
    }
    return obj;
}


static Json::Value redis_value_to_jsoncpp(const redisReply * elem) {
    Json::Value v;
    switch(elem->type) {
        case REDIS_REPLY_INTEGER:
            v = elem->integer;
            break;
        case REDIS_REPLY_STATUS: {
            char * endptr;
            double val = strtod(elem->str, &endptr);
            if (elem->str == endptr) // conversion failed
                v = elem->str;    
            else {
                v = val;
            }
            break;
        }
        case REDIS_REPLY_STRING:
            v = elem->str;
            break;
        default:
            break;
    }
    return v;
}



/*
Converts the output of 'ts.mget' request, that is to say a list of column names,
and associated last timestamp & value, to a json tree.

127.0.0.1:6379> TS.MGET FILTER class=sensor2
1) 1) "sensor2[0]"
   2) (empty array)
   3) 1) (integer) 123456546546
      2) "cool"
2) 1) "sensor2[1]"
   2) (empty array)
   3) 1) (integer) 123456546546
      2) "groovy"
3) 1) "sensor2[2]"
   2) (empty array)
   3) 1) (integer) 1606646435266
      2) 6


Expected result is ==>

{
  "response":
  {
    "sensor2":    
        { 
            "ts": 123456546546, 
           "data" : [ "cool", "groovy", 6 ] 
        }
  }
}

*/


extern "C"
int mgetReply2Json(const redisReply *rep, const char *classname, json_object **res) {
    int ret = -1;

    Json::Value root;
    std::stringstream ss;

    Json::StreamWriterBuilder builder;
    builder.settings_["precisionType"] = "decimal";
    builder.settings_["precision"] = 5;
    std::unique_ptr<Json::StreamWriter> writer(builder.newStreamWriter());

    
    Json::Value obj;
    Json::Value ts;

    redisReply *elem0;

    if (rep->elements == 0)
        goto fail;

    elem0 = rep->element[0];

    for (size_t ix = 0; ix < rep->elements; ix++) {
        redisReply *elem = rep->element[ix];

        if (elem->elements != 3)
            continue;

        char *name = elem->element[0]->str;

        redisReply *sample = elem->element[2];

        if (sample->elements != 2) {
            fprintf(stderr, "XXX bad number of elements (given %zu, expected 2)\n", sample->elements);
            continue;
        }

        redisReply *value = sample->element[1];

        Json::Value valueJ;

        valueJ = redis_value_to_jsoncpp(value);

        /* converts composite name (something like 'foo.bar[3].dum') into a json structure */
        deflatten(obj, name, valueJ);

        std::stringstream ss;
        ss << obj;

    }
    
    /* might not have any timestamp yet */
    if (elem0->elements >=2 ) {
        redisReply *data0 = elem0->element[2];
        if (data0->elements != 0) {
            long long int timestamp = data0->element[0]->integer;
            root[classname]["ts"] = timestamp;
        }
    }

    root[classname]["data"] = obj[classname];
    writer->write(root, &ss);
    *res = json_tokener_parse(ss.str().c_str());

    ret = 0;
fail:    
    return ret;
}




/*
Converts the output of 'ts.mrange' request to a json tree.

For instance, if the following data has been recorded like this:

afb-client-demo -H ws://localhost:1234/api?token=1 redis ts_jinsert '{ "class":"sensor2", "data": [ "cool" , "groovy", 6 , 23.5 ] }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis ts_jinsert '{ "class":"sensor2", "data": [ "cool" , "groovy", 6 , 23.6 ] }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis ts_jinsert '{ "class":"sensor2", "data": [ "cool" , "groovy", 6 , 23.7 ] }'

The output on redis-cli is this one:

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

And the expected result is:

{
  "response":{
    "class":"sensor2",
    "ts": [1606743420408, 1606743426621, 1606743429893],
    "data": [ 
        [ "sensor2[0]", [ "cool" , "cool, "cool" ] ],  
        [ "sensor2[1]", [ "groovy", "groovy", "groovy" ] ],  
        [ "sensor2[2]", [ 6, 6, 6 ] ],  
        [ "sensor2[3]", [ 25.3, 23.6, 23.7 ] ]
    ]
  }
}

Notice that, at the opposite of the reply format of a mget request,
the column names are not 'deflattened'
Namely, that would generate a noisy and inefficient output, on the one
hand, and on the other hand, since one of the usage of mrange is to
collect data to replicate it on another redis server, the steps
would be to jsonify and then unjsonify the data names, which 
is a non sense in term of performances.

*/



int mrangeReply2Json(const redisReply * rep, const char * classname, json_object ** res) {
    int ret = -1;

    json_object * resobj = json_object_new_object();
    json_object * ts_array = json_object_new_array();
    json_object * column_array = json_object_new_array();

    json_object_object_add(resobj, "class", json_object_new_string(classname));  
    json_object_object_add(resobj, "ts", ts_array);
    json_object_object_add(resobj, "data", column_array);  

    redisReply * column0;
    redisReply * samples0;

    /* according to the documentation, each element is an array with 3 items:
    1 - key name
    2 - matching labels, when asked, else empty array
    3 - [ 
          [(int)timestamp, (char*)value], 
          [(int)timestamp, (char*)value], 
          [(int)timestamp, (char*)value], 
          ...
        ]
    */
   
    if (rep->elements == 0)
        goto done;

    column0 = rep->element[0];
    samples0 = column0->element[2];
    
    // fill the timestamps header
    for (size_t ix=0; ix< samples0->elements; ix++) {
        redisReply * sample = samples0->element[ix];
        uint64_t ts = sample->element[0]->integer;
        json_object_array_add(ts_array, json_object_new_int64(ts));
    }

    for (size_t ix=0;ix< rep->elements;ix++) {
        redisReply * column = rep->element[ix];
        redisReply * samples = column->element[2];

        char * column_name = column->element[0]->str;

        json_object * columnJ = json_object_new_array();
        json_object * valuesJ = json_object_new_array();

        json_object_array_add(column_array, columnJ);
        json_object_array_add(columnJ, json_object_new_string(column_name));
        json_object_array_add(columnJ, valuesJ);

        for (size_t jx = 0; jx < samples->elements; jx++) {
            redisReply * sample = samples->element[jx];
            json_object_array_add(valuesJ, redis_value_to_json(sample->element[1]));
        }
    }

done:
    *res = resobj;
    ret = 0;
    return ret;

}

