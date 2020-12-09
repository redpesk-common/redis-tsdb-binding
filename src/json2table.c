#include "json2table.h"

#include <string.h>
#include <stdio.h>
#include <stdbool.h>

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
        pair = malloc(sizeof(JSON_PAIR));
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
        int len = json_object_array_length(obj);
        for (int ix = 0; ix < len; ix++)
        {
            sprintf(_curpos, "%s[%d]", curpos, ix);
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

int json2table(const char *class, json_object *obj, struct cds_list_head *list)
{
    return _json2table(obj, list, class);
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


/* converts a flattened name (something like 'foo.bar[3].dum') into a json structure 
--- > 
        { "bar": [ ?, ?, "dum": ? ] } 

    and returns pointer to the targetted location (in the above case, the value associated to the "dum" key)
    That location is always a 'single' value (not a composite object)

    foo is skipped because it is the class name.

    When the result is an array, index is set different of -1
    When the result is an object, index is left to -1, and parent and key are set

*/

static void deflatten(char *name, json_object *obj, int * index, json_object ** parent, char ** key)
{
    char * ptr;
    bool array = false;
    int ret;
    int _index = -1;

    size_t namelen = strlen(name);
    json_object * _obj = obj;
    char *base = NULL;
    bool once = false;
    const char * eos = name+namelen;

    for (ptr = name;; ptr = NULL) {

        char *token = strtok(ptr, ".");

        /* skip the first base, because it is the class name */
        if (!once) {
            once = true;
            continue;
        }

        /* last token ? */
        if (token == NULL) {
            break;
        }

        _index = -1;
        array = false;
        bool end_of_parsing = false;

        base = token;
        char *closeb = NULL;

        char *openb = strstr(token, "[");
        if (openb)
        {
            closeb = strstr(openb, "]");
            if (closeb)
            {
                ret = sscanf(openb, "[%d]", &_index);
                if (ret == 1)
                {
                    base[openb - base] = '\0';
                    array = true;
                }
            }
        }

        /* we use that logic because in case of brackets, the token is altered to avoid a copy */
        if (closeb)
            end_of_parsing = (closeb == eos - 1);
        else
            end_of_parsing = (token+strlen(token) == eos);

        json_object *jvalue;

        /* Does the current object exist ? If not, create it, if yes, use it */

        if (json_object_object_get_ex(_obj, base, &jvalue)) {
            _obj = jvalue;
        }
        else {

            json_object *newobject;

            if (array) {
                newobject = json_object_new_array();
                json_object_object_add(_obj, base, newobject);
                _obj = newobject;
            }
            else if (!end_of_parsing) {
                /* only the caller will add the key/value object */
                newobject = json_object_new_object();
                json_object_object_add(_obj, base, newobject);
                _obj = newobject;
            }
        }

        /* 
            At this step, _obj points to an array, or an object.
            If we are at the end of parsing, that means that the array element
            will contain a simple value, not an object.
        */

        if (!array)
            continue;

        /* This is an array. If we are at the end of parsing, just return the array */

        if (!end_of_parsing) {

            json_object *elem = json_object_array_get_idx(_obj, _index);
            if (elem == NULL) {
                elem = json_object_new_object();
                json_object_array_put_idx(_obj, _index, elem);
            }

            _obj = elem;
            continue;
        }
    }

    *parent = _obj;
    *key = base;
    *index = _index;
   
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
  "response":{
    "sensor2": [ 
        { "ts": 123456546546, "data" : [ "cool", "groovy", 6 ] },  
    ]
  }
}

*/

int mgetReply2Json(const redisReply *rep, const char *class, json_object **res)
{
    int ret = -1;

    json_object *resobj = json_object_new_object();
        
    /* according to the documentation, each element is an array with 3 items:
    1 - key name
    2 - matching labels, when asked, else empty array
    3 - [ (int)timestamp, (char*)value ]
    */

    if (rep->elements == 0)
        goto done;

    json_object_object_add(resobj, "class", json_object_new_string(class));
    json_object * sampledataJ = json_object_new_object();
    json_object_object_add(resobj, "data", sampledataJ);

    redisReply *elem0 = rep->element[0];
    redisReply *data0 = elem0->element[2];
    long long int timestamp = data0->element[0]->integer;

    json_object_object_add(resobj, "ts", json_object_new_int64(timestamp));

    for (int ix = 0; ix < rep->elements; ix++)
    {
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

        json_object * valueJ = redis_value_to_json(value);

        /* converts composite name (something like 'foo.bar[3].dum') into a json structure */

        int index = -1;

        json_object * parentJ;
        char * key;

        deflatten(name, sampledataJ, &index, &parentJ, &key);

        // End of parsing. Update the pointed object with the given value.

        if (index == -1) {
            json_object_object_add(parentJ, key, valueJ);
        }
        else {
            json_object_array_put_idx(parentJ, index, valueJ);
        }
     }

done:
    *res = resobj;
    ret = 0;
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


*/



int mrangeReply2Json(const redisReply * rep, const char * class, json_object ** res) {
    int ret = -1;

    json_object * resobj = json_object_new_object();
    json_object * ts_array = json_object_new_array();
    json_object * column_array = json_object_new_array();

    json_object_object_add(resobj, "class", json_object_new_string(class));  
    json_object_object_add(resobj, "ts", ts_array);
    json_object_object_add(resobj, "data", column_array);  


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

    redisReply * column0 = rep->element[0];
    redisReply * samples0 = column0->element[2];
    
    // fill the timestamps header
    for (int ix=0; ix< samples0->elements; ix++) {
        redisReply * sample = samples0->element[ix];
        uint64_t ts = sample->element[0]->integer;
        json_object_array_add(ts_array, json_object_new_int64(ts));
    }

    for (int ix=0;ix< rep->elements;ix++) {
        redisReply * column = rep->element[ix];
        redisReply * samples = column->element[2];

        char * column_name = column->element[0]->str;

        json_object * columnJ = json_object_new_array();
        json_object * valuesJ = json_object_new_array();

        json_object_array_add(column_array, columnJ);
        json_object_array_add(columnJ, json_object_new_string(column_name));
        json_object_array_add(columnJ, valuesJ);

        for (int jx = 0; jx < samples->elements; jx++) {
            redisReply * sample = samples->element[jx];
            json_object_array_add(valuesJ, redis_value_to_json(sample->element[1]));
        }
    }

done:
    *res = resobj;
    ret = 0;
    return ret;

}

