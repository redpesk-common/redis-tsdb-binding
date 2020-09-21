#include "json2table.h"

#include <string.h>
#include <stdio.h>
#include <stdbool.h>

#define MAX_PATH_LEN 128

static int _json2table(json_object * obj, struct cds_list_head * list, const char * curpos) {
    
    int type = json_object_get_type(obj);
    
    char _curpos[MAX_PATH_LEN];
    JSON_PAIR * pair = NULL;

    if (type == json_type_boolean ||
        type == json_type_double ||
        type == json_type_int ||
        type == json_type_string) {
        pair = malloc(sizeof(JSON_PAIR));
        CDS_INIT_LIST_HEAD(&pair->node);
    }

    switch(type) {
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
        case json_type_string: {
            pair->type = VALUE_TYPE_BLOB;
            pair->d.s = strdup(json_object_get_string(obj));
            break;
        }            
        case json_type_object: {
            json_object_object_foreach(obj, key, val) {
                sprintf(_curpos, "%s.%s", curpos, key);
                _json2table(val, list, _curpos);
            }
            break;
        }
        case json_type_array: {
            int len = json_object_array_length(obj);
            for (int ix = 0; ix < len ; ix++) {
                sprintf(_curpos, "%s[%d]", curpos, ix);
                json_object * _obj = json_object_array_get_idx(obj, ix);
                _json2table(_obj, list, _curpos);
            }
            break;
        }

    }

    if (pair) {
        pair->key = strdup(curpos);
        cds_list_add(&pair->node, list);
    }

    return 0;

}


int json2table(const char * class, json_object * obj, struct cds_list_head * list) {
    return _json2table(obj, list, class);
}


static void addSampleToJObject(json_object * obj, long long timestamp, const char * value) {
    json_object_object_add(obj, "ts", json_object_new_int64(timestamp));
    json_object_object_add(obj, "v",  json_object_new_string(value));
}

/*
Converts the output of 'ts.mget' request, that is to say a list of column names,
and associated last timestamp & value, to a json tree.
*/

int mgetReply2Json(const redisReply * rep, json_object ** res) {
    int ret = -1;

    json_object * obj = json_object_new_object();

    /* according to the documentation, each element is an array with 3 items:
    1 - key name
    2 - matching labels, when asked, else empty 
    3 - [ (int)timestamp, (char*)value ]
    */

    for (int ix = 0; ix < rep->elements; ix++) {
        redisReply * elem = rep->element[ix];

        if (elem->elements != 3)
            continue;

        redisReply * data = elem->element[2];

        if (data->elements != 2)
            continue;

        char * name = elem->element[0]->str;
        long long int timestamp = data->element[0]->integer;
        char * value = data->element[1]->str;

        /* converts composite name (something like 'foo.bar[3].dum') into a json structure */

        char * ptr = name;
        size_t namelen = strlen(name);

        json_object * _obj = obj;
        bool array = false;
        int index = -1;

        for (ptr = name; ; ptr = NULL) {
            char * token = strtok(ptr, ".");
            if (token == NULL)
                break;

            array = false; 
            index = -1;
            char * base = token;
            char * closeb = NULL;
            
            char * openb = strstr(token, "[");
            if (openb) {
                closeb = strstr(openb, "]");
                if (closeb) {
                    ret = sscanf(openb, "[%d]", &index);
                    if (ret == 1) {
                        base[openb-base] = '\0';
                        array = true;
                    }
                }
            }

            json_object * jvalue;

            /* Does the current object exist ? If not, create it */

            if (json_object_object_get_ex(_obj, base, &jvalue)) {
                _obj = jvalue;
            }
            else {
                json_object * newobject;

                if (array)
                    newobject = json_object_new_array();
                else
                    newobject = json_object_new_object();

                json_object_object_add(_obj, base, newobject);

                _obj = newobject;
            }

            /* 
            At this step, _obj points to an array, or an object.
            If we are at the end of parsing, that means that the array element
            will contain a simple value, not an object.
            */

            if (!array) 
                continue;

            json_object * elem = json_object_array_get_idx(_obj, index);
            if (elem == NULL) {
                elem = json_object_new_object();
                json_object_array_put_idx(_obj, index, elem);

            } 

            /* not the end of parsing ? -> there is something to append to our array element at next iteration, update _obj pointer to it */
            if (closeb != name+namelen-1) { 
                _obj = elem;
                continue;
            }

        }

        // End of parsing. Update the pointed object with the given value.

        if (!array)
            addSampleToJObject(_obj, timestamp, value);
        else {
            json_object * elem = json_object_new_object();
            addSampleToJObject(elem, timestamp, value);
            json_object_array_put_idx(_obj, index, elem);
        }

    }

    *res = obj;
    ret = 0;

    return ret;
}