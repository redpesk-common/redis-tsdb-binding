#pragma once

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


#ifdef __cplusplus
extern "C" {
#endif

#include <wrap-json.h>
#include <urcu/list.h>

#include <hiredis.h>

typedef enum  {
    VALUE_TYPE_DOUBLE,
    VALUE_TYPE_BLOB,
} VALUE_TYPE ;

typedef struct {
    struct cds_list_head node;
    VALUE_TYPE type;
    char * key;
    union  {
        double value;
        char * s;
    } d;
} JSON_PAIR;

extern int json2table(const char * classname, json_object * obj, struct cds_list_head * list);
extern int mgetReply2Json(const redisReply * rep, const char * classname, json_object ** obj);
extern int mrangeReply2Json(const redisReply * rep, const char * classname, json_object ** obj);

#ifdef __cplusplus
}
#endif
