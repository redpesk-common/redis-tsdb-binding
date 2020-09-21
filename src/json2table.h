#pragma once

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
        const char * s;
    } d;
} JSON_PAIR;

extern int json2table(const char * class, json_object * obj, struct cds_list_head * list);
extern int mgetReply2Json(const redisReply * rep, json_object ** obj);

