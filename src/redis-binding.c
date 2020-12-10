
#define _GNU_SOURCE
#include <stdio.h>
#include <string.h>
#include <time.h>

#include <wrap-json.h>
#include <ctl-config.h>

#include <hiredis.h>
#include <time.h>
#include <stdbool.h>
#include <errno.h>
#include <unistd.h>

#include "json2table.h"
#include "redis-systemd.h"

#define INSUFFICIENT_MEMORY "Insufficient memory"

// #define DEBUG

// default api to print log when apihandle not available
afb_api_t AFB_default;

static redisAsyncContext * asyncRedisContext = NULL;
static redisContext * syncRedisContext = NULL;

static void initRedis(afb_api_t, json_object *);

static int onloadConfig(afb_api_t apiHandle, CtlSectionT *section, json_object *actionsJ);

// Config Section definition (note: controls section index should match handle retrieval in HalConfigExec)
static CtlSectionT ctrlSections[]= {
    {.key="resources" , .loadCB= PluginConfig},
    {.key="onload"  , .loadCB= onloadConfig},
    // {.key="controls", .loadCB= ControlConfig},

    {.key=NULL}
};

static void ctrlapi_ping (afb_req_t request) {
    static int count=0;

    count++;
    AFB_REQ_NOTICE (request, "Controller:ping count=%d", count);
    afb_req_success(request,json_object_new_int(count), NULL);

    return;
}

static int _redisPutStr(afb_req_t request, const char * str, int * argc, char ** argv, size_t * argvlen) {
    int ret = -ENOMEM;
    argv[*argc] = strdup(str);
    if (argv[*argc] == NULL)
        goto nomem;

    argvlen[*argc] = strlen(argv[*argc]);
    (*argc)++;
    ret = 0;

nomem:
    return ret;

}

static int _redisPutBlob(afb_req_t request, int * argc, char ** argv, size_t * argvlen) {
    return _redisPutStr(request, "BLOB", argc, argv, argvlen);
}

static int _redisPutDuplicatePolicyLast(afb_req_t request, int * argc, char ** argv, size_t * argvlen) {
    int ret = -1;

    if (_redisPutStr(request, "DUPLICATE_POLICY", argc, argv, argvlen) != 0)
        goto failed;
    if (_redisPutStr(request, "LAST", argc, argv, argvlen) != 0)
        goto failed;

    ret = 0;
failed:
    return ret;
}


#ifdef DEBUG
static void _redisDisplayCmd(afb_req_t request, int argc, const char ** argv) {
    char str[256];
    str[0] = '\0';
    for (int ix=0; ix < argc; ix++) {
        strncat(str, argv[ix], 256-strlen(str)-1);
        strncat(str, " ", 256-strlen(str)-1);
    }
    AFB_API_INFO (request->api, "SENDING: %s", str);

}
#endif /* DEBUG */

static int _redisPutDouble(afb_req_t request, double value, int * argc, char ** argv, size_t * argvlen) {
    int ret = -ENOMEM;

    if (asprintf(&argv[*argc], "%f", value) == -1)
        goto nomem;
    argvlen[*argc] = strlen(argv[*argc]);
    (*argc)++;
    ret = 0;
nomem:
    return ret;    
}


static int _redisPutUint32(afb_req_t request, uint32_t value, int * argc, char ** argv, size_t * argvlen) {
    int ret = -ENOMEM;

    if (asprintf(&argv[*argc], "%d", value) == -1)
        goto nomem;
    argvlen[*argc] = strlen(argv[*argc]);
    (*argc)++;
    ret = 0;
nomem:
    return ret;    
}

static int _allocate_argv_argvlen(int argc, char ***argv, size_t ** argvlen) {
    int ret = -ENOMEM;

    *argv = calloc(argc, sizeof(char*));
    if (*argv == NULL) {
        goto nomem;
    }

    *argvlen = calloc(argc, sizeof(size_t));
    if (*argvlen == NULL) {
        goto nomem;
    }
    ret = 0;
nomem:
    return ret;
}

/* a valid timestamp string can be parsed as a UNIX timestamp (to an uint64), or a wildcard to automatic,
   or '-' for the oldest, and '+' for the most recent */
static int _redisCheckTimestamp(const char * timestampS) {
    int ret = -EINVAL;
    char * end;

    (void) strtol(timestampS, &end, 10);

    if (end == timestampS && 
        strcmp(timestampS, "*") != 0 &&
        strcmp(timestampS, "-") != 0 &&
        strcmp(timestampS, "+") != 0 )
            goto fail;

    ret = 0;
fail:
    return ret;    

}

static int redisPutCmd(afb_req_t request, const char * cmd, int * argc, char ** argv, size_t * argvlen) {
    return _redisPutStr(request, cmd, argc, argv, argvlen);
}

static int redisPutKey(afb_req_t request, const char * key, int * argc, char ** argv, size_t * argvlen) {
    return _redisPutStr(request, key, argc, argv, argvlen);
}

static int redisPutValue(afb_req_t request, double value, int * argc, char ** argv, size_t * argvlen) {
    return _redisPutDouble(request, value, argc, argv, argvlen);
}

static int redisPutTimestamp(afb_req_t request, const char * timestampS, int * argc, char ** argv, size_t * argvlen) {
    int ret = -EINVAL;

    if ((ret = _redisCheckTimestamp(timestampS)) != 0) {
        AFB_API_ERROR (request->api, "%s: wrong timestamp format", __func__);
        goto fail;
    }

    ret = _redisPutStr(request, timestampS, argc, argv, argvlen);
fail:
    return ret;    
}


static int redisPutRetention(afb_req_t request, uint32_t retention, int * argc, char ** argv, size_t * argvlen) {
    int ret = -ENOMEM;

    if ((ret = _redisPutStr(request, "RETENTION", argc, argv, argvlen)) != 0)
        goto fail;
    
    if ((ret = _redisPutUint32(request, retention, argc, argv, argvlen)) != 0)
        goto fail;

    ret = 0;
fail:
    return ret;
}

static int redisPutUncompressed(afb_req_t request, int * argc, char ** argv, size_t * argvlen) {
    int ret = -ENOMEM;

    if ((ret = _redisPutStr(request, "UNCOMPRESSED", argc, argv, argvlen)) != 0)
        goto fail;

    ret = 0;
fail:
    return ret;
}


static int redisPutLabels(afb_req_t request, json_object * labelsJ, int * argc, char ** argv, size_t * argvlen) {
    int ret = -EINVAL;
    enum json_type type;

    AFB_API_INFO (request->api, "%s: put labels %s", __func__, json_object_get_string(labelsJ));

    if ((ret = _redisPutStr(request, "LABELS", argc, argv, argvlen)) != 0)
        goto fail;

    json_object_object_foreach(labelsJ, key, val) {
        type = json_object_get_type(val);
        switch (type) {
        case json_type_string: 

            if ((ret = redisPutKey(request, key, argc, argv, argvlen)) != 0)
                goto fail;

            if ((ret = _redisPutStr(request, json_object_get_string(val), argc, argv, argvlen)) !=0 )
                goto fail;

            break;

        case json_type_int: {

            if ((ret = redisPutKey(request, key, argc, argv, argvlen)) != 0)
                goto fail;

            if ((ret = _redisPutUint32(request, json_object_get_int(val), argc, argv, argvlen)) != 0)
                goto fail;

            break;
        }
        
        default:
            goto fail;
            break;
        }
    }

    ret = 0;
    goto done;

fail:
done:
    return ret;

}


static int redisPutAggregation(afb_req_t request, json_object * aggregationJ, int * argc, char ** argv, size_t * argvlen) {
    int ret = -EINVAL;
    char * aggregationType = NULL;
    uint32_t bucket = 0;

    if (!aggregationJ)
        goto fail;

    int err = wrap_json_unpack(aggregationJ, "{ss,si !}",
        "type", &aggregationType,
        "bucket", &bucket
        );
    if (err != 0)
        goto fail;

    if ((ret = _redisPutStr(request, "AGGREGATION", argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = _redisPutStr(request, aggregationType, argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = _redisPutUint32(request, bucket, argc, argv, argvlen)) != 0)
        goto fail;

    ret = 0;
    goto done;

fail:
done:
    return ret;
}


static int redisPutFilter(afb_req_t request, json_object * filterJ, bool with_keyword, int * argc, char ** argv, size_t * argvlen) {
    int ret = -EINVAL;
    int ix;
    if (!filterJ)
        goto fail;

    if (with_keyword)
        if ((ret =_redisPutStr(request, "FILTER", argc, argv, argvlen)) != 0)
            goto fail;

    for (ix = 0; ix< json_object_array_length(filterJ); ix++) {
        struct json_object * oneRuleJ = json_object_array_get_idx(filterJ, ix);
        if ((ret = _redisPutStr(request, json_object_get_string(oneRuleJ), argc, argv, argvlen)) != 0)
            goto fail;
    }

    ret = 0;

fail:
    return ret;

}

static redisReply * get_class_keys(afb_req_t request, const char * class);

typedef int (*key_func) (afb_req_t request, const char * key, json_object * param);

static int class_keys_for_each(afb_req_t request, const char* class, key_func func, json_object * param) {
    int ret = -1;
    redisReply * keys = get_class_keys(request, class);
    if (keys == NULL)
        goto fail;

    for (int ix = 0; ix < keys->elements; ix++) {
        redisReply * elem = keys->element[ix];
        char * key = elem->str;

        ret = func(request, key, param);
        if (ret != 0) {
            goto fail;
        }
    }

    ret = 0;
fail:
    return ret;
}



#define XSTR(s) str(s)
#define str(s) #s

#define REDIS_REPLY_CASE(suffix) case REDIS_REPLY_##suffix: _s=XSTR(suffix); break;

#define REDIS_REPLY_TYPE_STR(redis_type) ({ \
    char * _s = NULL; \
    switch(redis_type) {\
        REDIS_REPLY_CASE(STRING);\
        REDIS_REPLY_CASE(ARRAY);\
        REDIS_REPLY_CASE(INTEGER);\
        REDIS_REPLY_CASE(NIL);\
        REDIS_REPLY_CASE(STATUS);\
        REDIS_REPLY_CASE(ERROR);\
        default: break;\
    }\
    _s;\
})

static int redisReplyToJson(afb_req_t request, const redisReply* reply, json_object ** replyJ ) {
    int ret = -EINVAL;
    if (!reply)
        goto fail;
    if (!replyJ)
        goto fail;

    *replyJ = NULL;

    switch (reply->type)  {
    case REDIS_REPLY_ARRAY: {
        int ix;
        json_object * arrayJ = json_object_new_array();
        if (!arrayJ)
            goto nomem;

        for (ix = 0; ix < reply->elements; ix++) {
            redisReply * element = reply->element[ix];
            json_object * objectJ = NULL;
            ret = redisReplyToJson(request, element, &objectJ);
            if (ret != 0)
                goto fail;
            if (objectJ != 0)
                json_object_array_add(arrayJ, objectJ);
        }

        *replyJ = arrayJ;
        break;
    }

    case REDIS_REPLY_INTEGER: {
        json_object * intJ = json_object_new_int64(reply->integer);
        if (!intJ)
            goto nomem;
        // json_object_object_add(*replyJ, "v", intJ);
        *replyJ = intJ;
        break;
    }

    case REDIS_REPLY_STATUS:
    case REDIS_REPLY_STRING: {
        json_object * strJ = json_object_new_string(reply->str);
        if (!strJ)
            goto nomem;
        *replyJ = strJ;
        break;
    }

    case REDIS_REPLY_NIL: {
        json_object * strJ = json_object_new_string("NIL");
        if (!strJ)
            goto nomem;
        *replyJ = strJ;
        break;
    }


    default:
        AFB_API_ERROR (request->api, "%s: Unhandled result type %s, str %s", __func__, REDIS_REPLY_TYPE_STR(reply->type), reply->str);
        goto fail;
        break;
    }

    ret = 0;
    goto done;
nomem:
    ret = -ENOMEM;
fail:
    if (*replyJ)
        free(*replyJ);

done:
    return ret;
}

/* redisCommandArgv does not check the null pointer ! */

static void * __redisCommandArgv(redisContext *c, int argc, const char **argv, const size_t *argvlen) {
    redisReply * rep = NULL;
    if (c == NULL)
        goto fail;

    rep = redisCommandArgv(syncRedisContext, argc, argv, argvlen);
fail:
    return rep;
}

static int redisSendCmd(afb_req_t request, int argc, const char ** argv, const size_t * argvlen, json_object ** replyJ, char ** resstr) {
    int ret = -EINVAL;

#ifdef DEBUG
    _redisDisplayCmd(request, argc, argv);
#endif /* DEBUG */

    if (resstr)
        *resstr = NULL;

    redisReply * rep = __redisCommandArgv(syncRedisContext, argc, argv, argvlen);
    if (rep == NULL) {
        ret = asprintf(resstr, "redis-error: redis command failed");
        if (ret == -1)
            ret = -ENOMEM;
    	goto fail;
    }

    AFB_API_INFO (request->api, "%s: cmd result type %s, str %s", __func__, REDIS_REPLY_TYPE_STR(rep->type), rep->str);

    if (rep->type == REDIS_REPLY_ERROR) {
        ret = asprintf(resstr, "redis_command error %s", rep->str);
    	goto fail;
    }

    if (rep->type == REDIS_REPLY_INTEGER)
        AFB_API_INFO (request->api, "%s: ret %lld", __func__, rep->integer);

    if (!replyJ)
        goto done;

    ret = redisReplyToJson(request, rep, replyJ);
    if (ret != 0 || *replyJ == NULL) {
        ret = asprintf(resstr, "failed to convert reply to json");
        if (ret == -1)
            ret = -ENOMEM;
        goto fail;
    }

    AFB_API_INFO (request->api, "%s: json result %s", __func__, json_object_get_string(*replyJ));

done:
    ret = 0;
fail:    
    return ret;
}


static void argvCleanup(int argc, char ** argv, size_t * argvlen) {
    int ix;
    if (argv) {
        for (ix=0; ix < argc; ix++) {
            if (argv[ix])
                free(argv[ix]);
        }
        free(argv);
    }

    if (argvlen)
        free(argvlen);

}

static int _redis_create(afb_req_t request, const char * key, int32_t retention, uint32_t uncompressed, json_object * labelsJ, bool blob , char ** resstr) {
    int ret = 0;
    char ** argv = NULL;
    size_t * argvlen = NULL;

    AFB_API_INFO(request->api, "Creating key %s, retention %d, uncomp %d, labels %s, blob %d",
                    key, retention, uncompressed, json_object_get_string(labelsJ), blob);

    int argc = 2; /* one slot for command name, one for the key */
    
    if (retention)
        argc += 2;

    if (uncompressed)
        argc++;

    if (labelsJ) {
        argc++;
        argc += 2*json_object_object_length(labelsJ);
    }

    if (blob)
        argc++;

    if ((ret = _allocate_argv_argvlen(argc, &argv, &argvlen)) != 0)
        goto fail;

    argc = 0;

    if ((ret = redisPutCmd(request, "TS.CREATE", &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisPutKey(request, key, &argc, argv, argvlen)) != 0)
        goto fail;

    if (retention) 
        if ((ret = redisPutRetention(request, retention, &argc, argv, argvlen)) != 0) {
            AFB_API_ERROR (request->api, "%s: failed to put retention", __func__);
            goto fail;
        }

    if (uncompressed) {
        if ((ret = redisPutUncompressed(request, &argc, argv, argvlen)) != 0)
            goto fail;
    }

    if (labelsJ) {
        ret = redisPutLabels(request, labelsJ, &argc, argv, argvlen);
        if (ret != 0) {
            AFB_API_ERROR (request->api, "%s: failed to put labels %s", __func__, json_object_get_string(labelsJ));
            goto fail;
        }
    }

    if (blob) {
        ret = _redisPutBlob(request, &argc, argv, argvlen);
        if (ret != 0)
            goto fail;
    }

    if ((ret = redisSendCmd(request, argc, (const char **)argv, argvlen, NULL, resstr)) != 0)
        goto fail;

    ret = 0;

fail:
    argvCleanup(argc, argv, argvlen);
    return ret;
}

static void redis_create (afb_req_t request) {
    json_object *argsJ = afb_req_json(request);
    char * rkey = NULL;
    int32_t retention = 0;
    uint32_t uncompressed = false;
    json_object * labelsJ = NULL;
    uint32_t blob = false;

    char * resstr = NULL;
    int ret = -EINVAL;

    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));

    int err = wrap_json_unpack(argsJ, "{ss,s?i,s?b,s?o,si !}",
        "key", &rkey,
        "retention", &retention,
        "uncompressed", &uncompressed,
        "labels", &labelsJ,
        "blob", &blob
        );
    if (err) {
        err = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    ret = _redis_create(request, rkey, retention, uncompressed, labelsJ, blob, &resstr);
    if (ret != 0)
        goto fail;

    afb_req_success(request, NULL, NULL);
    goto done;

fail:
    if (ret == -ENOMEM)
        ret = asprintf(&resstr, INSUFFICIENT_MEMORY);
    afb_req_fail(request, "error", resstr);

done:
    free(resstr);
    return;
}


static void redis_del(afb_req_t request) {
    json_object *argsJ = afb_req_json(request);

    char ** argv = NULL;
    size_t * argvlen = NULL;
    char * resstr = NULL;

    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));

    int ret = -EINVAL;
    int argc = 0;
    int nbargs = 1;
    json_object * keyJ;

    int err = wrap_json_unpack(argsJ, "{so !}",
        "key", &keyJ );

    if (err) {
        err = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    if (json_object_get_type(keyJ) == json_type_string) {
        nbargs++;
    } else if (json_object_get_type(keyJ) == json_type_array) {
        nbargs+= json_object_array_length(keyJ);
    } else {
        err = asprintf(&resstr, "wrong json type in '%s'", json_object_get_string(keyJ));
        goto fail;
    }

    if ((ret = _allocate_argv_argvlen(nbargs, &argv, &argvlen)) != 0)
        goto fail;

    if ((ret = redisPutCmd(request, "DEL", &argc, argv, argvlen)) != 0)
        goto fail;

    if (json_object_get_type(keyJ) == json_type_string) {
        const char * key = json_object_get_string(keyJ);

        ret = redisPutKey(request, key, &argc, argv, argvlen);
        if (ret == -EINVAL) {
            ret = asprintf(&resstr, "parse error: %s", json_object_get_string(argsJ));
            goto fail;
        }

    } else if (json_object_get_type(keyJ) == json_type_array) {
        int nbelems = json_object_array_length(keyJ);

        for (int ix = 0; ix < nbelems; ix++) {
            json_object * elem = json_object_array_get_idx(keyJ, ix);
            const char * key = json_object_get_string(elem);
            
            ret = redisPutKey(request, key, &argc, argv, argvlen);
            if (ret == -EINVAL) {
                ret = asprintf(&resstr, "parse error: %s", json_object_get_string(elem));
                goto fail;
            }
              

        }
    } 

    if ((ret = redisSendCmd(request, argc, (const char **)argv, argvlen, NULL, &resstr)) != 0)
        goto fail;

    afb_req_success(request, NULL, NULL);
    goto done;

fail:
    if (ret == -ENOMEM)
        ret = asprintf(&resstr, INSUFFICIENT_MEMORY);

    afb_req_fail(request, "error", resstr);

done:

    free(resstr);
    argvCleanup(argc, argv, argvlen);

}


static void redis_alter (afb_req_t request) {
    json_object *argsJ = afb_req_json(request);
    char * rkey = NULL;
    int32_t retention = 0;
    uint32_t uncompressed = false;
    json_object * labelsJ = NULL;

    char ** argv = NULL;
    size_t * argvlen = NULL;
    char * resstr = NULL;

    int ret = -EINVAL;

    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));

    int err = wrap_json_unpack(argsJ, "{ss,s?i,s?o !}",
        "key", &rkey,
        "retention", &retention,
        "labels", &labelsJ
        );
    if (err) {
        err = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    int argc = 2; /* one slot for command name, one for the key */

    if (retention)
        argc += 2;

    if (uncompressed)
        argc++;

    if (labelsJ) {
        argc++;
        json_object_object_foreach(labelsJ, key, val) {
            (void) val;
            (void) key;
            argc += 2;
        }
    }

    if ((ret = _allocate_argv_argvlen(argc, &argv, &argvlen)) != 0)
        goto fail;

    argc = 0;

    if ((ret = redisPutCmd(request, "TS.ALTER", &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisPutKey(request, rkey, &argc, argv, argvlen)) !=0)
        goto fail;

    if (retention)
        if ((ret = redisPutRetention(request, retention, &argc, argv, argvlen)) != 0) {
            AFB_API_ERROR (request->api, "%s: failed to put retention", __func__);
            goto fail;
        }

    if (labelsJ) {
        if ((ret = redisPutLabels(request, labelsJ, &argc, argv, argvlen)) != 0) {
            AFB_API_ERROR (request->api, "%s: failed to put labels %s", __func__, json_object_get_string(labelsJ));
            goto fail;
        }
    }

    if ((ret = redisSendCmd(request, argc, (const char **)argv, argvlen, NULL, &resstr)) != 0)
        goto fail;

    afb_req_success(request, NULL, NULL);
    goto done;

fail:
    if (ret == -ENOMEM)
        ret = asprintf(&resstr, INSUFFICIENT_MEMORY);
    afb_req_fail(request, "error", resstr);

done:

    free(resstr);
    argvCleanup(argc, argv, argvlen);

    return;
}

static int internal_redis_add_cmd(afb_req_t request, const char * key, const char * timestampS, int * argc, char ** argv, size_t * argvlen) {

    int ret = -1;

    if ((ret = redisPutCmd(request, "TS.ADD", argc, argv, argvlen)) != 0)
        goto fail;
    if ((ret = redisPutKey(request, key, argc, argv, argvlen)) != 0)
        goto fail;
    if ((ret = redisPutTimestamp(request, timestampS, argc, argv, argvlen)) != 0)
        goto fail;
    ret = 0;
fail:
    return ret;

}

static int internal_redis_put_class_label(afb_req_t request, const char * class, int * argc, char ** argv, size_t * argvlen) {
    int ret = -1;
    if ((ret = _redisPutStr(request, "LABELS", argc, argv, argvlen)) != 0)
        goto fail;
    if ((ret = _redisPutStr(request, "class", argc, argv, argvlen)) != 0)
        goto fail;
    if ((ret = _redisPutStr(request, class, argc, argv, argvlen)) != 0)
        goto fail;
    ret = 0;
fail:
    return ret;
}

static int internal_redis_add_double(afb_req_t request, const char * key, double value, const char * timestampS, const char * class, char ** resstr) {

    int ret = -1;
    char ** argv = NULL;
    size_t * argvlen = NULL;

    int argc = 4; /* 1 slot for command name, 1 for the key, 1 for timestamp, and 1 for value */

    if (class)
        argc += 3;

    if ((ret = _allocate_argv_argvlen(argc, &argv, &argvlen)) != 0)
        goto fail;

    argc = 0;
    if ((ret = internal_redis_add_cmd(request, key, timestampS, &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisPutValue(request, value, &argc, argv, argvlen)) != 0)
        goto fail;

    if (class) {
        if ((ret = internal_redis_put_class_label(request, class, &argc, argv, argvlen)) != 0)
            goto fail;
    }

    if ((ret = redisSendCmd(request, argc, (const char **)argv, argvlen, NULL, resstr)) != 0)
        goto fail;
    
    ret = 0;

fail:
    argvCleanup(argc, argv, argvlen);
    return ret;
}

static int internal_redis_add_string(afb_req_t request, const char * key, const char * value, const char * timestampS, const char * class, char ** resstr ) {

    int ret = -1;
    char ** argv = NULL;
    size_t * argvlen = NULL;

    int argc = 5; /* 1 slot for command name, 1 for the key, 1 for timestamp, and 1 for value, 1 for BLOB keyword */

    if (class)
        argc += 3;

    if ((ret = _allocate_argv_argvlen(argc, &argv, &argvlen)) != 0)
        goto fail;

    argc = 0;
    if ((ret = internal_redis_add_cmd(request, key, timestampS, &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = _redisPutStr(request, value, &argc, argv, argvlen)) != 0)
        goto fail;

    if (class) {
        if ((ret = internal_redis_put_class_label(request, class, &argc, argv, argvlen)) != 0)
            goto fail;
    }

    if ((ret = _redisPutBlob(request, &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisSendCmd(request, argc, (const char **)argv, argvlen, NULL, resstr)) != 0)
        goto fail;

    ret = 0;

fail:
    argvCleanup(argc, argv, argvlen);
    return ret;
}


static void redis_add (afb_req_t request) {

    json_object *argsJ = afb_req_json(request);

    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));

    char * rkey;
    uint32_t retention = 0;
    uint32_t uncompressed  = false;
    json_object * labelsJ = NULL;
    char * timestampS = NULL;
    double value;

    char ** argv = NULL;
    size_t * argvlen = NULL;
    char * resstr = NULL;

    int ret = -EINVAL;

    int err = wrap_json_unpack(argsJ, "{s:s,s:s,s:F,s?i,s?b,s?o !}", 
        "key", &rkey,
        "timestamp", &timestampS,
        "value", &value,
        "retention", &retention,
        "uncompressed", &uncompressed,
        "labels", &labelsJ );
    if (err) {
        err = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    int argc = 4; /* 1 slot for command name, 1 for the key, 1 for timestamp, and 1 for value */
    
    if (retention)
        argc += 2;

    if (uncompressed)
        argc++;

    if (labelsJ) {
        argc++;
        argc += 2*json_object_object_length(labelsJ);
    }

    if ((ret = _allocate_argv_argvlen(argc, &argv, &argvlen)) != 0) 
        goto fail;

    argc = 0;

    if ((ret = internal_redis_add_cmd(request, rkey, timestampS, &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisPutValue(request, value, &argc, argv, argvlen)) != 0)
        goto fail;

    if (retention) 
        if ((ret = redisPutRetention(request, retention, &argc, argv, argvlen)) != 0) {
            AFB_API_ERROR (request->api, "%s: failed to put retention", __func__);
            goto fail;
        }

    if (uncompressed) {
        if ((ret = redisPutUncompressed(request, &argc, argv, argvlen)) != 0)
            goto fail;
    }

    if (labelsJ) {
        if ((ret = redisPutLabels(request, labelsJ, &argc, argv, argvlen)) != 0) {
            AFB_API_ERROR (request->api, "%s: failed to put labels %s", __func__, json_object_get_string(labelsJ));
            goto fail;
        }
    }

    if ((ret = redisSendCmd(request, argc, (const char **)argv, argvlen, NULL, &resstr)) != 0)
        goto fail;

    afb_req_success(request, NULL, NULL);
    goto done;

fail:
    if (ret == -ENOMEM)
        ret = asprintf(&resstr, INSUFFICIENT_MEMORY);
    afb_req_fail(request, "error", resstr);

done:

    free(resstr);
    argvCleanup(argc, argv, argvlen);
    return;
}

static int _redis_get_ts_value(afb_req_t request, json_object * v, int * argc, char ** argv, size_t * argvlen) {
    int ret = -EINVAL;

    char * rkey = NULL;
    char * timestampS = NULL;
    double value;

    int err = wrap_json_unpack(v, "{s:s,s:s,s:F !}",
        "key", &rkey,
        "timestamp", &timestampS,
        "value", &value);
    if (err) {
        AFB_API_ERROR (request->api, "%s: json error %s", __func__, json_object_get_string(v));
        goto fail;
    }

    if ((ret = redisPutKey(request, rkey, argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisPutTimestamp(request, timestampS, argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisPutValue(request, value, argc, argv, argvlen)) != 0)
        goto fail;

    ret = 0;

fail:
    return ret;
}



static void redis_madd (afb_req_t request) {

    json_object *argsJ = afb_req_json(request);

    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));

    char ** argv = NULL;
    size_t * argvlen = NULL;
    char * resstr = NULL;

    int ret = -EINVAL;
    int argc = 0;
    int nbargs = 0;

    if (json_object_get_type(argsJ) == json_type_object) {
        nbargs = 4;
        if ((ret=_allocate_argv_argvlen(nbargs, &argv, &argvlen)) != 0)
            goto fail;

        ret = _redis_get_ts_value(request, argsJ, &argc, argv, argvlen);
        if (ret == -EINVAL) {
            ret = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
            goto fail;
        }

    } else if (json_object_get_type(argsJ) == json_type_array) {

        int nbelems = json_object_array_length(argsJ);
        nbargs = 1 + 3*nbelems;

        if ((ret =_allocate_argv_argvlen(nbargs, &argv, &argvlen)) != 0)
            goto fail;

        for (int ix = 0; ix < nbelems; ix++) {
            json_object * elem = json_object_array_get_idx(argsJ, ix);
            ret = _redis_get_ts_value(request, elem, &argc, argv, argvlen);

            if (ret == -EINVAL) {
                ret = asprintf(&resstr, "parse error: %s", json_object_get_string(elem));
                goto fail;
            }
        }
    } else {
        ret = asprintf(&resstr , "wrong json type in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    argc = 0;
    if ((ret = redisPutCmd(request, "TS.MADD", &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisSendCmd(request, nbargs, (const char **)argv, argvlen, NULL, &resstr)) != 0)
        goto fail;

    afb_req_success(request, NULL, NULL);
    goto done;


fail:
    if (ret == -ENOMEM)
        ret = asprintf(&resstr, INSUFFICIENT_MEMORY);
    afb_req_fail(request, "error", resstr);

done:

    free(resstr);
    argvCleanup(argc, argv, argvlen);
    return;
}




static void redis_range (afb_req_t request) {
    json_object *argsJ = afb_req_json(request);

    char * rkey;
    char * fromtimestampS;
    char * totimestampS;
    uint32_t count = 0;
    json_object * aggregationJ = NULL;
    json_object * replyJ = NULL;

    char ** argv = NULL;
    int argc = 4; // 1 cmd, 1 key, 2 timestamp
    size_t * argvlen = NULL;
    char * resstr = NULL;
    int ret = -EINVAL;

    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));

    int err = wrap_json_unpack(argsJ, "{s:s,s:s,s:s,s?i,s?o !}", 
        "key", &rkey,
        "fromts", &fromtimestampS,
        "tots", &totimestampS,
        "count", &count,
        "aggregation", &aggregationJ
        );
    if (err) {
        err = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    if (count != 0)
        argc++;

    if (aggregationJ)
        argc+=3;

    if ((ret = _allocate_argv_argvlen(argc, &argv, &argvlen)) != 0)
        goto fail;

    argc = 0;

    if ((ret = redisPutCmd(request, "TS.RANGE", &argc, argv, argvlen)) != 0)
        goto fail;

    if (redisPutKey(request, rkey, &argc, argv, argvlen) != 0)
        goto fail;

    if ((ret = redisPutTimestamp(request, fromtimestampS, &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisPutTimestamp(request, totimestampS,  &argc, argv, argvlen)) != 0 )
        goto fail;

    if (count != 0) {
        if ((ret = _redisPutStr(request, "COUNT", &argc, argv, argvlen)) != 0)
            goto fail;

        if ((ret = _redisPutUint32(request, count, &argc, argv, argvlen)) != 0)
           goto fail;
    }

    if (aggregationJ) {
        if ((ret = redisPutAggregation(request, aggregationJ, &argc, argv, argvlen)) != 0) {
            AFB_API_ERROR (request->api, "%s: failed to put aggregation %s", __func__, json_object_get_string(aggregationJ));
            goto fail;
        }
    }

    if ((ret = redisSendCmd(request, argc, (const char **)argv, argvlen, &replyJ, &resstr)) != 0)
        goto fail;
    
    afb_req_success(request, replyJ, resstr);
    goto done;

fail:
    if (ret == -ENOMEM)
        ret = asprintf(&resstr, INSUFFICIENT_MEMORY);
    afb_req_fail(request, "error", resstr);
    free(replyJ);

done:

    free(resstr);
    argvCleanup(argc, argv, argvlen);
    return;
}


static void _redis_incr_or_decr_by (afb_req_t request, bool incr) {
    json_object *argsJ = afb_req_json(request);

    char * rkey;
    uint32_t retention = 0;
    uint32_t uncompressed  = false;

    char * timestampS = NULL;
    double value;

    AFB_API_DEBUG (request->api, "%s: %s (%s)", __func__, json_object_get_string(argsJ), incr?"incr":"decr");

    char ** argv = NULL;
    size_t * argvlen = NULL;
    char * resstr = NULL;

    int ret = -EINVAL;

    const char * cmd = incr?"TS.INCRBY":"TS.DECRBY";

    int err = wrap_json_unpack(argsJ, "{s:s,s:F,s?s,s?i,s?b !}", 
        "key", &rkey,
        "value", &value,
        "timestamp", &timestampS,
        "retention", &retention,
        "uncompressed", &uncompressed );

    if (err) {
        err = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    int argc = 3; /* 1 slot for command name, 1 for the key, and 1 for value */
    
    if (timestampS)
        argc += 2;
    
    if (retention)
        argc += 2;

    if (uncompressed)
        argc++;

    if ((ret = _allocate_argv_argvlen(argc, &argv, &argvlen)) != 0)
        goto fail;

    argc = 0;

    if ((ret = redisPutCmd(request, cmd, &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisPutKey(request, rkey, &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisPutValue(request, value, &argc, argv, argvlen)) != 0)
        goto fail;

    if (timestampS) {
        if ((ret = redisPutTimestamp(request, timestampS, &argc, argv, argvlen)) != 0)
            goto fail;
    }

    if (retention) 
        if ((ret = redisPutRetention(request, retention, &argc, argv, argvlen)) != 0) {
            AFB_API_ERROR (request->api, "%s: failed to put retention", __func__);
            goto fail;
        }

    if (uncompressed) {
        if ((ret = redisPutUncompressed(request, &argc, argv, argvlen)) != 0)
            goto fail;
    }

    if ((ret = redisSendCmd(request, argc, (const char **)argv, argvlen, NULL, &resstr)) != 0)
        goto fail;
    
    afb_req_success(request, NULL, NULL);
    goto done;

fail:
    if (ret == -ENOMEM)
        ret = asprintf(&resstr, INSUFFICIENT_MEMORY);
    afb_req_fail(request, "error", resstr);

done:

    free(resstr);
    argvCleanup(argc, argv, argvlen);

}


static void redis_decrby (afb_req_t request) {
    _redis_incr_or_decr_by(request, false);
}

static void redis_incrby (afb_req_t request) {
    _redis_incr_or_decr_by(request, true);
}

static int _redis_create_rule(afb_req_t request, const char * srckey, const char * destkey, json_object * aggregationJ, char ** resstr ) {

    char ** argv = NULL;
    size_t * argvlen = NULL;
    int ret = -1;

    int argc = 6; // cmd, source, dest, +3 for aggregation

    if ((ret = _allocate_argv_argvlen(argc, &argv, &argvlen)) != 0)
        goto fail;
        
    argc = 0;

    if ((ret = redisPutCmd(request, "TS.CREATERULE", &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisPutKey(request, srckey, &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisPutKey(request, destkey, &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisPutAggregation(request, aggregationJ, &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisSendCmd(request, argc, (const char **)argv, argvlen, NULL, resstr)) != 0)
        goto fail;

    ret = 0;

fail:
    argvCleanup(argc, argv, argvlen);
    return ret;
}

static void redis_create_rule (afb_req_t request) {
    json_object *argsJ = afb_req_json(request);
    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));

    char * skey = NULL;
    char * dkey = NULL;
    json_object * aggregationJ = NULL;

    char * resstr;
    int ret = -EINVAL;

    int err = wrap_json_unpack(argsJ, "{s:s,s:s,s:o !}",
        "sourceKey", &skey,
        "destKey", &dkey,
        "aggregation", &aggregationJ );

    if (err) {
        err = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    ret = _redis_create_rule(request, skey, dkey, aggregationJ, &resstr);
    if (ret != 0)
        goto fail;

    afb_req_success(request, NULL, NULL);
    goto done;

fail:
    if (ret == -ENOMEM)
        ret = asprintf(&resstr, INSUFFICIENT_MEMORY);
    afb_req_fail(request, "error", resstr);

done:
    free(resstr);
    return;

}

static void redis_delete_rule (afb_req_t request) {
    json_object *argsJ = afb_req_json(request);
    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));

    char * skey = NULL;
    char * dkey = NULL;

    char ** argv = NULL;
    size_t * argvlen = NULL;
    char * resstr = NULL;

    int ret = -EINVAL;

    int err = wrap_json_unpack(argsJ, "{s:s,s:s !}",
        "sourceKey", &skey,
        "destKey", &dkey);

    if (err) {
        err = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    int argc = 3; // cmd, source, dest

    if ((ret = _allocate_argv_argvlen(argc, &argv, &argvlen)) != 0)
        goto fail;
        
    argc = 0;

    if ((ret = redisPutCmd(request, "TS.DELETERULE", &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisPutKey(request, skey, &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisPutKey(request, dkey, &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisSendCmd(request, argc, (const char **)argv, argvlen, NULL, &resstr)) != 0)
        goto fail;

    afb_req_success(request, NULL, NULL);
    goto done;

fail:
    if (ret == -ENOMEM)
        ret = asprintf(&resstr, INSUFFICIENT_MEMORY);
    afb_req_fail(request, "error", resstr);

done:

    free(resstr);
    argvCleanup(argc, argv, argvlen);
    return;

}

static void _redis_mrange (afb_req_t request, bool forward) {

    char * fromtimestampS;
    char * totimestampS;
    uint32_t count = 0;
    json_object * aggregationJ = NULL;
    json_object * filterJ = NULL;
    json_object * replyJ = NULL;

    char ** argv = NULL;
    size_t * argvlen = NULL;
    char * resstr = NULL;
    int ret = -EINVAL;
    uint32_t withlabels = false;

    json_object *argsJ = afb_req_json(request);
    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));

    const char * cmd = forward?"TS.MRANGE":"TS.MREVRANGE";

    int err = wrap_json_unpack(argsJ, "{s:s,s:s,s?i,s?o,s?b,s:o !}", 
        "fromts", &fromtimestampS,
        "tots", &totimestampS,
        "count", &count,
        "aggregation", &aggregationJ,
        "withlabels", &withlabels,
        "filter", &filterJ
        );
    if (err) {
        err = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    int argc = 3; // cmd, fromts, tots

    if (count != 0)
        argc++;

    if (aggregationJ)
        argc+=3;

    if (withlabels)
        argc++;

    if (filterJ) {
        argc++;
        argc += json_object_array_length(filterJ);
    }

    if ((ret = _allocate_argv_argvlen(argc, &argv, &argvlen)) != 0)
        goto fail;

    argc = 0;

    if ((ret = redisPutCmd(request, cmd, &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisPutTimestamp(request, fromtimestampS, &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisPutTimestamp(request, totimestampS, &argc, argv, argvlen)) != 0)
        goto fail;            

    if (count != 0) {

        if ((ret = _redisPutStr(request, "COUNT", &argc, argv, argvlen)) != 0)
            goto fail;

        if ((ret = _redisPutUint32(request, count, &argc, argv, argvlen)) != 0)
           goto fail;
    }

    if (aggregationJ) {
        if ((ret = redisPutAggregation(request, aggregationJ, &argc, argv, argvlen)) != 0) {
            AFB_API_ERROR (request->api, "%s: failed to put aggregation %s", __func__, json_object_get_string(aggregationJ));
            goto fail;
        }
    }

    if (withlabels)
        if ((ret = _redisPutStr(request, "WITHLABELS", &argc, argv, argvlen)) != 0)
            goto fail;

    if (filterJ)
        if ((ret = redisPutFilter(request, filterJ, true, &argc, argv, argvlen)) != 0)
            goto fail;

    if ((ret = redisSendCmd(request, argc, (const char **)argv, argvlen, &replyJ, &resstr)) != 0)
        goto fail;

    afb_req_success(request, replyJ, resstr);
    goto done;

fail:
    if (ret == -ENOMEM)
        ret = asprintf(&resstr, INSUFFICIENT_MEMORY);
    afb_req_fail(request, "error", resstr);
    free(replyJ);

done:

    free(resstr);
    argvCleanup(argc, argv, argvlen);
    return;

}

static void redis_mrange (afb_req_t request) {
    _redis_mrange(request, true);
}

static void redis_mrevrange (afb_req_t request) {
    _redis_mrange(request, false);
}


static void redis_get (afb_req_t request) {
    json_object *argsJ = afb_req_json(request);
    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));

    int ret = -EINVAL;

    char * keyS = NULL;
    json_object * replyJ = NULL;
    char * resstr = NULL;

    char ** argv = NULL;
    size_t * argvlen = NULL;


    int err = wrap_json_unpack(argsJ, "{s:s !}", 
        "key", &keyS
        );
    if (err) {
        err = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    int argc = 2; // cmd, key

    if ((ret = _allocate_argv_argvlen(argc, &argv, &argvlen)) != 0)
        goto fail;

    argc = 0;

    if ((ret = redisPutCmd(request, "TS.GET", &argc, argv, argvlen )) != 0)
        goto fail;

    if ((ret = redisPutKey(request, keyS, &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisSendCmd(request, argc, (const char **)argv, argvlen, &replyJ, &resstr)) != 0)
        goto fail;

    afb_req_success(request, replyJ, resstr);
    goto done;

fail:
    if (ret == -ENOMEM)
        ret = asprintf(&resstr, INSUFFICIENT_MEMORY);
    afb_req_fail(request, "error", resstr);
    free(replyJ);

done:

    free(resstr);
    argvCleanup(argc, argv, argvlen);
    return;

}

static void redis_mget (afb_req_t request) {
    json_object *argsJ = afb_req_json(request);
    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));

    int ret = -EINVAL;

    json_object * replyJ = NULL;
    json_object * filterJ = NULL;
    uint32_t withlabels = false;
    char * resstr = NULL;
    char ** argv = NULL;
    size_t * argvlen = NULL;

    int err = wrap_json_unpack(argsJ, "{s?b,s:o !}", 
        "withlabels", &withlabels,
        "filter", &filterJ
        );
    if (err) {
        err = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    int argc = 1; // cmd
    
    if (withlabels)
        argc++;
    
    if (filterJ) {
        argc++;
        argc += json_object_array_length(filterJ);
    }

    if ((ret = _allocate_argv_argvlen(argc, &argv, &argvlen)) != 0)
        goto fail;

    argc = 0;

    if ((ret = redisPutCmd(request, "TS.MGET", &argc, argv, argvlen )) != 0)
        goto fail;

    if (withlabels)
        if ((ret = _redisPutStr(request, "WITHLABELS", &argc, argv, argvlen)) != 0)
        goto fail;

    if (filterJ)
        if ((ret = redisPutFilter(request, filterJ, true, &argc, argv, argvlen)) != 0)
            goto fail;

    if ((ret = redisSendCmd(request, argc, (const char **)argv, argvlen, &replyJ, &resstr)) != 0)
        goto fail;
    
    afb_req_success(request, replyJ, NULL);
    goto done;

fail:
    if (ret == -ENOMEM)
        ret = asprintf(&resstr, INSUFFICIENT_MEMORY);
    afb_req_fail(request, "error", resstr);
    free(replyJ);

done:

    free(resstr);
    argvCleanup(argc, argv, argvlen);
    return;

}

static int _redis_info(afb_req_t request, const char * key, json_object ** replyJ, char ** resstr) {

    char ** argv = NULL;
    size_t * argvlen = NULL;
    int ret = -1;

    int argc = 2; // cmd + key

    if ((ret = _allocate_argv_argvlen(argc, &argv, &argvlen)) != 0)
        goto fail;

    argc = 0;

    if ((ret = redisPutCmd(request, "TS.INFO", &argc, argv, argvlen )) != 0)
        goto fail;

    if ((ret = redisPutKey(request, key, &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisSendCmd(request, argc, (const char **)argv, argvlen, replyJ, resstr)) != 0)
        goto fail;

    ret = 0;
fail:
    argvCleanup(argc, argv, argvlen);
    return ret;
}

static void redis_info (afb_req_t request) {
    json_object *argsJ = afb_req_json(request);
    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));

    json_object * replyJ = NULL;
    char * keyS = NULL;
    char * resstr = NULL;
    int ret = -EINVAL;

    int err = wrap_json_unpack(argsJ, "{s:s !}",
        "key", &keyS );
    if (err) {
        err = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    ret = _redis_info(request, keyS, &replyJ, &resstr);
    if (ret != 0)
        goto fail;
    
    afb_req_success(request, replyJ, NULL);
    goto done;

fail:
    if (ret == -ENOMEM)
        ret = asprintf(&resstr, INSUFFICIENT_MEMORY);
    afb_req_fail(request, "error", resstr);
    free(replyJ);

done:
    free(resstr);
    return;
}

static void redis_queryindex (afb_req_t request) {
    json_object *argsJ = afb_req_json(request);
    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));
    
    json_object * replyJ = NULL;
    json_object * filterJ = NULL;

    char * resstr = NULL;
    char ** argv = NULL;
    size_t * argvlen = NULL;
    int ret = -EINVAL;

    int err = wrap_json_unpack(argsJ, "{s:o !}", 
        "filter", &filterJ );
    if (err) {
        err = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    int argc = 1; // cmd

    if (filterJ) {
        argc++;
        argc += json_object_array_length(filterJ);
    }

    if ((ret = _allocate_argv_argvlen(argc, &argv, &argvlen)) != 0)
        goto fail;

    argc = 0;

    if ((ret = redisPutCmd(request, "TS.QUERYINDEX", &argc, argv, argvlen )) != 0)
        goto fail;

    if ((ret = redisPutFilter(request, filterJ, false, &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisSendCmd(request, argc, (const char **)argv, argvlen, &replyJ, &resstr)) != 0)
        goto fail;

    afb_req_success(request, replyJ, NULL);
    goto done;

fail:
    if (ret == -ENOMEM)
        ret = asprintf(&resstr, INSUFFICIENT_MEMORY);
    afb_req_fail(request, "error", resstr);
    free(replyJ);

done:

    free(resstr);
    argvCleanup(argc, argv, argvlen);

    return;
}


static void ts_jinsert (afb_req_t request) {

    struct cds_list_head list;
    CDS_INIT_LIST_HEAD(&list);

    json_object* argsJ = afb_req_json(request);
    json_object* replyJ = NULL;
    json_object* dataJ = NULL;
    char * class = NULL;
    char * resstr = NULL;
    char * timestampS = NULL;
    char * _timestampS = NULL;
    int ret = -EINVAL;

    int err = wrap_json_unpack(argsJ, "{s:s, s?s, s:o !}",
        "class", &class,
        "timestamp", &timestampS,
        "data", &dataJ );
    if (err) {
        err = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));

    if (timestampS == NULL) {
        err = asprintf(&_timestampS , "%ld", time(NULL));
        if (err == -1) {
            fprintf(stderr, "%s ts error\n", __func__);
            goto fail;
        }
    }
    else {
        _timestampS = timestampS;
    }

    json2table(class, dataJ, &list);

    JSON_PAIR * pair;

    replyJ = json_object_new_object();
    cds_list_for_each_entry(pair, &list, node) {

#ifdef DEBUG
        json_object * val;
        if (pair->type == VALUE_TYPE_DOUBLE)
            val = json_object_new_double(pair->d.value);
        else
            val = json_object_new_string(pair->d.s);

        json_object_object_add(replyJ, pair->key, val );
#endif

        if (pair->type == VALUE_TYPE_DOUBLE) {
            if ((ret = internal_redis_add_double(request, pair->key, pair->d.value, _timestampS, class, &resstr)) != 0)
                goto fail;
        } else {
            if ((ret = internal_redis_add_string(request, pair->key, pair->d.s, _timestampS, class, &resstr)) != 0)
                goto fail;
        }
    }

    afb_req_success(request, replyJ, resstr);
    goto done;

fail:
    if (ret == -ENOMEM)
        ret = asprintf(&resstr, INSUFFICIENT_MEMORY);
    afb_req_fail(request, "error", resstr);
    free(replyJ);

done:
    free(resstr);
    if (timestampS == NULL)
        free(_timestampS);

    return;
}


static void ts_mget (afb_req_t request) {

    json_object* argsJ = afb_req_json(request);
    json_object* replyJ = NULL;

    char * class = NULL;
    char * resstr = NULL;
    int ret = -EINVAL;
    char ** argv = NULL;
    size_t * argvlen = NULL;

    int err = wrap_json_unpack(argsJ, "{s:s !}",
        "class", &class );
    if (err) {
        err = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    /* retrieves keys having this class label */

    int argc = 3; /* cmd + FILTER + filter */
    char * filter = NULL;

    if ((ret == asprintf(&filter, "class=%s", class)) != 0)
        goto fail;

    if ((ret = _allocate_argv_argvlen(argc, &argv, &argvlen)) != 0)
        goto fail;

    argc = 0;
    if ((ret = redisPutCmd(request, "TS.MGET", &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = _redisPutStr(request, "FILTER", &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = _redisPutStr(request, filter, &argc, argv, argvlen)) != 0)
        goto fail;

    redisReply * rep = __redisCommandArgv(syncRedisContext, argc,  (const char **)argv, argvlen);
    if (rep == NULL) {
        ret = asprintf(&resstr, "redis-error: redis command failed");
        goto fail;
    }

    AFB_API_INFO (request->api, "%s: cmd result type %s, str %s", __func__, REDIS_REPLY_TYPE_STR(rep->type), rep->str);

    if (rep->type == REDIS_REPLY_ERROR) {
        ret = asprintf(&resstr, "redis_command error %s", rep->str);
        goto fail;
    }

    if (rep->type != REDIS_REPLY_ARRAY) {
        ret = asprintf(&resstr, "%s: unexpected response type", __func__);
        goto fail;
    }

    if ((ret = mgetReply2Json( rep, class, &replyJ )) != 0)
        goto fail;

    afb_req_success(request, replyJ, resstr);
    goto done;

fail:
    if (ret == -ENOMEM)
        ret = asprintf(&resstr, INSUFFICIENT_MEMORY);
    afb_req_fail(request, "error", resstr);
    free(replyJ);

done:
    free(filter);
    free(resstr);

    argvCleanup(argc, argv, argvlen);

    return;
}


static void ts_mrange (afb_req_t request) {

    json_object* argsJ = afb_req_json(request);
    json_object* replyJ = NULL;

    char * class = NULL;
    char * resstr = NULL;
    int ret = -EINVAL;
    char ** argv = NULL;
    size_t * argvlen = NULL;
    char * fromtsS = NULL;
    char * totsS = NULL;

    int err = wrap_json_unpack(argsJ, "{s:s, s:s, s:s!}",
        "class", &class,
        "fromts", &fromtsS,
        "tots", &totsS );
    if (err) {
        err = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    if (fromtsS == NULL)
        fromtsS = "-";

    if (totsS == NULL)
        totsS = "+";
    
    int argc = 5; /* cmd + fromts + tots + FILTER + filter */
    char * filter = NULL;

    if ((ret = asprintf(&filter, "class=%s", class)) == -1)
        goto fail;

    if ((ret = _allocate_argv_argvlen(argc, &argv, &argvlen)) != 0)
        goto fail;

    argc = 0;
    if ((ret = redisPutCmd(request, "TS.MRANGE", &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisPutTimestamp(request, fromtsS, &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = redisPutTimestamp(request, totsS, &argc, argv, argvlen)) != 0)
        goto fail;   

    if ((ret = _redisPutStr(request, "FILTER", &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = _redisPutStr(request, filter, &argc, argv, argvlen)) != 0)
        goto fail;

    redisReply * rep = __redisCommandArgv(syncRedisContext, argc,  (const char **)argv, argvlen);
    if (rep == NULL) {
        ret = asprintf(&resstr, "redis-error: redis command failed");
        goto fail;
    }


    AFB_API_INFO (request->api, "%s: cmd result type %s, str %s", __func__, REDIS_REPLY_TYPE_STR(rep->type), rep->str);

    if (rep->type == REDIS_REPLY_ERROR) {
        ret = asprintf(&resstr, "redis_command error %s", rep->str);
        goto fail;
    }

    if (rep->type != REDIS_REPLY_ARRAY) {
        ret = asprintf(&resstr, "%s: unexpected response type", __func__);
        goto fail;
    }

    if ((ret = mrangeReply2Json( rep, class, &replyJ )) != 0)
        goto fail;

    afb_req_success(request, replyJ, resstr);
    goto done;

fail:
    if (ret == -ENOMEM)
        ret = asprintf(&resstr, INSUFFICIENT_MEMORY );
    afb_req_fail(request, "error", resstr);

done:
    free(filter);
    free(resstr);
    argvCleanup(argc, argv, argvlen);


}

/* Attempts to use TS.CREATE, or TS.ALTER, to properly set the class label, blob flag, and duplication policy */

static int _redisSetKeyProperties(afb_req_t request, const char * key, const char * class, bool isBlob) {
    int ret  = -1;
    int argc = 7; // 1 cmd, 1 key, 3 for label, 2 duplicate policy
    char ** argv = NULL;
    size_t * argvlen = NULL;
    char* resstr;

    if (isBlob)
        argc++;

    if ((ret = _allocate_argv_argvlen(argc, &argv, &argvlen)) != 0) 
        goto fail;

    argc = 0;

    if (redisPutCmd(request, "TS.CREATE", &argc, argv, argvlen) != 0)
        goto fail;

    if (_redisPutStr(request, key, &argc, argv, argvlen) != 0)
        goto fail;

    if (internal_redis_put_class_label(request, class, &argc, argv, argvlen) !=0 )
        goto fail;
    
    if (_redisPutDuplicatePolicyLast(request, &argc, argv, argvlen) != 0)
        goto fail;

    if (isBlob && _redisPutBlob(request, &argc, argv, argvlen) != 0) 
        goto fail;

    if (redisSendCmd(request, argc, (const char **)argv, argvlen, NULL, &resstr) == 0)
        goto done;

    // if CREATE fails, attempt with ALTER

    free(argv[0]);
    argv[0] = strdup("TS.ALTER");
    argvlen[0] = strlen(argv[0]);

    if (redisSendCmd(request, argc, (const char **)argv, argvlen, NULL, &resstr) != 0)
        goto fail;

done:
    ret = 0;

fail:
    argvCleanup(argc, argv, argvlen);
    return ret;

}

/*
ts_minsert has been created for replicating a data set

Expected input: a timestamps array, and an array of samples
{
    "class":"sensor2",
    "ts": [1606743420408, 1606743426621, 1606743429893],
    "data": [
        [ "sensor2[0]", [ "cool" , "cool, "cool" ] ],           // a sample
        [ "sensor2[1]", [ "groovy", "groovy", "groovy" ] ],  
        [ "sensor2[2]", [ 6, 6, 6 ] ],  
        [ "sensor2[3]", [ 23.3, 23.6, 23.7 ] ]
    ]
  }

*/

static void ts_minsert (afb_req_t request) {
    char ** tsArray = NULL;
    json_object* argsJ = afb_req_json(request);
    char * resstr;
    char * class;
    uint32_t nbts = 0;

    json_object * timestampsTableJ;
    json_object * dataJ;
    
    int err = wrap_json_unpack(argsJ, "{s:s s:o, s:o!}",
        "class", &class,
        "ts", &timestampsTableJ,
        "data", &dataJ
    );

    if (err) {
        err = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    if (!json_object_is_type(timestampsTableJ, json_type_array)) {
        err = asprintf(&resstr, "json error: '%s' should be an array of timestamps", json_object_get_string(argsJ));
        goto fail;
    }

    if (!json_object_is_type(dataJ, json_type_array)) {
        err = asprintf(&resstr, "json error: '%s' should be an array of samples", json_object_get_string(argsJ));
        goto fail;
    }

    /* The number of sample values must match the number of timestamps */

    nbts = json_object_array_length(timestampsTableJ);
    uint32_t nbkeys = json_object_array_length(dataJ);

    /* compute the 'converted to string' timestamp once */
    tsArray = (char**) calloc(nbts, sizeof(char*));
    if (!tsArray)
        goto fail;

    for (int ix=0; ix<nbts; ix++) {
        json_object * tsJ = json_object_array_get_idx(timestampsTableJ, ix);
        int ret = asprintf(&tsArray[ix], "%ld", json_object_get_int64(tsJ));
        if (!ret)
            goto fail;
    }

    for (int ix=0; ix < nbkeys; ix++) {
        const char * key;
        json_object* sampleJ = json_object_array_get_idx(dataJ, ix);

        if (!json_object_is_type(sampleJ, json_type_array))  {
            err = asprintf(&resstr, "json error: sample '%s' must be an array", json_object_get_string(sampleJ));
            goto fail;
        }

        if (json_object_array_length(sampleJ) != 2) {
            err = asprintf(&resstr, "json error: sample '%s' must have 2 entries", json_object_get_string(sampleJ));
            goto fail;
        }

        json_object* keyJ = json_object_array_get_idx(sampleJ, 0);
        json_object* valuesJ = json_object_array_get_idx(sampleJ, 1);

        if (!json_object_is_type(keyJ, json_type_string))  {
            err = asprintf(&resstr, "json error: key '%s' should be a string", json_object_get_string(keyJ));
            goto fail;
        }

        key = json_object_get_string(keyJ);

        if (!json_object_is_type(valuesJ, json_type_array))  {
            err = asprintf(&resstr, "json error: key '%s' must have an array of values (instead of %s)", key, json_object_get_string(valuesJ));
            goto fail;
        }

        int nbvalues = json_object_array_length(valuesJ);
        if (nbvalues != nbts) {
            err = asprintf(&resstr, "json error: key '%s' should have %d values", key, nbts);
            goto fail;
        }
        for (int jx=0; jx < nbvalues; jx++)  {
            
            char ** argv = NULL;
            size_t * argvlen = NULL;
            char * resstr = NULL;
            int ret;

            int argc = 4; /* 1 slot for command name, 1 for the key, 1 for timestamp, 1 for value */

            json_object * valueJ = json_object_array_get_idx(valuesJ, jx);
            json_type type = json_object_get_type(valueJ);

            if (jx == 0) {
                if (_redisSetKeyProperties(request, key, class, type == json_type_string) != 0)
                    goto fail;
            }

            if ((ret = _allocate_argv_argvlen(argc, &argv, &argvlen)) != 0) 
                goto fail;

            argc = 0;

            // +3
            if ((ret = internal_redis_add_cmd(request, key, tsArray[jx], &argc, argv, argvlen)) != 0)
                goto fail;

            if (type == json_type_boolean) {
                ret = redisPutValue(request, json_object_get_boolean(valueJ), &argc, argv, argvlen);
            }
            else if (type == json_type_double) {
                ret = redisPutValue(request, json_object_get_double(valueJ), &argc, argv, argvlen);
            }
            else if (type == json_type_int) {
                ret = redisPutValue(request, json_object_get_int(valueJ), &argc, argv, argvlen);
            }
            else if (type == json_type_string) {
                ret = _redisPutStr(request, json_object_get_string(valueJ), &argc, argv, argvlen);
            }
            else
                goto fail;

            if (ret != 0)
                goto fail;

            if ((ret = redisSendCmd(request, argc, (const char **)argv, argvlen, NULL, &resstr)) != 0)
                goto fail;
            
            argvCleanup(argc, argv, argvlen);

        }

    }

    afb_req_success(request, NULL, resstr);
    goto done;

fail:
    afb_req_fail(request, "error", resstr);
done:
    if (tsArray)
        for (int ix=0; ix<nbts;ix++)
            free(tsArray[ix]);
    free(tsArray);
    return;

}

static json_object * ts_info_get_field(json_object* infoJ, const char * fieldS) {
    for (int ix=0; ix<json_object_array_length(infoJ); ix++) {

        json_object * field = json_object_array_get_idx(infoJ, ix);
        if (!json_object_is_type(field, json_type_string))
            continue;

        if (strcmp(json_object_get_string(field), fieldS) == 0) {
            return json_object_array_get_idx(infoJ, ix+1);
        }
    }
    return NULL;
}

/*
converts:
[
    [
    "class",
    "sensor2"
    ]
]
to:
{ "class":"sensor2" }
*/


static json_object* ts_info_labels_to_create_labels(json_object * infoLabelsJ) {

    fprintf(stderr, "converting labels %s\n", json_object_get_string(infoLabelsJ));

    json_object * result = json_object_new_object();

    for (int ix=0; ix<json_object_array_length(infoLabelsJ); ix++) {
        json_object* labelJ = json_object_array_get_idx(infoLabelsJ, ix);

        fprintf(stderr, "converting label %s\n", json_object_get_string(labelJ));

        if (json_object_array_length(labelJ) != 2) {
            fprintf(stderr, "wrong size\n");
            continue;
        }

        json_object* keyJ = json_object_array_get_idx(labelJ, 0);
        json_object* valueJ= json_object_array_get_idx(labelJ, 1);

        if (!json_object_is_type(keyJ, json_type_string))
            continue;

        json_object_object_add(result, json_object_get_string(keyJ), valueJ);
    }
    return result;
}

/*
    creates a subkey with the given aggregation rule
    For instance, if the keyname is foo[1].bla,
    the subkey will be called foo[1].bla_<name>
    For convenience and easy retrieval of all the keys, the following label is added:

    "class=<parent_class>|<name>" 

*/

static int _key_aggregate(afb_req_t request, const char * key, json_object * argsJ) {
    int ret = -1;
    char * class;
    char * name;
    json_object * aggregationJ;
    json_object * infoJ;

    char * dstkey;
    char * resstr = NULL;
    bool blob = false;

    int err = wrap_json_unpack(argsJ, "{s:s, s:s, s:o!}",
        "class", &class,
        "name", &name,
        "aggregation", &aggregationJ
    );
    if (err)
        goto fail;

    ret = asprintf(&dstkey, "%s|%s", key, name);
    if (ret == -1)
        goto fail;

    // get info about the key
    ret = _redis_info(request, key, &infoJ, &resstr);
    if (ret != 0) {
        AFB_API_ERROR(request->api, "Failed to get info about key %s", resstr);
        goto fail;
    }

    const char* typeS = NULL;
    json_object* typeJ = ts_info_get_field(infoJ, "type");

    if (typeJ && json_object_is_type(typeJ, json_type_string)) {
        typeS = json_object_get_string(typeJ);
    }

    if (strcmp(typeS, "blob") == 0)
        blob = true;

    /* let's inherit from the parent's labels */
    json_object* infoLabelsJ = ts_info_get_field(infoJ, "labels");
    json_object* labelsJ  = ts_info_labels_to_create_labels(infoLabelsJ);

    json_object * parent_classJ;
    json_object_object_get_ex(labelsJ, "class", &parent_classJ);

    const char *parent_class = json_object_get_string(parent_classJ);
    char *class_name = NULL;
    ret = asprintf(&class_name, "%s|%s", parent_class, name);

    json_object_object_del(labelsJ, "class");
    json_object_object_add(labelsJ, "class", json_object_new_string(class_name));

    // create the subkey
    AFB_API_INFO(request->api, "Creating subkey %s (%s)", dstkey, blob?"blob":"scalar");

    ret = _redis_create(request, dstkey, 0, false, labelsJ, blob, &resstr);
    if (ret != 0) {
        AFB_API_ERROR(request->api, "subkey %s creation error %s", dstkey, resstr);
        goto fail;
    }

    // apply the rule
    ret = _redis_create_rule(request, key, dstkey, aggregationJ, &resstr);
    if (ret != 0) {
        AFB_API_ERROR(request->api, "subkey %s rule creation error %s", dstkey, resstr);
        goto fail;
    }

    AFB_API_INFO(request->api, "subkey %s created", dstkey);

    ret = 0;

fail:

    if (infoJ)
        json_object_put(infoJ);

    free(resstr);
    free(class_name);
    free(dstkey);

    return ret;
}

/*
Creates a compaction rule for all the keys of class 'class'

'{ "class":"sensor2", "name":"avg", "aggregation": {"type": "avg", "bucket":500} }'

*/

static void ts_maggregate (afb_req_t request) {
    char * resstr = NULL;
    json_object* argsJ = afb_req_json(request);
    json_object* replyJ = NULL;
    json_object* aggregationJ;
    char * class;
    char * name;

    int err = wrap_json_unpack(argsJ, "{s:s, s:s, s:o!}",
        "class", &class,
        "name", &name,
        "aggregation", &aggregationJ
    );
    if (err) {
        err = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    err = class_keys_for_each(request, class, _key_aggregate, argsJ);
    if (err) {
        err = asprintf(&resstr, "failed to apply rule on class '%s', aggregation: '%s'", class, json_object_get_string(aggregationJ));
        goto fail;
    }

    afb_req_success(request, replyJ, resstr);
    goto done;

fail:
    afb_req_fail(request, "error", resstr);
done:
    return;

}

/* Get all the keys of the given class */

static redisReply * get_class_keys(afb_req_t request, const char * class) {
    int argc = 2; /* cmd + filter */
    char * filter = NULL;
    int ret;
    char ** argv = NULL;
    size_t * argvlen = NULL;
    redisReply* rep = NULL;

    if ((ret = asprintf(&filter, "class=%s", class)) == -1)
        goto fail;

    if (_allocate_argv_argvlen(argc, &argv, &argvlen) != 0)
        goto fail;

    argc = 0;
    if (redisPutCmd(request, "TS.QUERYINDEX", &argc, argv, argvlen) != 0)
        goto fail;

    if (_redisPutStr(request, filter, &argc, argv, argvlen) != 0)
        goto fail;

    rep = __redisCommandArgv(syncRedisContext, argc,  (const char **)argv, argvlen);

fail:
    argvCleanup(argc, argv, argvlen);
    return rep;
}


static int _key_del(afb_req_t request, const char * key, json_object * not_used) {
    int argc = 2;
    char ** argv = NULL;
    size_t * argvlen = NULL;
    redisReply* rep = NULL;
    int ret = -1;

    if ((ret = _allocate_argv_argvlen(argc, &argv, &argvlen)) != 0)
        goto fail;

    argc = 0;

    if ((ret = redisPutCmd(request, "DEL", &argc, argv, argvlen)) != 0)
        goto fail;

    if ((ret = _redisPutStr(request, key, &argc, argv, argvlen)) != 0)
        goto fail;

    rep = __redisCommandArgv(syncRedisContext, argc,  (const char **)argv, argvlen);
    if (rep == NULL) {
        AFB_API_ERROR (request->api, "%s: error while deleting key %s", __func__, key);
        goto fail;
    }

    ret = 0;
fail:
    return ret;

}

static void ts_mdel (afb_req_t request) {

    json_object* argsJ = afb_req_json(request);
    json_object* replyJ = NULL;

    char * class = NULL;
    char * resstr = NULL;

    int err = wrap_json_unpack(argsJ, "{s:s !}",
        "class", &class );
    if (err) {
        err = asprintf(&resstr, "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    err = class_keys_for_each(request, class, _key_del, NULL);
    if (err) {
        err = asprintf(&resstr, "failed to delete keys of class '%s'", class);
        goto fail;
    }

    afb_req_success(request, replyJ, resstr);
    goto done;

fail:
    afb_req_fail(request, "error", resstr);

done:
    free(resstr);
    return;

}

static afb_verb_t CtrlApiVerbs[] = {
    /* VERB'S NAME         FUNCTION TO CALL         SHORT DESCRIPTION */
    { .verb = "ping",     .callback = ctrlapi_ping     , .info = "ping test for API"},
    { .verb = "create", .callback = redis_create , .info = "create a timed value in TS" },
    { .verb = "del", .callback = redis_del , .info = "Remove the specified keys" },
    { .verb = "alter", .callback = redis_alter , .info = "Update the retention, labels of an existing key." },
	{ .verb = "add", .callback = redis_add , .info = "add a timed value in TS" },
    { .verb = "madd", .callback = redis_madd , .info = "append new samples o a list of series" },
    { .verb = "incrby", .callback = redis_incrby , .info = "Creates a new sample that increments the latest sample's value" },
    { .verb = "decrby", .callback = redis_decrby , .info = "Creates a new sample that decrements the latest sample's value" },
    { .verb = "create_rule", .callback = redis_create_rule , .info = "Create a compaction rule" },
    { .verb = "delete_rule", .callback = redis_delete_rule , .info = "Delete a compaction rule" },
    { .verb = "range", .callback = redis_range , .info = "query a range in TS" },
    { .verb = "mrange", .callback = redis_mrange , .info = "query a timestamp range across multiple time-series by filters (forward)" },
    { .verb = "mrevrange", .callback = redis_mrevrange , .info = "query a timestamp range across multiple time-series by filters (reverse)" },
    { .verb = "get", .callback = redis_get , .info = "get the last sample TS" },
    { .verb = "mget", .callback = redis_mget , .info = "get the last samples matching the specific filter." },
    { .verb = "info", .callback = redis_info , .info = "returns information and statistics on the time-series." },
    { .verb = "queryindex", .callback = redis_queryindex , .info = "get all the keys matching the filter list." },

    { .verb = "ts_jinsert", .callback = ts_jinsert, .info = "insert a json object in the database "},
    { .verb = "ts_mget", .callback = ts_mget, .info = "gets a flattened json object from the database (latest sample) "},
    { .verb = "ts_mdel", .callback = ts_mdel, .info = "deletes a flattened json object from the database, giving its class name"},
    { .verb = "ts_minsert", .callback = ts_minsert, .info = "insert replication set in the database "},
    { .verb = "ts_mrange", .callback = ts_mrange, .info = "gets a flattened json object from the database (with time range) "},
    { .verb = "ts_maggregate", .callback = ts_maggregate, .info = "creates a compaction rule for all the keys of the given class"},

    { .verb = NULL} /* marker for end of the array */
};

static int CtrlLoadStaticVerbs (afb_api_t apiHandle, afb_verb_t *verbs) {
    int errcount=0;

    for (int idx=0; verbs[idx].verb; idx++) {

        errcount+= afb_api_add_verb(apiHandle,
        		                    CtrlApiVerbs[idx].verb,
									CtrlApiVerbs[idx].info,
									CtrlApiVerbs[idx].callback,
									(void*)&CtrlApiVerbs[idx],
									CtrlApiVerbs[idx].auth, 0, 0);
    }

    return errcount;
};

static int CtrlLoadOneApi (void *cbdata, afb_api_t apiHandle) {
    CtlConfigT *ctrlConfig = (CtlConfigT*) cbdata;

    // save closure as api's data context
    afb_api_set_userdata(apiHandle, ctrlConfig);

    // add static controls verbs
    int error = CtrlLoadStaticVerbs (apiHandle, CtrlApiVerbs);
    if (error) {
        AFB_API_ERROR(apiHandle, "CtrlLoadSection fail to Registry static V2 verbs");
        goto OnErrorExit;
    }

    // load section for corresponding API
    error= CtlLoadSections(apiHandle, ctrlConfig, ctrlSections);

    // declare an event event manager for this API;
    afb_api_on_event(apiHandle, CtrlDispatchApiEvent);

    // should not seal API as each mixer+stream create a new verb
    // afb_dynapi_seal(apiHandle);
    return error;

OnErrorExit:
    return 1;
}

int afbBindingEntry(afb_api_t apiHandle) {

    AFB_default = apiHandle;

    AFB_API_INFO(apiHandle, "%s...", __func__);

    const char *dirList = getenv("CONTROL_CONFIG_PATH");
    if (!dirList) dirList=CONTROL_CONFIG_PATH;

    // Select correct config file
    char *configPath = CtlConfigSearch(apiHandle, dirList, "redis-binding");

    if (!configPath) {
        AFB_API_ERROR(apiHandle, "CtlConfigSearch: No redis-%s-* config found in %s ", GetBinderName(), dirList);
        goto OnErrorExit;
    }

    AFB_API_INFO(apiHandle, "Using %s", configPath);

    // load config file and create API
    CtlConfigT *ctrlConfig = CtlLoadMetaData (apiHandle, configPath);
    if (!ctrlConfig) {
        AFB_API_ERROR(apiHandle, "CtlLoadMetaData No valid control config file in:\n-- %s", configPath);
        goto OnErrorExit;
    }

    if (!ctrlConfig->api) {
        AFB_API_ERROR(apiHandle, "CtlLoadMetaData API Missing from metadata in:\n-- %s", configPath);
        goto OnErrorExit;
    }

    AFB_API_NOTICE (apiHandle, "Controller API='%s' info='%s'", ctrlConfig->api, ctrlConfig->info);
    // create one API per config file (Pre-V3 return code ToBeChanged)

    afb_api_t handle = afb_api_new_api(apiHandle, ctrlConfig->api, ctrlConfig->info, 1, CtrlLoadOneApi, ctrlConfig);

    int status = 0;

    // config exec should be done after api init in order to enable onload to use newly defined ctl API.
    if (handle)
        status = CtlConfigExec (apiHandle, ctrlConfig);

    return status;

OnErrorExit:
    return -1;
}

static int onloadConfig(afb_api_t api, CtlSectionT *section, json_object *actionsJ) {

    AFB_API_NOTICE (api, "%s...", __func__);
    size_t count;
    int ix;
    json_object * redisJ;

    if (json_object_get_type(actionsJ) == json_type_array) {
    	count = json_object_array_length(actionsJ);
    	for (ix = 0 ; ix < count; ix++) {
    		json_object * obj = json_object_array_get_idx(actionsJ, ix);
    		if (json_object_object_get_ex(obj, "redis", &redisJ)) {
    			initRedis(api, redisJ);
    			break;
    		}
    	}

    } else {
    	if (json_object_object_get_ex(actionsJ, "redis", &redisJ)) {
    		initRedis(api, redisJ);
    	}
    }

    return 0;

}

static char * hostname;
static uint32_t port;


static void connectCallback(const redisAsyncContext *c, int status);
static void disconnectCallback(const redisAsyncContext *c, int status);

static void connectRedis(afb_api_t api) {
    redisAsyncContext *c = NULL;

    c = redisAsyncConnect(hostname, port);
    if (c->err) {
        AFB_API_ERROR(api, "Connection error: can't allocate async redis context: %s\n", c->errstr);
        return;
    }

    sd_event * event = afb_api_get_event_loop(api);

    if (redisSdAttach(api, c, event) != REDIS_OK)
        return;

    redisAsyncSetConnectCallback(c,connectCallback);
    redisAsyncSetDisconnectCallback(c,disconnectCallback);
}

static bool logConnectionRefusedOnce = false;

static void connectCallback(const redisAsyncContext *c, int status) {

    afb_api_t api = c->data;

    if (status != REDIS_OK) {
        if (!logConnectionRefusedOnce)
            AFB_API_ERROR(api, "Error: %s\n", c->errstr);
        logConnectionRefusedOnce = true;

        usleep(200);
        connectRedis(api);
        return;
    }

    asyncRedisContext = (redisAsyncContext*) c;

    /* We use, for now, an additionnal synchronous connection,
       regarding the significant mandatory changes that would be needed
       to use full asynchronous requests calls */

    syncRedisContext = redisConnect(hostname, port);
    if (syncRedisContext == NULL) {
        AFB_API_ERROR(api, "Failed to get a SYNC connection");
    } else {
        AFB_API_NOTICE(api, "Connected...");
    }

}

static void disconnectCallback(const redisAsyncContext *c, int status) {

    afb_api_t api = c->data;
    logConnectionRefusedOnce = false;

    if (status != REDIS_OK) {
        AFB_API_ERROR(api, "Error: %s", c->errstr);
    }

    AFB_API_NOTICE(api, "Disconnected...");
    syncRedisContext = NULL;
    asyncRedisContext = NULL;
    connectRedis(api);
}

static void initRedis(afb_api_t api, json_object * redisJ) {

	int error = wrap_json_unpack(redisJ, "{ss,si !}",
            "hostname", &hostname,
			"port", &port
            );

	if (error) {
	    AFB_API_NOTICE (api, "%s failed to get redis params %s", __func__, json_object_get_string(redisJ));
		goto fail;
	}

    AFB_API_NOTICE (api, "Connect to %s:%d", hostname, port);
    connectRedis(api);

fail:
	return;
}

