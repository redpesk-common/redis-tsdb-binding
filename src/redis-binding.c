
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

// default api to print log when apihandle not available
afb_api_t AFB_default;

static redisContext * currentRedisContext = NULL;

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


static int redisPutRetention(afb_req_t request, uint32_t retention, int * argc, char ** argv, size_t * argvlen) {
    int ret = ENOMEM;

    argv[*argc] = strdup("RETENTION");
    if (argv[*argc] == NULL)
        goto nomem;
    argvlen[*argc] = strlen(argv[*argc]);
    (*argc)++;

    if (asprintf(&argv[*argc], "%d", retention) == -1)
        goto nomem;
    argvlen[*argc] = strlen(argv[*argc]);
    (*argc)++;

    ret = 0;
nomem:
    return ret;
}

static int redisPutUncompressed(int * argc, char ** argv, size_t * argvlen) {
    int ret = ENOMEM;
    argv[*argc] = strdup("UNCOMPRESSED");
    if (argv[*argc] == NULL)
        goto nomem;

    argvlen[*argc] = strlen(argv[*argc]);
    (*argc)++;

    ret = 0;
nomem:
    return ret;
}


static int redisPutLabels(afb_req_t request, json_object * labelsJ, int * argc, char ** argv, size_t * argvlen) {
    int ret = EINVAL;
    enum json_type type;

    AFB_API_INFO (request->api, "%s: put labels", __func__);

    argv[*argc] = strdup("LABELS");
    if (argv[*argc] == NULL)
        goto nomem;

    argvlen[*argc] = strlen(argv[*argc]);
    (*argc)++;

    json_object_object_foreach(labelsJ, key, val) {
        type = json_object_get_type(val);
        switch (type) {
        case json_type_string: 
            argv[*argc] = strdup(key);
            if (argv[*argc] == NULL)
                goto nomem;
            argvlen[*argc] = strlen(argv[*argc]);
            (*argc)++;

            argv[*argc] = strdup(json_object_get_string(val));
            if (argv[*argc] == NULL)
                goto nomem;

            argvlen[*argc] = strlen(argv[*argc]);
            (*argc)++;
            break;

        case json_type_int: {
            argv[*argc] = strdup(key);
            if (argv[*argc] == NULL)
                goto nomem;
            argvlen[*argc] = strlen(argv[*argc]);
            (*argc)++;

            if (asprintf(&argv[*argc], "%d", json_object_get_int(val)) == -1)
                goto nomem;
            argvlen[*argc] = strlen(argv[*argc]);
            (*argc)++;
            break;
        }
        
        default:
            afb_req_fail_f(request, "invalid-syntax", "labels must be a string or an int (given:%s)", json_object_get_string(val));
            goto fail;
            break;
        }
    }

    ret = 0;
    goto done;
nomem:
    ret = ENOMEM;
fail:
done:
    return ret;

}


static int redisPutAggregation(afb_req_t request, json_object * aggregationJ, int * argc, char ** argv, size_t * argvlen) {
    int ret = EINVAL;
    char * aggregationType = NULL;
    uint32_t bucket = 0;

    if (!aggregationJ)
        goto fail;

    int err = wrap_json_unpack(aggregationJ, "{ss,si !}",
        "type", &aggregationType,
        "bucket", &bucket
        );
    if (err) {
        afb_req_fail_f(request, "parse-error", "json error in '%s'", json_object_get_string(aggregationJ));
        goto fail;
    }

    argv[*argc] = strdup("AGGREGATION");
    if (argv[*argc] == NULL)
        goto nomem;

    argvlen[*argc] = strlen(argv[*argc]);
    (*argc)++;

    argv[*argc] = strdup(aggregationType);
    if (argv[*argc] == NULL)
        goto nomem;

    argvlen[*argc] = strlen(argv[*argc]);
    (*argc)++;

    if (asprintf(&argv[*argc], "%d", bucket) == -1)
        goto nomem;

    argvlen[*argc] = strlen(argv[*argc]);
    (*argc)++;

    ret = 0;
    goto done;
nomem:
    ret = ENOMEM;
fail:
done:
    return ret;
}


/*
#define REDIS_REPLY_STRING 1
#define REDIS_REPLY_ARRAY 2
#define REDIS_REPLY_INTEGER 3
#define REDIS_REPLY_NIL 4
#define REDIS_REPLY_STATUS 5
#define REDIS_REPLY_ERROR 6
*/

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
    int ret = EINVAL;
    if (!reply)
        goto fail;
    if (!replyJ)
        goto fail;

    *replyJ = NULL;

    AFB_API_INFO (request->api, "%s: convert type %s", __func__, REDIS_REPLY_TYPE_STR(reply->type));

    switch (reply->type)
    {
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
        //json_object_object_add(*replyJ, "v", arrayJ);
        *replyJ = arrayJ;
        break;
    }

    case REDIS_REPLY_INTEGER: {
        AFB_API_INFO (request->api, "%s: got int %lld", __func__, reply->integer);
        json_object * intJ = json_object_new_int64(reply->integer);
        if (!intJ)
            goto nomem;
        // json_object_object_add(*replyJ, "v", intJ);
        *replyJ = intJ;
        break;
    }

    case REDIS_REPLY_STRING: {
        AFB_API_INFO (request->api, "%s: got string %s", __func__, reply->str);
        json_object * strJ = json_object_new_string(reply->str);
        if (!strJ)
            goto nomem;
        *replyJ = strJ;
        break;
    }

    default:
        AFB_API_INFO (request->api, "%s: Unhandled result type %s, str %s", __func__, REDIS_REPLY_TYPE_STR(reply->type), reply->str);
        goto fail;
        break;
    }

    ret = 0;
    goto done;
nomem:
    ret = ENOMEM;
fail:
    if (*replyJ)
        free(*replyJ);
done:
    return ret;
}


static int redis_send_cmd(afb_req_t request, int argc, const char ** argv, const size_t * argvlen, json_object ** replyJ, char ** resstr) {
    int ret = -1;
    
    AFB_API_INFO (request->api, "%s: send cmd", __func__);

    if (resstr)
        *resstr = NULL;

    redisReply * rep = redisCommandArgv(currentRedisContext, argc, argv, argvlen);
    if (rep == NULL) {
        afb_req_fail_f(request, "redis-error", "redis command failed");
    	goto fail;
    }

    AFB_API_INFO (request->api, "%s: cmd result type %s, str %s", __func__, REDIS_REPLY_TYPE_STR(rep->type), rep->str);

    if (rep->type == REDIS_REPLY_ERROR) {
        ret = asprintf(resstr, "redis_command error %s", rep->str);
    	goto fail;
    }

    if (!replyJ)
        goto done;

    ret = redisReplyToJson(request, rep, replyJ);
    if (ret != 0 || *replyJ == NULL) {
        ret = asprintf(resstr, "failed to convert reply to json");
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

static void redis_create (afb_req_t request) {
    json_object *argsJ = afb_req_json(request);
    char * rkey = NULL;
    int32_t retention = 0;
    uint32_t uncompressed = false;
    json_object * labelsJ = NULL;

    char ** argv = NULL;
    size_t * argvlen = NULL;
    char * resstr = NULL;

    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));

    int err = wrap_json_unpack(argsJ, "{ss,s?i,s?b,s?o !}", 
        "key", &rkey,
        "retention", &retention,
        "uncompressed", &uncompressed,
        "labels", &labelsJ
        );
    if (err) {
        afb_req_fail_f(request, "parse-error", "json error in '%s'", json_object_get_string(argsJ));
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

    argv = calloc(argc, sizeof(char*));
    if (argv == NULL)
        goto nomem;

    argvlen = calloc(argc, sizeof(size_t));
    if (argvlen == NULL)
        goto nomem;

    argc = 0;
    argv[argc] = strdup("TS.CREATE");
    if (argv[argc] == NULL)
        goto nomem;

    argvlen[argc] = strlen(argv[argc]);
    argc++;

    argv[argc] = strdup(rkey);
    if (argv[argc] == NULL)
        goto nomem;

    argvlen[argc] = strlen(argv[argc]);
    argc++;

    if (retention) 
        if (redisPutRetention(request, retention, &argc, argv, argvlen) != 0) {
            AFB_API_ERROR (request->api, "%s: failed to put retention", __func__);
            goto nomem;
        }

    if (uncompressed) {
        redisPutUncompressed(&argc, argv, argvlen);
    }

    if (labelsJ) {
        int ret = redisPutLabels(request, labelsJ, &argc, argv, argvlen);
        if (ret != 0) {
            AFB_API_ERROR (request->api, "%s: failed to put labels %s", __func__, json_object_get_string(labelsJ));
            if (ret == ENOMEM)
                goto nomem;
            goto fail;
        }
    }

    if (redis_send_cmd(request, argc, (const char **)argv, argvlen, NULL, &resstr) != 0) {
        afb_req_fail_f(request, "redis-error", "%s", resstr);
        goto fail;
    }

    afb_req_success(request, NULL, NULL);
    goto done;

nomem:
    afb_req_fail_f(request, "mem-error", "insufficient memory");
fail:
done:

    if (resstr)
        free(resstr);

    argvCleanup(argc, argv, argvlen);

    return;
}

static void redis_add (afb_req_t request) {

    json_object *argsJ = afb_req_json(request);

    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));

    char * rkey;
    uint32_t retention = 0;
    uint32_t uncompressed  = false;
    json_object * labelsJ = NULL;
    char * timestamp;
    double value;

    char ** argv = NULL;
    size_t * argvlen = NULL;
    char * resstr = NULL;

    int err = wrap_json_unpack(argsJ, "{s:s,s?s,s:F,s?i,s?b,s?o !}", 
        "key", &rkey,
        "timestamp", &timestamp,
        "value", &value,
        "retention", &retention,
        "uncompressed", &uncompressed,
        "labels", &labelsJ
        );
    if (err) {
        afb_req_fail_f(request, "parse-error", "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    int argc = 4; /* 1 slot for command name, 1 for the key, 1 for timestemp, and 1 for value */
    
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

    argv = calloc(argc, sizeof(char*));
    if (argv == NULL) {
        goto nomem;
    }

    argvlen = calloc(argc, sizeof(size_t));
    if (argvlen == NULL) {
        goto nomem;
    }

    argc = 0;
    argv[argc] = strdup("TS.ADD");
    if (argv[argc] == NULL)
        goto nomem;
    argvlen[argc] = strlen(argv[argc]);
    argc++;

    argv[argc] = strdup(rkey);
    if (argv[argc] == NULL)
        goto nomem;

    argvlen[argc] = strlen(argv[argc]);
    argc++;

    argv[argc] = strdup(timestamp);
    if (argv[argc] == NULL)
        goto nomem;

    argvlen[argc] = strlen(argv[argc]);
    argc++;

    if (asprintf(&argv[argc], "%f", value) == -1)
        goto nomem;

    argvlen[argc] = strlen(argv[argc]);
    argc++;

    if (retention) 
        if (redisPutRetention(request, retention, &argc, argv, argvlen) != 0) {
            AFB_API_ERROR (request->api, "%s: failed to put retention", __func__);
            goto nomem;
        }

    if (uncompressed) {
        if (redisPutUncompressed(&argc, argv, argvlen) != 0)
            goto nomem;
    }

    if (labelsJ) {
        int ret = redisPutLabels(request, labelsJ, &argc, argv, argvlen);
        if (ret != 0) {
            AFB_API_ERROR (request->api, "%s: failed to put labels %s", __func__, json_object_get_string(labelsJ));
            if (ret == ENOMEM)
                goto nomem;
            goto fail;
        }
    }

    if (redis_send_cmd(request, argc, (const char **)argv, argvlen, NULL, &resstr) != 0) {
        afb_req_fail_f(request, "redis-error", "%s", resstr);
        goto fail;
    }

    afb_req_success(request, NULL, NULL);
    goto done;

nomem:
    afb_req_fail_f(request, "mem-error", "insufficient memory");

fail:

done:
    if (resstr)
        free(resstr);

    argvCleanup(argc, argv, argvlen);
    return;
}

static void redis_range (afb_req_t request) {
    json_object *argsJ = afb_req_json(request);

    char * rkey;
    char * fromtimestamp;
    char * totimestamp;
    uint32_t count = 0;
    json_object * aggregationJ = NULL;
    json_object * replyJ = NULL;

    char ** argv = NULL;
    int argc = 4; // 1 cmd, 1 key, 2 timestamp
    size_t * argvlen = NULL;
    char * resstr;

    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));

    int err = wrap_json_unpack(argsJ, "{s:s,s:s,s:s,s?i,s?o !}", 
        "key", &rkey,
        "fromts", &fromtimestamp,
        "tots", &totimestamp,
        "count", &count,
        "aggregation", &aggregationJ
        );
    if (err) {
        afb_req_fail_f(request, "parse-error", "json error in '%s'", json_object_get_string(argsJ));
        goto fail;
    }

    if (count != 0)
        argc++;

    if (aggregationJ)
        argc++;

    argv = calloc(argc, sizeof(char*));
    if (argv == NULL) {
        goto nomem;
    }

    argvlen = calloc(argc, sizeof(size_t));
    if (argvlen == NULL) {
        goto nomem;
    }

    argc = 0;
    argv[argc] = strdup("TS.RANGE");
    if (argv[argc] == NULL)
        goto nomem;
    argvlen[argc] = strlen(argv[argc]);
    argc++;

    argv[argc] = strdup(rkey);
    if (argv[argc] == NULL)
        goto nomem;

    argvlen[argc] = strlen(argv[argc]);
    argc++;

    argv[argc] = strdup(fromtimestamp);
    if (argv[argc] == NULL)
        goto nomem;

    argvlen[argc] = strlen(argv[argc]);
    argc++;

    argv[argc] = strdup(totimestamp);
    if (argv[argc] == NULL)
        goto nomem;

    argvlen[argc] = strlen(argv[argc]);
    argc++;

    if (count != 0) {
        argv[argc] = strdup("COUNT");
        if (argv[argc] == NULL)
            goto nomem;
        argvlen[argc] = strlen(argv[argc]);
        argc++;

        if (asprintf(&argv[argc], "%d", count) == -1)
            goto nomem;
        argvlen[argc] = strlen(argv[argc]);
        argc++;

    }

    if (aggregationJ) {
        int ret;
        ret = redisPutAggregation(request, aggregationJ, &argc, argv, argvlen);
        if (ret != 0) {
            AFB_API_ERROR (request->api, "%s: failed to put aggregation %s", __func__, json_object_get_string(aggregationJ));
            if (ret == ENOMEM)
                goto nomem;
            goto fail;
        }
    }

    if (redis_send_cmd(request, argc, (const char **)argv, argvlen, &replyJ, &resstr) != 0) {
        afb_req_fail_f(request, "redis-error", "%s", resstr);
        goto fail;
    }

    afb_req_success(request, replyJ, NULL);
    goto done;

nomem:
    afb_req_fail_f(request, "mem-error", "insufficient memory");

fail:
done:

    if (replyJ)
        free(replyJ);

    if (resstr)
        free(resstr);

    argvCleanup(argc, argv, argvlen);
    return;
}

static void redis_alter (afb_req_t request) {
    json_object *argsJ = afb_req_json(request);
    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));
    afb_req_success(request, NULL, NULL);
}


// Every HAL export the same API & Interface Mapping from SndCard to AudioLogic is done through alsaHalSndCardT
static afb_verb_t CtrlApiVerbs[] = {
    /* VERB'S NAME         FUNCTION TO CALL         SHORT DESCRIPTION */
    { .verb = "ping",     .callback = ctrlapi_ping     , .info = "ping test for API"},
    { .verb = "create", .callback = redis_create , .info = "create a timed value in TS" },
	{ .verb = "add", .callback = redis_add , .info = "add a timed value in TS" },
    { .verb = "range", .callback = redis_range , .info = "request a timed value in TS" },
    { .verb = "alter", .callback = redis_alter , .info = "alters a timed value in TS" },
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
    int count, ix;
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

static void initRedis(afb_api_t api, json_object * redisJ) {
	char * hostname;
	uint32_t port;

    AFB_API_NOTICE (api, "%s...", __func__);

	int error = wrap_json_unpack(redisJ, "{ss,si !}"
            , "hostname", &hostname
			, "port", &port
            );

	if (error) {
	    AFB_API_NOTICE (api, "%s failed to get redis params %s", __func__, json_object_get_string(redisJ));
		goto fail;
	}

    AFB_API_NOTICE (api, "%s...to %s/%d", __func__, hostname, port);

    currentRedisContext = redisConnect(hostname, port);
    if (currentRedisContext == NULL) {
    	AFB_API_ERROR(api, "Connection error: can't allocate redis context\n");
    	goto fail;
    }
    if (currentRedisContext->err) {
    	 AFB_API_ERROR(api, "Connection error: %s\n", currentRedisContext->errstr);
    	 redisFree(currentRedisContext);
    	 goto fail;
   }

fail:
	return;
}
