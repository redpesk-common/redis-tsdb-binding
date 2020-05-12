
#define _GNU_SOURCE
#include <stdio.h>
#include <string.h>
#include <time.h>

#include <wrap-json.h>
#include <ctl-config.h>

#include <hiredis.h>
#include <time.h>
#include <stdbool.h>

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

    argv[*argc] = strdup("RETENTION");
    argvlen[*argc] = strlen(argv[*argc]);
    (*argc)++;
    asprintf(&argv[*argc], "%d", retention);
    argvlen[*argc] = strlen(argv[*argc]);
    (*argc)++;

    return 0;
}


static int redisPutLabels(afb_req_t request, json_object * labelsJ, int * argc, char ** argv, size_t * argvlen) {
    int ret = -1;
    enum json_type type;

    argv[*argc] = strdup("LABELS");
    argvlen[*argc] = strlen(argv[*argc]);
    (*argc)++;

    json_object_object_foreach(labelsJ, key, val) {
        type = json_object_get_type(val);
        switch (type) {
        case json_type_string: 
            argv[*argc] = strdup(key);
            argvlen[*argc] = strlen(argv[*argc]);
            (*argc)++;
            argv[*argc] = strdup(json_object_get_string(val));
            argvlen[*argc] = strlen(argv[*argc]);
            (*argc)++;
            break;

        case json_type_int: {
            argv[*argc] = strdup(key);
            argvlen[*argc] = strlen(argv[*argc]);
            (*argc)++;
            asprintf(&argv[*argc], "%d", json_object_get_int(val));
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
fail:    
    return ret;

}

static int redis_send_cmd(afb_req_t request, int argc, const char ** argv, const size_t * argvlen) {
    int ret = -1;
    
    redisReply * rep = redisCommandArgv(currentRedisContext, argc, argv, argvlen);
    if (rep == NULL) {
        afb_req_fail_f(request, "redis-error", "redis command failed");
    	goto fail;
    }

    if (rep->type == REDIS_REPLY_ERROR) {
        afb_req_fail_f(request, "redis-error", "redis command error: %s failed", rep->str);
    	goto fail;
    }

    AFB_API_DEBUG(request->api, "Redis Command reply: %s", rep->str);
    ret = 0;
fail:    
    return ret;
}


static void redis_create (afb_req_t request) {
    json_object *argsJ = afb_req_json(request);
    char * rkey = NULL;
    int32_t retention = 0;
    uint32_t uncompressed = false;
    json_object * labelsJ = NULL;

    char ** argv = NULL;
    size_t * argvlen = NULL;

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
    if (argv == 0) {
        afb_req_fail_f(request, "mem-error", "insufficient memory");
        goto fail;
    }

    argvlen = calloc(argc, sizeof(size_t));
    if (argvlen == 0) {
        afb_req_fail_f(request, "mem-error", "insufficient memory");
        goto fail;
    }

    argc = 0;
    argv[argc++] = strdup("TS.CREATE");
    argv[argc++] = strdup(rkey);

    if (retention) 
        if (redisPutRetention(request, retention, &argc, argv, argvlen) != 0) {
            AFB_API_ERROR (request->api, "%s: failed to put retention", __func__);
            goto fail;
        }

    if (uncompressed) {
        argv[argc++] = strdup("UNCOMPRESSED");
    }

    if (labelsJ) {
        if (redisPutLabels(request, labelsJ, &argc, argv, argvlen) != 0) {
            AFB_API_ERROR (request->api, "%s: failed to put labels %s", __func__, json_object_get_string(labelsJ));
            goto fail;
        }
    }

    if (redis_send_cmd(request, argc, (const char **)argv, 0) != 0)
        goto fail;

    afb_req_success(request, NULL, NULL);
    return;

fail:
    if (argv)
        free(argv);
    if (argvlen)
        free(argvlen);
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
    if (argv == 0) {
        afb_req_fail_f(request, "mem-error", "insufficient memory");
        goto fail;
    }

    argvlen = calloc(argc, sizeof(size_t));
    if (argvlen == 0) {
        afb_req_fail_f(request, "mem-error", "insufficient memory");
        goto fail;
    }

    argc = 0;
    argv[argc] = strdup("TS.ADD");
    argvlen[argc] = strlen(argv[argc]);
    argc++;
    argv[argc] = strdup(rkey);
    argvlen[argc] = strlen(argv[argc]);
    argc++;
    argv[argc] = strdup(timestamp);
    argvlen[argc] = strlen(argv[argc]);
    argc++;
    asprintf(&argv[argc], "%f", value);
    argvlen[argc] = strlen(argv[argc]);
    argc++;

    if (retention) 
        if (redisPutRetention(request, retention, &argc, argv, argvlen) != 0) {
            AFB_API_ERROR (request->api, "%s: failed to put retention", __func__);
            goto fail;
        }

    if (uncompressed) {
        argv[argc++] = strdup("UNCOMPRESSED");
    }

    if (labelsJ) {
        if (redisPutLabels(request, labelsJ, &argc, argv, argvlen) != 0) {
            AFB_API_ERROR (request->api, "%s: failed to put labels %s", __func__, json_object_get_string(labelsJ));
            goto fail;
        }
    }

    if (redis_send_cmd(request, argc, (const char **)argv, 0) != 0)
        goto fail;

    afb_req_success(request, NULL, NULL);
    return;
fail:
    if (argv)
        free(argv);
    if (argvlen)
        free(argvlen);

    return;
}

static void redis_range (afb_req_t request) {
    json_object *argsJ = afb_req_json(request);
    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));
    afb_req_success(request, NULL, NULL);
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
