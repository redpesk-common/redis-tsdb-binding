
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

#define REDIS_CMD_MAX_LEN 512

static void appendToRedisCmd(char * s, const char * item) {
    strncat(s, item, REDIS_CMD_MAX_LEN - strlen(s) -1);
}

#define REMAIN(m, s) (m - strlen(s) - 1)

static void redis_create (afb_req_t request) {
    json_object *argsJ = afb_req_json(request);
    char * rkey;

    uint32_t retention = 0;
    json_object * labelsJ = NULL;
    bool uncompressed  = false;

    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));

    int err = wrap_json_unpack(argsJ, "{s:s,s?i,s?b,s?o !}", 
        "key", &rkey,
        "retention", &retention,
        "uncompressed", &uncompressed,
        "labels", &labelsJ
        );
    if (err) {
        afb_req_fail_f(request, "parse-error", "json error in %s)", json_object_get_string(argsJ));
        goto fail;
    }

    char redisCommandS[REDIS_CMD_MAX_LEN];
    redisCommandS[0] = '\0';
    appendToRedisCmd(redisCommandS, rkey);

    if (retention) {
        char retentionS[16]; /* is enough to print 2^32 */
        sprintf(retentionS, "%d", retention);
        appendToRedisCmd(redisCommandS, " RETENTION ");
        appendToRedisCmd(redisCommandS, retentionS);
    }

    if (uncompressed) {
        appendToRedisCmd(redisCommandS, " UNCOMPRESSED ");
    }

    if (labelsJ) {
        enum json_type type;

        appendToRedisCmd(redisCommandS, " LABELS ");

        json_object_object_foreach(labelsJ, key, val) {
            type = json_object_get_type(val);
            switch (type) {
            case json_type_string: {
                appendToRedisCmd(redisCommandS, key);
                appendToRedisCmd(redisCommandS, " ");
                appendToRedisCmd(redisCommandS, json_object_get_string(val));
                appendToRedisCmd(redisCommandS, " ");
                break;
            }
            
            default:
                afb_req_fail_f(request, "invalid-syntax", "labels must be a string (given:%s)", json_object_get_string(val));
                goto fail;
                break;
            }
        }

    }

    AFB_API_DEBUG(request->api, "Redis create: %s", redisCommandS);

    redisReply * rep = redisCommand(currentRedisContext, "TS.CREATE %s", redisCommandS);
    if (rep == NULL) {
        afb_req_fail_f(request, "redis-error", "redis command failed");
    	goto fail;
    }

    if (rep->type == REDIS_REPLY_ERROR) {
        afb_req_fail_f(request, "redis-error", "redis command error: %s failed", rep->str);
    	goto fail;
    }

    AFB_API_DEBUG(request->api, "Redis Command reply: %s", rep->str);

    afb_req_success(request, NULL, NULL);
    return;

fail:
    return;
}

static void redis_add (afb_req_t request) {
    json_object *argsJ = afb_req_json(request);
    AFB_API_DEBUG (request->api, "%s: %s", __func__, json_object_get_string(argsJ));
    afb_req_success(request, NULL, NULL);
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
