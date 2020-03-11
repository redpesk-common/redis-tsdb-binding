
#define _GNU_SOURCE
#include <stdio.h>
#include <string.h>
#include <time.h>

#include <wrap-json.h>
#include <ctl-config.h>

#include <hiredis.h>

// default api to print log when apihandle not available
afb_api_t AFB_default;

// Config Section definition (note: controls section index should match handle retrieval in HalConfigExec)
static CtlSectionT ctrlSections[]= {
    {.key="resources" , .loadCB= PluginConfig},
    {.key="onload"  , .loadCB= OnloadConfig},
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

// Every HAL export the same API & Interface Mapping from SndCard to AudioLogic is done through alsaHalSndCardT
static afb_verb_t CtrlApiVerbs[] = {
    /* VERB'S NAME         FUNCTION TO CALL         SHORT DESCRIPTION */
    { .verb = "ping",     .callback = ctrlapi_ping     , .info = "ping test for API"},
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

    AFB_API_NOTICE (apiHandle, "Controller in afbBindingEntry");

    const char *dirList = getenv("CONTROL_CONFIG_PATH");
    if (!dirList) dirList=CONTROL_CONFIG_PATH;

    // Select correct config file
    char *configPath = CtlConfigSearch(apiHandle, dirList, "redis");

    if (!configPath) {
        AFB_API_ERROR(apiHandle, "CtlPreInit: No redis-%s-* config found in %s ", GetBinderName(), dirList);
        goto OnErrorExit;
    }

    // load config file and create API
    CtlConfigT *ctrlConfig = CtlLoadMetaData (apiHandle, configPath);
    if (!ctrlConfig) {
        AFB_API_ERROR(apiHandle, "CtrlBindingDyn No valid control config file in:\n-- %s", configPath);
        goto OnErrorExit;
    }

    if (!ctrlConfig->api) {
        AFB_API_ERROR(apiHandle, "CtrlBindingDyn API Missing from metadata in:\n-- %s", configPath);
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

