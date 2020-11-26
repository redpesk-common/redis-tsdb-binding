#pragma once

#include <stdlib.h>
#include <stdint.h>

#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#include <systemd/sd-event.h>
#include <ctl-config.h>
#include <wrap-json.h>

extern int redisSdAttach(afb_api_t api, redisAsyncContext *ac, sd_event* event);
