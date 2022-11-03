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

#include <stdlib.h>
#include <stdint.h>

#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#include <systemd/sd-event.h>
#include <afb-binding.h>

extern int redisSdAttach(afb_api_t api, redisAsyncContext *ac, sd_event* event);
