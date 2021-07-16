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

#include "redis-systemd.h"

#include <string.h>

typedef struct redisSdEvents {
    redisAsyncContext* context;
    sd_event_source*   source;
    uint32_t           events;
} redisSdEvents;


static int redisSdIoHandler(sd_event_source *s, int fd, uint32_t revents, void *userdata) {
    redisSdEvents* p = (redisSdEvents*)userdata;

    if (p->context != NULL && (revents & EPOLLIN)) {
        redisAsyncHandleRead(p->context);
    }
    if (p->context != NULL && (revents & EPOLLOUT)) {
        redisAsyncHandleWrite(p->context);
    }
  return 0;
}


static void redisSdAddRead(void *privdata) {
    redisSdEvents* p = (redisSdEvents*)privdata;

    p->events |= EPOLLIN;
    sd_event_source_set_io_events(p->source, p->events);
}


static void redisSdDelRead(void *privdata) {
    redisSdEvents* p = (redisSdEvents*)privdata;

    p->events &= ~EPOLLIN;
    sd_event_source_set_io_events(p->source, p->events);
}


static void redisSdAddWrite(void *privdata) {
    redisSdEvents* p = (redisSdEvents*)privdata;

    p->events |= EPOLLOUT;
    sd_event_source_set_io_events(p->source, p->events);
}


static void redisSdDelWrite(void *privdata) {
    redisSdEvents* p = (redisSdEvents*)privdata;

    p->events &= ~EPOLLOUT;
    sd_event_source_set_io_events(p->source, p->events);
}

static void redisSdCleanup(void *privdata) {

    redisSdEvents* p = (redisSdEvents*)privdata;

    p->context->ev.data = NULL;
    sd_event_source_unref(p->source);
    free(p);
}


int redisSdAttach(afb_api_t api, redisAsyncContext* ac, sd_event* event) {

  if (ac->ev.data != NULL) {
    return REDIS_ERR;
  }

  ac->ev.addRead  = redisSdAddRead;
  ac->ev.delRead  = redisSdDelRead;
  ac->ev.addWrite = redisSdAddWrite;
  ac->ev.delWrite = redisSdDelWrite;
  ac->ev.cleanup  = redisSdCleanup;
  ac->data        = api;

  redisSdEvents* p = (redisSdEvents*)malloc(sizeof(*p));
  if (p == NULL)
      return REDIS_ERR;

  memset(p, 0, sizeof(*p));

  ac->ev.data    = p;
  p->context     = ac;
  if (sd_event_add_io(event, &p->source, ac->c.fd, 0, redisSdIoHandler, p) != 0) {
    ac->ev.data = NULL;
    free(p);
    return REDIS_ERR;
  }

  return REDIS_OK;
}
