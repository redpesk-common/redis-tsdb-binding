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

#include "deflatten.h"


#include <cstring>
#include <iostream>
#include <sstream>

using namespace std;



/* converts a flattened name (something like 'foo.bar[3].dum') into a json structure 
--- > 
        { "bar": [ ?, ?, "dum": ? ] } 

*/

void deflatten(Json::Value & root, char *path, const Json::Value & value) {

    char * ptr = path;
    int ret;

    Json::Value* current = &root;

    for (ptr = path;; ptr = NULL) {
        char * token = strtok(ptr, ".]");

        if (token == NULL)
            break;

        bool is_array_elem = false;
        int32_t _index = -1;

        char *openb = strstr(token, "[");
        if (openb)
        {
            ret = sscanf(openb, "[%d", &_index);
            if (ret == 1)
                is_array_elem = true;
        }

        char* base = token;
        if (is_array_elem)
            base [openb - base] = '\0';

        if (is_array_elem) {

            if (strlen(base) != 0) {

                // create the array if it does not exist yet
                if ((*current)[base].type() == Json::nullValue) {
                    (*current)[base] = Json::Value(Json::arrayValue);

                }

                current = &(*current)[base];
            }

            if ((*current)[_index] == Json::nullValue) {
                (*current)[_index] = Json::Value();
            }

            current = &(*current)[_index];

        } else {

            if ((*current)[base].type() == Json::nullValue) {
                (*current)[base] = Json::Value();
            } 

            current = &(*current)[base];
            
        }

    };

    (*current) = value;

}
