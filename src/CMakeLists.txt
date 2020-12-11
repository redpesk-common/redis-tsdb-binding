#
# Copyright (C) 2020 "IoT.bzh"
# Author : Thierry Bultel <thierry.bultel@iot.bzh>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# Add target to project dependency list
PROJECT_TARGET_ADD(redis-binding)

file(GLOB SOURCE_FILES "*.c")

set( JSON_INFO_C ${CMAKE_CURRENT_BINARY_DIR}/json_info.c)

# Define project Targets
ADD_LIBRARY(${TARGET_NAME} MODULE ${SOURCE_FILES} ${JSON_INFO_C})

SET_TARGET_PROPERTIES(${TARGET_NAME} PROPERTIES
PREFIX ""
    LABELS "BINDING"
    OUTPUT_NAME ${TARGET_NAME}
)

# Define target includes for this target client
TARGET_INCLUDE_DIRECTORIES(${TARGET_NAME}
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR}
)
# make sure config is copied before starting
add_dependencies(${TARGET_NAME} redis-config)

add_custom_command(
    OUTPUT json_info.c
    COMMAND echo 'const char * info_verbS=\"\\' > ${JSON_INFO_C}
    COMMAND cat ${CMAKE_CURRENT_SOURCE_DIR}/info_verb.json | sed -e 's/$$/\\\\/' -e 's/\"/\\\\\"/g' >> ${JSON_INFO_C}
    COMMAND echo '\\n\"\;' >> ${JSON_INFO_C}
    DEPENDS info_verb.json
)