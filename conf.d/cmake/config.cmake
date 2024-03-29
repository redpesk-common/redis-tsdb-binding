###########################################################################
# Copyright 2015, 2016, 2017, 2021 IoT.bzh
#
# author: Thierry Bultel <thierry.bultel@iot.bzh>
#         Fulup Ar Foll <fulup@iot.bzh>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
###########################################################################

# Project Info
# ------------------
set(PROJECT_NAME redis-tsdb-binding)
set(PROJECT_VERSION 1.0)
set(PROJECT_PRETTY_NAME "Timeseries DB Binding of the Redis Plugin")
set(PROJECT_DESCRIPTION "Binding of the redis plugin: Redis Time Series database")
set(PROJECT_URL "https://github.com/redpesk-common/redis-tsdb-binding")
set(PROJECT_ICON "icon.png")
set(PROJECT_AUTHOR "Thierry Bultel")
set(PROJECT_AUTHOR_MAIL "thierry.bultel@iot.bzh")
set(PROJECT_LICENSE "APL2.0")
set(PROJECT_LANGUAGES "CXX")
set(API_NAME "redis")

# Where are stored the project configuration files
# relative to the root project directory
set(PROJECT_CMAKE_CONF_DIR "conf.d")

# Where are stored your external libraries for your project. This is 3rd party library that you don't maintain
# but used and must be built and linked.
# set(PROJECT_LIBDIR "libs")

# Where are stored data for your application. Pictures, static resources must be placed in that folder.
# set(PROJECT_RESOURCES "data")

# Which directories inspect to find CMakeLists.txt target files
# set(PROJECT_SRC_DIR_PATTERN "*")

# Compilation Mode (DEBUG, RELEASE)
# ----------------------------------
set(BUILD_TYPE "RELEASE")
set(USE_EFENCE 0)

# Kernel selection if needed. You can choose between a
# mandatory version to impose a minimal version.
# Or check Kernel minimal version and just print a Warning
# about missing features and define a preprocessor variable
# to be used as preprocessor condition in code to disable
# incompatibles features. Preprocessor define is named
# KERNEL_MINIMAL_VERSION_OK.
#
# NOTE*** FOR NOW IT CHECKS KERNEL Yocto environment and
# Yocto SDK Kernel version.
# -----------------------------------------------
#set (kernel_mandatory_version 4.8)
#set (kernel_minimal_version 4.8)

# Compiler selection if needed. Impose a minimal version.
# -----------------------------------------------
set (gcc_minimal_version 4.9)

# PKG_CONFIG required packages
# -----------------------------
set (PKG_REQUIRED_LIST
	json-c
	libsystemd>=222
	afb-binding
	uuid
	liburcu
	afb-helpers
	afb-libcontroller
	hiredis
	jsoncpp
	cmake_afb_templates>=10.0.4
)

# Prefix path where will be installed the files
# Default: /usr/local (need root permission to write in)
# ------------------------------------------------------
#set(CMAKE_INSTALL_PREFIX $ENV{HOME}/opt)

# Customize link option
# -----------------------------
#list(APPEND link_libraries -an-option)

set(COMPILE_OPTIONS
	-Werror
	-Wall)

# Compilation options definition
# Use CMake generator expressions to specify only for a specific language
# Values are prefilled with default options that is currently used.
# Either separate options with ";", or each options must be quoted separately
# DO NOT PUT ALL OPTION QUOTED AT ONCE , COMPILATION COULD FAILED !
# ----------------------------------------------------------------------------
#set(COMPILE_OPTIONS
# -Wall
# -Wextra
# -Wconversion
# -Wno-unused-parameter
# -Wno-sign-compare
# -Wno-sign-conversion
# -Werror=maybe-uninitialized
# -Werror=implicit-function-declaration
# -ffunction-sections
# -fdata-sections
# -fPIC
# CACHE STRING "Compilation flags")
#set(C_COMPILE_OPTIONS "" CACHE STRING "Compilation flags for C language.")
#set(CXX_COMPILE_OPTIONS "" CACHE STRING "Compilation flags for C++ language.")
#set(PROFILING_COMPILE_OPTIONS
# -g
# -O0
# -pg
# -Wp,-U_FORTIFY_SOURCE
# CACHE STRING "Compilation flags for PROFILING build type.")
#set(DEBUG_COMPILE_OPTIONS
# -g
# -O0
# -ggdb
# CACHE STRING "Compilation flags for DEBUG build type.")
#set(COVERAGE_COMPILE_OPTIONS
# -g
# -O0
# --coverage
# CACHE STRING "Compilation flags for COVERAGE build type.")
#set(RELEASE_COMPILE_OPTIONS
# -g
# -O2
# CACHE STRING "Compilation flags for RELEASE build type.")

set(CONTROL_SUPPORT_LUA 1)
add_definitions(-DCONTROL_PLUGIN_PATH="${CMAKE_BINARY_DIR}/package/lib/plugins:${CMAKE_INSTALL_PREFIX}/${PROJECT_NAME}/lib/plugins:${CMAKE_INSTALL_PREFIX}/${PROJECT_NAME}")
add_definitions(-DCONTROL_CONFIG_PATH="${CMAKE_SOURCE_DIR}/conf.d/project/etc:${CMAKE_BINARY_DIR}/package/etc:${CMAKE_INSTALL_PREFIX}/${PROJECT_NAME}/etc:${CMAKE_INSTALL_PREFIX}/${PROJECT_NAME}")
add_definitions(-DCONTROL_LUA_PATH="${CMAKE_SOURCE_DIR}/conf.d/project/lua.d:${CMAKE_INSTALL_PREFIX}/${PROJECT_NAME}/var:${CMAKE_INSTALL_PREFIX}/${PROJECT_NAME}")
add_definitions(-DCTL_PLUGIN_MAGIC=987456123)
add_definitions(-DAFB_BINDING_VERSION=3)


# (BUG!!!) as PKG_CONFIG_PATH does not work [should be an env variable]
# ---------------------------------------------------------------------
set(CMAKE_PREFIX_PATH ${CMAKE_INSTALL_PREFIX}/lib64/pkgconfig ${CMAKE_INSTALL_PREFIX}/lib/pkgconfig)
set(LD_LIBRARY_PATH ${CMAKE_INSTALL_PREFIX}/lib64 ${CMAKE_INSTALL_PREFIX}/lib)

# Optional location for config.xml.in
# -----------------------------------
#set(WIDGET_ICON conf.d/wgt/${PROJECT_ICON} CACHE PATH "Path to the widget icon")
set(WIDGET_CONFIG_TEMPLATE "${CMAKE_SOURCE_DIR}/conf.d/wgt/config.xml.in" CACHE PATH "Path to widget config file template (config.xml.in)")

# Mandatory widget Mimetype specification of the main unit
# --------------------------------------------------------------------------
# Choose between :
#- text/html : HTML application,
#	content.src designates the home page of the application
#
#- application/vnd.agl.native : AGL compatible native,
#	content.src designates the relative path of the binary.
#
# - application/vnd.agl.service: AGL service, content.src is not used.
#
#- ***application/x-executable***: Native application,
#	content.src designates the relative path of the binary.
#	For such application, only security setup is made.
#
set(WIDGET_TYPE application/vnd.agl.service)

# Mandatory Widget entry point file of the main unit
# --------------------------------------------------------------
# This is the file that will be executed, loaded,
# at launch time by the application framework.
#
set(WIDGET_ENTRY_POINT lib/redis-binding.so)

# Optional dependencies order
# ---------------------------
#set(EXTRA_DEPENDENCIES_ORDER)

# Optional Extra global include path
# -----------------------------------
#set(EXTRA_INCLUDE_DIRS)

# Optional extra libraries
# -------------------------
#set(EXTRA_LINK_LIBRARIES)

# Optional force binding Linking flag
# ------------------------------------
# set(BINDINGS_LINK_FLAG LinkOptions )

# Optional force package prefix generation, like widget
# -----------------------------------------------------
# set(PKG_PREFIX DestinationPath)

# Optional Application Framework security token
# and port use for remote debugging.
#------------------------------------------------------------
set(AFB_TOKEN   ""     CACHE PATH "Default binder security token")
set(AFB_REMPORT "1234" CACHE PATH "Default binder listening port")

# Print a helper message when every thing is finished
# ----------------------------------------------------
set(CLOSING_MESSAGE "Typical binding launch: afb-binder --name afbd-redis-tsdb-binding --port=${AFB_REMPORT} --workdir=${CMAKE_BINARY_DIR} --binding=package/lib/redis-binding.so --roothttp=package/htdocs --tracereq=common -vvv")
set(PACKAGE_MESSAGE "Install widget file using in the target : afm-util install ${PROJECT_NAME}.wgt")

# Optional schema validator about now only XML, LUA and JSON
# are supported
#------------------------------------------------------------
#set(LUA_CHECKER "luac" "-p" CACHE STRING "LUA compiler")
#set(XML_CHECKER "xmllint" CACHE STRING "XML linter")
#set(JSON_CHECKER "json_verify" CACHE STRING "JSON linter")

# This include is mandatory and MUST happens at the end
# of this file, else you expose you to unexpected behavior
#
# This CMake module could be found at the following url:
# https://gerrit.automotivelinux.org/gerrit/#/admin/projects/src/cmake-apps-module
# -----------------------------------------------------------
include(CMakeAfbTemplates)
