# redpesk® redis binding

This part is only useful if you plan to build and install the package from source.  


## 1. Using a distribution package manager

If you aren't planing to build it from source, add the redpesk® repository
to your package manager.  
Here is the url for Redpesk and Fedora:
`download.redpesk.bzh`  
Then, to install the package and all its dependencies, install the package **redis-tsdb-binding**

## 2. Building from source

We advise you to use the [local builder]({% chapter_link local-builder-doc.installation %}) for building the binding sources. The local builder comes with everything setup to build redpesk® projects.

### a. Tools

Install the building tools:
- gcc
- g++
- make
- cmake
- afb-cmake-modules

Install the dependencies:
- json-c
- afb-binding
- afb-libhelpers
- afb-libcontroller
- libhiredis

Fedora/OpenSuse:
```bash
dnf install gcc-c++ make cmake afb-cmake-modules json-c-devel afb-binding-devel afb-libhelpers-devel afb-libcontroller-devel libhiredis-devel
```

Ubuntu:
```bash
apt install gcc g++ make cmake afb-cmake-modules-bin libsystemd-dev libjson-c-dev afb-binding-dev afb-libhelpers-dev afb-libcontroller-dev libhiredis-dev
```

### b. Build

```bash
git clone https://github.com/redpesk/redpesk-common/redis-tsdb-binding.git
cd redis-tsdb-binding
mkdir build
cd build
cmake ..
make
make install
```

From then on, you have set up your environment to run the redis binding. Go to the [usage](./4-Usage.html) section to see how to use it.
