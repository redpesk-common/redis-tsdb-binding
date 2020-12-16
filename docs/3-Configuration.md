# redpesk® redis binding configuration

You can find below the configuration obtained after compiling the redis binding and a brief descritpion of several concept introduced.

```json
{
    "$schema": "http://iot.bzh/download/public/schema/json/ctl-schema.json",
    "metadata": {
        "uid": "Redis Binding",
        "version": "1.0",
        "api": "redis",
        "info": "Redis Client binding"
    },
    "onload": [ 
    {
      "redis": 
      {
      "hostname":"127.0.0.1",
      "port": 6379
      }
    }
    ]
}
```

## 1. Metadata

```json
"metadata": {
    "metadata": {
        "uid": "Redis Binding",
        "version": "1.0",
        "api": "redis",
        "info": "Redis Client binding"
    },

```
The metadata is the first block of the json configuration. It gathers basic statements regarding the binding.
It defines the version and the API name exposed by the binding.

## 2. redis specifics

```json
{
  "hostname":"127.0.0.1",
  "port": 6379
}
```

In this section, we define the hostname and port number where the redis server runs.
