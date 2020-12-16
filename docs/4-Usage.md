# redpesk® demo-n2k plugin usage

When used as a Redpesk RPM package, the service autostarts and is ready to reply to requests from other bindings

When built from source, this is a typical command line to launch it:

```bash
afb-binder --name afbd-redis-tsdb-binding --port=1234 --workdir=$HOME/rp-service-hiredis/build --ldpath=/dev/null --binding=package/lib/redis-binding.so -v
```