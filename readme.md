* redis binding

* create
afb-client-demo -H ws://localhost:1234/api?token=1 redis create '{ "key":"temperature", "retention":3000, "uncompressed":true, "labels": { "sens":"3", "asa":"44" } }'

* add
afb-client-demo -H ws://localhost:1234/api?token=1 redis add '{ "key":"temperature", "timestamp":"1548149191", "value":42 , "retention":3000, "uncompressed":true, "labels": { "sens":"3", "asa":"44" } }'

afb-client-demo -H ws://localhost:1234/api?token=1 redis add '{ "key":"temperature", "timestamp":"*", "value":42 , "retention":3000, "uncompressed":true, "labels": { "sens":"3", "asa":"44" } }'


* range
afb-client-demo -H ws://localhost:1234/api?token=1 redis range '{ "key":"temperature", "fromts":"1548149191", "tots":"1548149200" }'

* alter
afb-client-demo -H ws://localhost:1234/api?token=1 redis alter '{ "key":"temperature", "retention":3000,  "labels": { "sens":"3", "asa":"45" } }'

* madd

afb-client-demo -H ws://localhost:1234/api?token=1 redis madd '{ "key":"temperature", "timestamp":"15481491091" , "value":32 }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis madd '[{ "key":"temperature", "timestamp":"15481491091" , "value":32 }]'

afb-client-demo -H ws://localhost:1234/api?token=1 redis madd '[{ "key":"temperature", "timestamp":"*" , "value":32 }, { "key":"temperature1", "timestamp":"*" , "value":29 }]'


* incrby

afb-client-demo -H ws://localhost:1234/api?token=1 redis incrby '{ "key":"temperature", "value":1, "timestamp":"*" }'

afb-client-demo -H ws://localhost:1234/api?token=1 redis incrby '{ "key":"temperature", "value":2 , "timestamp":"*", "retention":2000 }'

afb-client-demo -H ws://localhost:1234/api?token=1 redis incrby '{ "key":"temperature", "value":2.2, "timestamp":"*", "uncompressed":true }'

* decrby


afb-client-demo -H ws://localhost:1234/api?token=1 redis decrby '{ "key":"temperature", "value":1, "timestamp":"*" }'

afb-client-demo -H ws://localhost:1234/api?token=1 redis decrby '{ "key":"temperature", "value":2 , "timestamp":"*", "retention":2000 }'

afb-client-demo -H ws://localhost:1234/api?token=1 redis decrby '{ "key":"temperature", "value":2.2, "timestamp":"*", "uncompressed":true }'

* create/deleterule

afb-client-demo -H ws://localhost:1234/api?token=1 redis create '{ "key":"temp1" }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis create_rule '{ "sourceKey":"temperature", "destKey":"temp1", "aggregation": {"type": "avg", "bucket":500} }'

afb-client-demo -H ws://localhost:1234/api?token=1 redis delete_rule '{ "sourceKey":"temperature", "destKey":"temp1" }

* mrange/mrevrange
afb-client-demo -H ws://localhost:1234/api?token=1 redis mrange '{ "fromts":"1548149191", "tots":"1548149200" , "filter": [ "sens=3" ] }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis mrange '{ "fromts":"-", "tots":"+" , "filter": [ "sens=3" ] }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis mrange '{ "fromts":"-", "tots":"+" , "filter": [ "sens=3" ], "count":3 }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis mrange '{ "fromts":"1548149191", "tots":"1548149200", "withlabels":true, "filter": [ "sens=3" ] }'

* get/mget
afb-client-demo -H ws://localhost:1234/api?token=1 redis get '{ "key":"temp1" }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis mget '{ "filter": [ "sens=3" ] }'

* info/query index
afb-client-demo -H ws://localhost:1234/api?token=1 redis info '{ "key":"temperature" }'
afb-client-demo -H ws://localhost:1234/api?token=1 redis queryindex '{ "filter": [ "sens"=3 ] }'

