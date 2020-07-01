* redis binding

* cmd create test
afb-client-demo -H ws://localhost:1234/api?token=1 redis create '{ "key":"temperature", "retention":3000, "uncompressed":true, "labels": { "sens":"3", "asa":"44" } }'

* cmd add test

afb-client-demo -H ws://localhost:1234/api?token=1 redis add '{ "key":"temperature", "timestamp":"1548149191", "value":42 , "retention":3000, "uncompressed":true, "labels": { "sens":"3", "asa":"44" } }'

afb-client-demo -H ws://localhost:1234/api?token=1 redis add '{ "key":"temperature", "timestamp":"*", "value":42 , "retention":3000, "uncompressed":true, "labels": { "sens":"3", "asa":"44" } }'


* cmd range test
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


