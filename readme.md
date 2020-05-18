* redis binding

afb-client-demo -H ws://localhost:1234/api?token=1 redis create '{ "key":"temperature", "retention":3000, "uncompressed":true, "labels": { "sens":"3", "asa":"44" } }'

afb-client-demo -H ws://localhost:1234/api?token=1 redis add '{ "key":"temperature", "timestamp":1548149191, "value":42 , "retention":3000, "uncompressed":true, "labels": { "sens":"3", "asa":"44" } }'
