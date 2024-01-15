# PDLog - test log service

#### Run locally

```go
go run cmd/server/main.go
```

#### Add new log

```shell
curl -X POST localhost:9099 -d '{"record": {"value": "TESTLOG1"}}'
curl -X POST localhost:9099 -d '{"record": {"value": "TESTLOG2"}}'
curl -X POST localhost:9099 -d '{"record": {"value": "TESTLOG3"}}'
```

### Read logs

```shell
curl -X GET localhost:9099 -d '{"offset": 0}'
curl -X GET localhost:9099 -d '{"offset": 1}'
curl -X GET localhost:9099 -d '{"offset": 2}'
```

#### keywords

write-ahead logs, transaction logs, commit logs

#### Terms

Record - the data stored in out log
Store - the file we store records in
Index - the file we store index entries in
Segment - the abstraction that ties a store and an index together
Log - the abstraction that ties al the segments together
