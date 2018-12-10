## Module overview

This module provides the functionality required to access and manipulate data stored in an Cassandra datasource.

### Client

To access a Cassandra datasource, you must first create a `client` object. Create a client object of the cassandra client type (i.e., `cassandra:Client`) and provide the necessary connection parameters. This will create a pool of connections to the given Cassandra database. A sample for creating a client with a Cassandra client can be found below.

### Database operations

Once the client is created, database operations can be executed through that client. This module provides support for updating data/schema and select data.

## Samples

### Creating a Client
```ballerina
cassandra:Client conn = new({
    host: "localhost",
    port: 9042,
    username: "cassandra",
    password: "cassandra",
    options: {
        queryOptionsConfig: { consistencyLevel: "ONE", defaultIdempotence: false },
        protocolOptionsConfig: { sslEnabled: false },
        socketOptionsConfig: { connectTimeoutMillis: 500, readTimeoutMillis: 1000 },
        poolingOptionsConfig: { maxConnectionsPerHostLocal: 5, newConnectionThresholdLocal: 10 } }
});
```
For the full list of available configuration options refer the API docs of the client.

### Update data

```ballerina

var returned = conn->update("CREATE TABLE testballerina.person(id int PRIMARY KEY,name text,salary float,income double,
                      married boolean)");
if (returned is ()) {
    io:println("Table creation success ");
} else {
    io:println("Table creation failed: " + returned.reason());
}
```

### Select data

```ballerina

var selectRet = conn->select("select id, name, salary from testballerina.person where salary = ? ALLOW FILTERING",
                                    Person, pSalary);
if (selectRet is table) {
    table dt = selectRet;
    // Processing logic
} else {
    io:println("Select data from person table failed: " + selectRet.reason());
}
```