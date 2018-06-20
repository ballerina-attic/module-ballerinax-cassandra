## Package overview

This package provides the functionality required to access and manipulate data stored in an Cassandra datasource. 

### Endpoint 

To access a Cassandra datasource, you must first create an `endpoint`, which is a virtual representation of the physical endpoint of the Cassandra database that you are trying to connect to. Create an endpoint of the cassandra client type (i.e., `cassandra:Client`) and provide the necessary connection parameters. This will create a pool of connections to the given Cassandra database. A sample for creating an endpoint with a Cassandra client can be found below. 

### Database operations

Once the endpoint is created, database operations can be executed through that endpoint. This package provides support for updating data/schema and select data. 

## Samples

### Creating an endpoint
```ballerina
endpoint c:Client conn {
    host: "localhost",
    port: 9042,
    username: "cassandra",
    password: "cassandra",
    options: {
        queryOptionsConfig: { consistencyLevel: "ONE", defaultIdempotence: false },
        protocolOptionsConfig: { sslEnabled: false },
        socketOptionsConfig: { connectTimeoutMillis: 500, readTimeoutMillis: 1000 },
        poolingOptionsConfig: { maxConnectionsPerHostLocal: 5, newConnectionThresholdLocal: 10 } }
};
```
For the full list of available configuration options refer the API docs of the endpoint.

### Update data

```ballerina

var returned = conn->update("CREATE TABLE testballerina.person(id int PRIMARY KEY,name text,salary float,income double,
                      married boolean)");
match returned {
  () => io:println("Table creation success ");
  error e => io:println("Table creation failed: " + e.message);
}
```

### Select data

```ballerina

table dt;
var selectRet = conn->select("select id, name, salary from testballerina.person where salary = ? ALLOW FILTERING",
                                    Person, pSalary);
match selectRet {
    table tableReturned => dt = tableReturned;
    error e => io:println("Select data from person table failed: " + e.message);
}
```