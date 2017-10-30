package ballerina.data.cassandra;

@Description { value:"Cassandra client connector."}
@Param { value:"host: Cassandra host name" }
@Param { value:"properties: Optional properties for cassandra connection" }
public connector ClientConnector (string host, map properties) {

    map sharedMap = {};

    @Description {value:"Execute query action implementation of cassandra connector"}
    @Param {value:"query: Query to be executed"}
    native action execute (string query) (datatable);
}
