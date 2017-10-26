package ballerina.data.cassandra;

import ballerina.doc;

@doc:Description { value:"Cassandra client connector."}
@doc:Param { value:"host: Parameter array used with the SQL query" }
@doc:Param { value:"properties: Optional properties for cassandra connection" }
public connector ClientConnector (string host, map properties) {

    map sharedMap = {};

    @doc:Description {value:"Execute query action implementation of cassandra connector"}
    @doc:Param {value:"query: Query to be executed"}
    native action execute (string query) (datatable);
}
