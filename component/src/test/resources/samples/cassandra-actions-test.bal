import ballerina/cassandra as c;

struct RS {
    int id;
    string name;
    float salary;
    boolean married;
}

function testKeySpaceCreation() {
    endpoint c:Client conn {
       host: "localhost",
       port: 9142,
       username: "cassandra",
       password: "cassandra",
       options: {}
    };
    _ = conn -> update("CREATE KEYSPACE dummyks  WITH replication = {'class':'SimpleStrategy', 'replication_factor'
    :1}",
    null);
    _ = conn -> close();
}

function testDuplicateKeySpaceCreation() returns (any) {
    endpoint c:Client conn {
        host: "localhost",
        port: 9142,
        username: "cassandra",
        password: "cassandra",
        options: {}
    };
    _ = conn -> update("CREATE KEYSPACE duplicatekstest  WITH replication = {'class':'SimpleStrategy',
    'replication_factor'
    :1}",
                       null);

    var result = conn -> update("CREATE KEYSPACE duplicatekstest  WITH replication = {'class':'SimpleStrategy',
    'replication_factor'
    :1}", null);
    _ = conn -> close();

    return result;
}

function testTableCreation() {
    endpoint c:Client conn {
       host: "localhost",
       port: 9142,
       username: "cassandra",
       password: "cassandra",
       options: {}
    };
    _ = conn -> update("CREATE TABLE peopleinfoks.student(id int PRIMARY KEY,name text, age int)", null);
    _ = conn -> close();
}

function testInsert() {
    endpoint c:Client conn {
       host: "localhost",
       port: 9142,
       username: "cassandra",
       password: "cassandra",
       options: {}
    };
    c:Parameter pID = {cqlType:c:Type.INT, value:2};
    c:Parameter pName = {cqlType:c:Type.TEXT, value:"Tim"};
    c:Parameter pSalary = {cqlType:c:Type.FLOAT, value:100.5};
    c:Parameter pIncome = {cqlType:c:Type.DOUBLE, value:1000.5};
    c:Parameter pMarried = {cqlType:c:Type.BOOLEAN, value:true};
    c:Parameter[] pUpdate = [pID, pName, pSalary, pIncome, pMarried];
    _ = conn -> update("INSERT INTO peopleinfoks.person(id, name, salary, income, married) values (?,?,?,?,?)",
                       pUpdate);
    _ = conn -> close();
}

function testSelectWithParamArray() returns (int, string, float, boolean) {
    endpoint c:Client conn {
       host: "localhost",
       port: 9142,
       username: "cassandra",
       password: "cassandra",
       options: {}
    };
    int[] intDataArray = [1, 5];
    float[] floatDataArray = [100.2, 100.5];
    float[] doubleDataArray = [10000.5, 11100.8];
    string[] stringDataArray = ["Jack", "Jill"];
    boolean[] booleanDataArray = [true, false];

    c:Parameter idArray = {cqlType:c:Type.INT, value:intDataArray};
    c:Parameter nameArray = {cqlType:c:Type.TEXT, value:stringDataArray};
    c:Parameter salaryArray = {cqlType:c:Type.FLOAT, value:floatDataArray};
    c:Parameter incomeArray = {cqlType:c:Type.DOUBLE, value:doubleDataArray};
    c:Parameter marriageStatusArray = {cqlType:c:Type.BOOLEAN, value:booleanDataArray};

    c:Parameter[] params = [idArray, nameArray, salaryArray, incomeArray, marriageStatusArray];

    table dt =? conn -> select("SELECT id, name, salary, married FROM peopleinfoks.person WHERE id in (?) AND
    name in (?) AND salary in (?) AND income in (?) AND married in (?) ALLOW FILTERING", params, typeof RS);

    int id;
    string name;
    float salary;
    boolean married;

    while (dt.hasNext()) {
        var rs =? <RS> dt.getNext();
        id = rs.id;
        name = rs.name;
        salary = rs.salary;
        married = rs.married;
    }
    _ = conn -> close();
    return (id, name, salary, married);
}

function testSelect() returns (int, string, float) {
    endpoint c:Client conn {
       host: "localhost",
       port: 9142,
       username: "cassandra",
       password: "cassandra",
       options: {}
    };
    c:Parameter pID = {cqlType:c:Type.INT, value:1};
    c:Parameter[] params = [pID];
    table dt =? conn -> select("SELECT id, name, salary, married FROM peopleinfoks.person WHERE id = ?", params, typeof
    RS);
    int id;
    string name;
    float salary;
    while (dt.hasNext()) {
            var rs =? <RS> dt.getNext();
            id = rs.id;
            name = rs.name;
            salary = rs.salary;
    }
    _ = conn -> close();
    return (id, name, salary);
}

function testSelectNonExistentColumn() returns (any) {
    endpoint c:Client conn {
        host: "localhost",
        port: 9142,
        username: "cassandra",
        password: "cassandra",
        options: {}
    };
    c:Parameter pID = {cqlType:c:Type.INT, value:1};
    c:Parameter[] params = [pID];
    var result = conn -> select("SELECT x FROM peopleinfoks.person WHERE id = ?", params, typeof RS);
    return result;
}

