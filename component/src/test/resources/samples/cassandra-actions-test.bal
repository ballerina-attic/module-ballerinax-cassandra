import ballerina/cassandra as c;

type RS {
    int id;
    string name;
    float salary;
    boolean married;
};

function testKeySpaceCreation() {
    endpoint c:Client conn {
       host: "localhost",
       port: 9142,
       username: "cassandra",
       password: "cassandra",
       options: {}
    };
    _ = conn -> update("CREATE KEYSPACE dummyks  WITH replication = {'class':'SimpleStrategy', 'replication_factor'
    :1}");
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
    'replication_factor':1}");

    var result = conn -> update("CREATE KEYSPACE duplicatekstest  WITH replication = {'class':'SimpleStrategy',
    'replication_factor':1}");
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
    _ = conn -> update("CREATE TABLE peopleinfoks.student(id int PRIMARY KEY,name text, age int)");
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

    c:Parameter pID = (c:TYPE_INT, 2);
    c:Parameter pName = (c:TYPE_TEXT, "Tim");
    c:Parameter pSalary = (c:TYPE_FLOAT, 100.5);
    c:Parameter pIncome = (c:TYPE_DOUBLE, 1000.5);
    c:Parameter pMarried = (c:TYPE_BOOLEAN, true);

    _ = conn -> update("INSERT INTO peopleinfoks.person(id, name, salary, income, married) values (?,?,?,?,?)",
        pID, pName, pSalary, pIncome, pMarried);
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

    c:Parameter idArray = (c:TYPE_INT, intDataArray);
    c:Parameter nameArray = (c:TYPE_TEXT, stringDataArray);
    c:Parameter salaryArray = (c:TYPE_FLOAT, floatDataArray);
    c:Parameter incomeArray = (c:TYPE_DOUBLE, doubleDataArray);
    c:Parameter marriageStatusArray = (c:TYPE_BOOLEAN, booleanDataArray);

    var temp = conn -> select("SELECT id, name, salary, married FROM peopleinfoks.person WHERE id in (?) AND
    name in (?) AND salary in (?) AND income in (?) AND married in (?) ALLOW FILTERING", RS, idArray, nameArray,
        salaryArray, incomeArray, marriageStatusArray);

    var dt = check temp;

    int id;
    string name;
    float salary;
    boolean married;

    while (dt.hasNext()) {
        var rs = check <RS> dt.getNext();
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

    c:Parameter pID = (c:TYPE_INT, 1);

    var temp = conn -> select("SELECT id, name, salary, married FROM peopleinfoks.person WHERE id = ?", RS, pID);
    table dt = check temp;
    int id;
    string name;
    float salary;
    while (dt.hasNext()) {
            var rs = check <RS> dt.getNext();
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

    c:Parameter pID = (c:TYPE_INT, 1);

    var result = conn -> select("SELECT x FROM peopleinfoks.person WHERE id = ?", RS, pID);
    return result;
}

function testInsertWithNilParams() {
    endpoint c:Client conn {
        host: "localhost",
        port: 9142,
        username: "cassandra",
        password: "cassandra",
        options: {}
    };

    _ = conn -> update("INSERT INTO peopleinfoks.person(id, name, salary, income, married) values (10,'Jim',101.5,1001.5,false)");
    _ = conn -> close();
}

function testSelectWithNilParams() returns (int, string, float) {
    endpoint c:Client conn {
        host: "localhost",
        port: 9142,
        username: "cassandra",
        password: "cassandra",
        options: {}
    };

    var temp = conn -> select("SELECT id, name, salary, married FROM peopleinfoks.person WHERE id = 1", RS);
    table dt = check temp;
    int id;
    string name;
    float salary;
    while (dt.hasNext()) {
        var rs = check <RS> dt.getNext();
        id = rs.id;
        name = rs.name;
        salary = rs.salary;
    }
    _ = conn -> close();
    return (id, name, salary);
}

