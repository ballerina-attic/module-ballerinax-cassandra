import wso2/cassandra as c;

type RS record {
    int id;
    string name;
    float salary;
    boolean married;
};

function testKeySpaceCreation() {
    c:Client conn = new({
        host: "localhost",
        port: 9142,
        username: "cassandra",
        password: "cassandra",
        options: {}
    });
    checkpanic conn->update("CREATE KEYSPACE dummyks  WITH replication = {'class':'SimpleStrategy', 'replication_factor'
    :1}");
    conn.stop();
}

function testDuplicateKeySpaceCreation() returns error? {
    c:Client conn = new({
        host: "localhost",
        port: 9142,
        username: "cassandra",
        password: "cassandra",
        options: {}
    });
    checkpanic conn->update("CREATE KEYSPACE duplicatekstest  WITH replication = {'class':'SimpleStrategy',
    'replication_factor':1}");

    var result = conn->update("CREATE KEYSPACE duplicatekstest  WITH replication = {'class':'SimpleStrategy',
    'replication_factor':1}");
    conn.stop();

    return result;
}

function testTableCreation() {
    c:Client conn = new({
        host: "localhost",
        port: 9142,
        username: "cassandra",
        password: "cassandra",
        options: {}
    });
    checkpanic conn->update("CREATE TABLE peopleinfoks.student(id int PRIMARY KEY,name text, age int)");
    conn.stop();
}

function testInsert() {
    c:Client conn = new({
        host: "localhost",
        port: 9142,
        username: "cassandra",
        password: "cassandra",
        options: {}
    });

    c:Parameter pID = { cqlType: c:TYPE_INT, value: 2 };
    c:Parameter pName = { cqlType: c:TYPE_TEXT, value: "Tim" };
    c:Parameter pSalary = { cqlType: c:TYPE_FLOAT, value: 100.5 };
    c:Parameter pIncome = { cqlType: c:TYPE_DOUBLE, value: 1000.5 };
    c:Parameter pMarried = { cqlType: c:TYPE_BOOLEAN, value: true };

    checkpanic conn->update("INSERT INTO peopleinfoks.person(id, name, salary, income, married) values (?,?,?,?,?)",
        pID, pName, pSalary, pIncome, pMarried);
    conn.stop();
}

function testInsertRawParams() {
    c:Client conn = new({
        host: "localhost",
        port: 9142,
        username: "cassandra",
        password: "cassandra",
        options: {}
    });

    c:Parameter pIncome = { cqlType: c:TYPE_DOUBLE, value: 1001.5 };

    checkpanic conn->update("INSERT INTO peopleinfoks.person(id, name, salary, income, married) values (?,?,?,?,?)",
        10, "Tommy", 101.5, pIncome, false);
    conn.stop();
}

function testSelectWithParamArray() returns (int, string, float, boolean) {
    c:Client conn = new({
        host: "localhost",
        port: 9142,
        username: "cassandra",
        password: "cassandra",
        options: {}
    });
    int[] intDataArray = [1, 5];
    float[] floatDataArray = [100.2, 100.5];
    float[] doubleDataArray = [10000.5, 11100.8];
    string[] stringDataArray = ["Jack", "Jill"];
    boolean[] booleanDataArray = [true, false];

    c:Parameter idArray = { cqlType: c:TYPE_INT, value: intDataArray };
    c:Parameter nameArray = { cqlType: c:TYPE_TEXT, value: stringDataArray };
    c:Parameter salaryArray = { cqlType: c:TYPE_FLOAT, value: floatDataArray };
    c:Parameter incomeArray = { cqlType: c:TYPE_DOUBLE, value: doubleDataArray };
    c:Parameter marriageStatusArray = { cqlType: c:TYPE_BOOLEAN, value: booleanDataArray };

    var dt = conn->select("SELECT id, name, salary, married FROM peopleinfoks.person WHERE id in (?) AND
    name in (?) AND salary in (?) AND income in (?) AND married in (?) ALLOW FILTERING", RS, idArray, nameArray,
        salaryArray, incomeArray, marriageStatusArray);

    int id = -1;
    string name = "";
    float salary = -1;
    boolean married = false;

    if (dt is table<record {}>) {
        while (dt.hasNext()) {
            var rs = <RS>dt.getNext();
            id = rs.id;
            name = rs.name;
            salary = rs.salary;
            married = rs.married;
        }
    }
    conn.stop();
    return (id, name, salary, married);
}

function testSelect() returns (int, string, float) {
    c:Client conn = new({
        host: "localhost",
        port: 9142,
        username: "cassandra",
        password: "cassandra",
        options: {}
    });

    var dt = conn->select("SELECT id, name, salary, married FROM peopleinfoks.person WHERE id = ?", RS, 1);

    int id = -1;
    string name = "";
    float salary = -1;

    if (dt is table<record {}>) {
        while (dt.hasNext()) {
            var rs = <RS>dt.getNext();
            id = rs.id;
            name = rs.name;
            salary = rs.salary;
        }
    }
    conn.stop();
    return (id, name, salary);
}

function testSelectNonExistentColumn() returns any | error {
    c:Client conn = new({
        host: "localhost",
        port: 9142,
        username: "cassandra",
        password: "cassandra",
        options: {}
    });

    c:Parameter pID = { cqlType: c:TYPE_INT, value:1 };

    var result = conn->select("SELECT x FROM peopleinfoks.person WHERE id = ?", RS, 1);
    return result;
}

function testInsertWithNilParams() {
    c:Client conn = new({
        host: "localhost",
        port: 9142,
        username: "cassandra",
        password: "cassandra",
        options: {}
    });

    checkpanic conn->update("INSERT INTO peopleinfoks.person(id, name, salary, income, married)
    values (10,'Jim',101.5,1001.5,false)");
    conn.stop();
}

function testSelectWithNilParams() returns (int, string, float) {
    c:Client conn = new({
        host: "localhost",
        port: 9142,
        username: "cassandra",
        password: "cassandra",
        options: {}
    });

    var dt = conn->select("SELECT id, name, salary, married FROM peopleinfoks.person WHERE id = 1", RS);

    int id = -1;
    string name = "";
    float salary = -1;

    if (dt is table<record {}>) {
        while (dt.hasNext()) {
            var rs = <RS>dt.getNext();
            id = rs.id;
            name = rs.name;
            salary = rs.salary;
        }
    }
    conn.stop();
    return (id, name, salary);
}

