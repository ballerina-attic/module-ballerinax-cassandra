import ballerina.data.cassandra as c;

const int port = 9142;
const string host = "localhost";
const string username = "cassandra";
const string password = "cassandra";

struct RS {
    int id;
    string name;
    float salary;
    boolean married;
}

function testKeySpaceCreation() {
    endpoint<c:ClientConnector> conn {
         create c:ClientConnector(host, port, username, password, {});
    }
    conn.update("CREATE KEYSPACE dummyks  WITH replication = {'class':'SimpleStrategy', 'replication_factor' :1}",
    null);
    conn.close();
}

function testTableCreation() {
    endpoint<c:ClientConnector> conn {
             create c:ClientConnector(host, port, username, password, {});
    }
    conn.update("CREATE TABLE peopleinfoks.student(id int PRIMARY KEY,name text, age int)", null);
    conn.close();
}

function testInsert() {
    endpoint<c:ClientConnector> conn {
                 create c:ClientConnector(host, port, username, password, {});
    }
    c:Parameter pID = {cqlType:c:Type.INT, value:2};
    c:Parameter pName = {cqlType:c:Type.TEXT, value:"Tim"};
    c:Parameter pSalary = {cqlType:c:Type.FLOAT, value:100.5};
    c:Parameter pIncome = {cqlType:c:Type.DOUBLE, value:1000.5};
    c:Parameter pMarried = {cqlType:c:Type.BOOLEAN, value:true};
    c:Parameter[] pUpdate = [pID, pName, pSalary, pIncome, pMarried];
    conn.update("INSERT INTO peopleinfoks.person(id, name, salary, income, married) values (?,?,?,?,?)", pUpdate);
    conn.close();
}

function testSelectWithParamArray() (int id, string name, float salary, boolean married) {
    endpoint<c:ClientConnector> conn {
                 create c:ClientConnector(host, port, username, password, {});
    }

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

    table dt = conn.select("SELECT id, name, salary, married FROM peopleinfoks.person WHERE id in (?) AND
    name in (?) AND salary in (?) AND income in (?) AND married in (?) ALLOW FILTERING", params, typeof RS);

    while (dt.hasNext()) {
        var rs, _ = (RS) dt.getNext();
        id = rs.id;
        name = rs.name;
        salary = rs.salary;
        married = rs.married;
    }
    conn.close();
    return;
}

function testSelect() (int id, string name, float salary) {
    endpoint<c:ClientConnector> conn {
                 create c:ClientConnector(host, port, username, password, {});
    }
    c:Parameter pID = {cqlType:c:Type.INT, value:1};
    c:Parameter[] params = [pID];
    table dt = conn.select("SELECT id, name, salary, married FROM peopleinfoks.person WHERE id = ?", params, typeof
    RS);
    while (dt.hasNext()) {
            var rs, _ = (RS) dt.getNext();
            id = rs.id;
            name = rs.name;
            salary = rs.salary;
    }
    conn.close();
    return;
}

