import java.sql.*;
import java.io.File;
import java.util.*;

public class A3Solution {

    public static final String schemaName = "dbhw3";
    public static final String userName = "root";
    public static final String password = "root";
    public static final String filePath = new File("").getAbsolutePath() + "\\transfile.txt";

    public static Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + schemaName, userName, password);
        } catch (Exception e) {
            System.out.println("Unable to create a connection due to " + e);
        }
        return connection;
    }

    public static void afterAll(Connection connection) {
        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate("DROP TABLE IF EXISTS employee, department");
            connection.close();
        } catch (Exception e) {
            System.out.println("Unable to drop tables and close connection");
        }
    }

    public static void createTables(Connection connection) {
        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate("DROP TABLE IF EXISTS employee, department");
            String createEmployeeTableSql = "CREATE TABLE IF NOT EXISTS `dbhw3`.`employee` (" +
                    "`ename` VARCHAR(45) NOT NULL," +
                    "`dept_name` VARCHAR(45) NULL," +
                    "`salary` VARCHAR(45) NULL," +
                    "`city` VARCHAR(45) NULL," +
                    "PRIMARY KEY (`ename`));";
            String createDepartmentTableSql = "CREATE TABLE IF NOT EXISTS `dbhw3`.`department` (" +
                    "`dept_name` VARCHAR(45) NOT NULL," +
                    "`mname` VARCHAR(45) NULL," +
                    "PRIMARY KEY (`dept_name`)," +
                    "FOREIGN KEY (`mname`)" +
                    "REFERENCES `dbhw3`.`employee` (`ename`))";
            statement.execute(createEmployeeTableSql);
            System.out.println("employee=(ename, dept-name, salary, city) relation created");
            statement.execute(createDepartmentTableSql);
            System.out.println("department=(dept-name, mname) relation created");
            System.out.println("==========================================================");
        } catch (Exception e) {
            System.out.println("Unable to create tables due to " + e);
        }
    }

    public static List<String> readFile() {
        List<String> transactions = new ArrayList<String>();
        try {
            File transfile = new File(filePath);
            Scanner scanner = new Scanner(transfile);
            while (scanner.hasNextLine()) {
                String transaction = scanner.nextLine();
                transactions.add(transaction);
            }
        } catch (Exception e) {
            System.out.println(e);
        }
        return transactions;
    }

    public static void printSubordinates(Connection connection, String[] transaction) {
        String managerName = transaction[1];
        if (!isDepartmentPresentMname(connection, managerName)) {
            System.out.println("Manager not present with name: " + managerName);
            return;
        }
        HashSet<String> set = new HashSet<String>();
        set.add(managerName);
        try {
            printAllSubOrdinates(managerName, connection, set);
        } catch (Exception e) {
            System.out.println("Unable to print subordinates: " + e);
        }
    }

    public static void printAllSubOrdinates(String mname, Connection connection, HashSet<String> set) throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement("select employee.ename from employee inner join department on employee.dept_name = department.dept_name where department.mname = ?");
        preparedStatement.setString(1, mname);
        ResultSet rs = preparedStatement.executeQuery();
        while (rs.next()) {
            String employeeName = rs.getString(1);
            if (!set.contains(employeeName)) {
                System.out.println(employeeName);
                set.add(employeeName);
                printAllSubOrdinates(employeeName, connection, set);
            }
        }
        return;
    }

    public static void deleteEmployeeEname(Connection connection, String[] transaction) {
        try {
            String ename = transaction[1];
            PreparedStatement preparedStatement = connection.prepareStatement("delete from employee where ename = ?");
            preparedStatement.setString(1, ename);
            int i = preparedStatement.executeUpdate();
            if (i == 0)
                System.out.println("Employee with ename: " + ename + " Not found");
            else {
                preparedStatement = connection.prepareStatement("update department set mname = NULL where mname = ?");
                preparedStatement.setString(1, ename);
                preparedStatement.executeUpdate();
                System.out.println("Deleted. Records updated in employee: " + i);
            }
        } catch (Exception e) {
            System.out.println("Unable to delete employee due to: " + e);
        }
    }

    public static void insertEmployee(Connection connection, String[] transaction) {
        String ename = transaction[1];
        String departmentName = transaction[2];
        String salary = transaction[3];
        String city = transaction[4];
        if (isEmployeePresent(connection, ename)) {
            System.out.println("Duplicate name");
            return;
        }

        try {
            PreparedStatement preparedStatement = connection.prepareStatement("insert into employee values(?,?,?,?)");
            preparedStatement.setString(1, ename);
            preparedStatement.setString(2, departmentName);
            preparedStatement.setString(3, salary);
            preparedStatement.setString(4, city);
            preparedStatement.executeUpdate();
            System.out.println("Added Employee " + ename);
        } catch (Exception e) {
            System.out.println("Unable to add employee due to: " + e);
        }
    }

    public static void insertDepartment(Connection connection, String[] transaction) {
        String departmentName = transaction[1];
        String managerName = transaction[2];

        if (isDepartmentPresentDeptName(connection, departmentName)) {
            System.out.println("Deleting existing department with dept-name: " + departmentName);
            String[] temperoryTransaction = new String[2];
            temperoryTransaction[0] = "3";
            temperoryTransaction[1] = departmentName;
            deleteDepartment(connection, temperoryTransaction);
        }

        if (!isEmployeePresent(connection, managerName)) {
            System.out.println("No employee found with ename: " + managerName + ". Unable to add the department");
            return;
        }

        try {
            System.out.println("Employee with ename: " + managerName + " present. Adding Department");
            PreparedStatement preparedStatement = connection.prepareStatement("insert into department values(?,?)");
            preparedStatement.setString(1, departmentName);
            preparedStatement.setString(2, managerName);
            preparedStatement.executeUpdate();
            System.out.println("Department added");
        } catch (Exception e) {
            System.out.println("Unable to add department due to " + e);
        }
    }

    public static void deleteDepartment(Connection connection, String[] transaction) {
        String departmentName = transaction[1];
        if (!isDepartmentPresentDeptName(connection, departmentName)) {
            System.out.println("Department with dept-name: " + departmentName + " Not found");
            return;
        }

        try {
            PreparedStatement preparedStatement = connection.prepareStatement("delete from department where dept_name=?");
            preparedStatement.setString(1, departmentName);
            preparedStatement.executeUpdate();
            preparedStatement = connection.prepareStatement("update employee set dept_name = NULL where dept_name = ?");
            preparedStatement.setString(1, departmentName);
            preparedStatement.executeUpdate();
            System.out.println("Deleted department with dept-name: " + departmentName);
        } catch (Exception e) {
            System.out.println("Unable to delete department due to: " + e);
        }
    }

    public static void printDepartments(Connection connection, String[] transaction) {
        String managerName = transaction[1];
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("select dept_name from employee where mname=?");
            preparedStatement.setString(1, managerName);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next())
                System.out.println(resultSet.getString(1));
        } catch (Exception e) {
            System.out.println("Unable to print departments due to: " + e);
        }
    }

    public static boolean isEmployeePresent(Connection connection, String ename) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("select * from employee where ename=?");
            preparedStatement.setString(1, ename);
            ResultSet resultSet = preparedStatement.executeQuery();
            return resultSet.next();
        } catch (Exception e) {
            System.out.println("Unable to find employee");
        }
        return false;
    }

    public static boolean isDepartmentPresentDeptName(Connection connection, String departmentName) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("select * from department where dept_name=?");
            preparedStatement.setString(1, departmentName);
            ResultSet resultSet = preparedStatement.executeQuery();
            return resultSet.next();
        } catch (Exception e) {
            System.out.println("Unable to find department");
        }
        return false;
    }

    public static boolean isDepartmentPresentMname(Connection connection, String managerName) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("select * from department where mname=?");
            preparedStatement.setString(1, managerName);
            ResultSet resultSet = preparedStatement.executeQuery();
            return resultSet.next();
        } catch (Exception e) {
            System.out.println("Unable to find department");
        }
        return false;
    }


    public static void main(String args[]) {
        Connection connection = getConnection();
        createTables(connection);
        List<String> transactions = readFile();
        for (String transactionLine : transactions) {
            String[] transaction = transactionLine.split(" ");
            for (String transactionToken : transaction)
                transactionToken = transactionToken.trim();
            String transactionCode = transaction[0];
            switch (transactionCode) {
                case "1":
                    deleteEmployeeEname(connection, transaction);
                    break;

                case "2":
                    insertEmployee(connection, transaction);
                    break;

                case "3":
                    deleteDepartment(connection, transaction);
                    break;

                case "4":
                    insertDepartment(connection, transaction);
                    break;

                case "5":
                    printSubordinates(connection, transaction);
                    break;

                case "6":
                    printDepartments(connection, transaction);
                    break;
            }
            System.out.println("-----------------------------------------------------");
        }
        afterAll(connection);
    }
}