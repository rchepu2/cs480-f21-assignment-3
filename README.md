# CS480 Homework 3
## Instructions for setup
1. Add mysql-connector-java-8.0.25 to the library 
2. Run my sql server preferrably on port no. 3306 and create a schema with name "dbhw3"
3. Give credentials of mysql server : change username in line no. 8 and password in line no. 9
4. Place the transfile.txt in the same folder as A3Solution.java or give the absolute path of the file in line no. 10
5. Run A3Solution.java

## Description of Program
The program works as follows:
1. Creates a connection with the given mysql server credentials
2. Creates relations employee, department as mentioned in the document
3. Reads the file transfile.txt and converts it to a List of transactions
4. It iterates over each transaction and performs the operations according to the transaction code as specified in the document
5. After all the transactions are completed, it drops the created tables and closes the connection.
