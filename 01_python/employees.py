from script import main

schema_name = "CM_20050609"
table_name = "employees"
columns = ["employeeNumber", "lastName", "firstName", "extension", "email", "officeCode", "reportsTo", "jobTitle"]
main(schema_name,table_name, columns)
