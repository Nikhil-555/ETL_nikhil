from script import main

schema_name = "CM_20050609"
table_name = "customers"
columns = ["customerNumber", "customerName", "contactLastName", "contactFirstName", "phone", "addressLine1", "city", "country"]
main(schema_name, table_name, columns)
