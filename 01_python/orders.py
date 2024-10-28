from script import main

schema_name = "CM_20050609"
table_name = "orders"
columns = ["orderNumber", "orderDate", "requiredDate", "shippedDate", "status", "comments", "customerNumber"]
main(schema_name, table_name, columns)
