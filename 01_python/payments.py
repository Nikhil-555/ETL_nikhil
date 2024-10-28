from script import main

schema_name = "CM_20050609"
table_name = "payments"
columns = ["customerNumber", "checkNumber", "paymentDate", "amount"]
main(schema_name, table_name, columns)
