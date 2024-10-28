from script import main

schema_name = "CM_20050609"
table_name = "orderdetails"
columns = ["orderNumber", "productCode", "quantityOrdered", "priceEach", "orderLineNumber"]
main(schema_name, table_name, columns)
