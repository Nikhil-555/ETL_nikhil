from script import main

schema_name = "CM_20050609"
table_name = "productlines"
columns = ["productLine", "textDescription", "htmlDescription", "image"]
main(schema_name, table_name, columns)
