from script import main

schema_name = "CM_20050609"
table_name = "offices"
columns = ["officeCode", "city", "phone", "addressLine1", "postalCode", "country", "territory"]
main(schema_name, table_name, columns)