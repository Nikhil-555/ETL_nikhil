from script import main

schema_name = "CM_20050609"
table_name = "products"
columns = ["productCode", "productName", "productLine", "productScale", "productVendor", "quantityInStock", "buyPrice", "MSRP"]
main(schema_name, table_name, columns)
