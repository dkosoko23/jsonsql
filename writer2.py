import mysql.connector
import json
import time

# Start measuring execution time
start_time = time.time()

# Load the JSON data from your file
with open("product_catalog.json", "r") as json_file:
    data = json.load(json_file)

# Connect to your MySQL database
connection = mysql.connector.connect(
    host="localhost",
    port="3307",
    user="root",
    password="morijin",
    database="catalog"
)

# Create a cursor
cursor = connection.cursor()

# Iterate through the JSON data and insert it into the tables
for product_group in data:
    # Insert into the product_group table
    cursor.execute("INSERT IGNORE INTO product_group (item_group_id, group_title, supplier_id, long_description) VALUES (%s, %s, %s, %s)",
                   (product_group["item_group_id"], product_group.get("group_title", ""), product_group.get("supplier_id", ""), product_group.get("long_description", "")))

    for category in product_group.get("categories", []):
        # Insert into the category table
        cursor.execute("INSERT IGNORE INTO category (category_id, parent, name) VALUES (%s, %s, %s)",
                       (category.get("category_id", ""), category.get("parent", ""), category.get("name", "")))

    for product_item in product_group.get("items", []):
        item_id = product_item.get("item_id", "")
        item_group_id = product_group.get("item_group_id", "")

        if item_id and item_group_id:  # Check for essential keys
            # Insert into the product_item table
            cursor.execute("INSERT IGNORE INTO product_item (item_id, item_group_id, supplier_item_id, item_title, inventory, inventory_unit, reg_price, client_price, ship_methods, client_ship_cost, upc, brand, country_of_origin, main_image, small_image, mfg_item_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (item_id, item_group_id, product_item.get("supplier_item_id", ""), product_item.get("item_title", ""), product_item.get("item_availability", {}).get("inventory", ""),
                product_item.get("item_availability", {}).get("inventory_unit", ""), product_item.get("reg_price", ""), product_item.get("client_price", ""),
                json.dumps(product_item.get("ship_methods", "")), product_item.get("client_ship_cost", ""), product_item.get("upc", ""),
                product_item.get("brand", ""), product_item.get("country_of_origin", ""), product_item.get("main_image", ""),
                product_item.get("small_image", ""), product_item.get("mfg_item_id", "")))

            for feature in product_group.get("features", []):
                # Insert into the product_features table
                cursor.execute("INSERT IGNORE INTO product_features (item_id, feature) VALUES (%s, %s)", (item_id, feature))

# Commit the changes and close the cursor and connection
connection.commit()
cursor.close()
connection.close()

# Calculate the total execution time
end_time = time.time()
execution_time = end_time - start_time

# Print the execution time
print(f"Execution time: {execution_time} seconds")
