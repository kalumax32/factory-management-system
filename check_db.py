import sqlite3

# Connect to the database
conn = sqlite3.connect('inventory.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# Get table schema
cursor.execute('PRAGMA table_info(products)')
schema = cursor.fetchall()
print("Products table schema:")
for column in schema:
    print(f"  {column['name']} ({column['type']})")

# Get count of products
cursor.execute('SELECT COUNT(*) as count FROM products')
count = cursor.fetchone()
print(f"\nTotal products: {count['count']}")

# Get sample data
cursor.execute('SELECT * FROM products LIMIT 3')
rows = cursor.fetchall()
print("\nSample products:")
for row in rows:
    print(f"  ID: {row['id']}, SKU: {row['sku']}, Name: {row['name']}, Unit: {row['unit']}")
    print(f"    Opening: {row['opening']}, Receipt: {row['receipt']}, Issue: {row['issue']}")

# Check transactions
cursor.execute('SELECT COUNT(*) as count FROM InventoryTransactions')
tx_count = cursor.fetchone()
print(f"\nTotal transactions: {tx_count['count']}")

conn.close()