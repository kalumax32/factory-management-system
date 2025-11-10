import sqlite3

# Connect to the database
conn = sqlite3.connect('inventory.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# Get transaction types
cursor.execute('SELECT type, COUNT(*) as count FROM InventoryTransactions GROUP BY type')
rows = cursor.fetchall()
print('Transaction types:')
for row in rows:
    print(f"  {row['type']}: {row['count']}")

# Check a few sample transactions
cursor.execute('SELECT * FROM InventoryTransactions LIMIT 5')
rows = cursor.fetchall()
print("\nSample transactions:")
for row in rows:
    print(f"  ID: {row['id']}, Product ID: {row['product_id']}, Type: {row['type']}, Quantity: {row['quantity_change']}, Notes: {row['notes']}, Timestamp: {row['timestamp']}")

conn.close()