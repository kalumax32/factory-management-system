import sqlite3
from flask import Flask, render_template, request, jsonify
import datetime
import pandas as pd
import os
import sys
import openpyxl
# --- App & Database Setup ---

app = Flask(__name__)
DB_NAME = 'inventory.db'

def get_db():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    # This function is unchanged
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                unit TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS InventoryTransactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                quantity_change INTEGER NOT NULL,
                notes TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products (id)
            )
        ''')
        # Add a new table for suppliers (optional enhancement)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS suppliers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                contact TEXT
            )
        ''')
        db.commit()

# --- Frontend Route ---

@app.route('/')
def home():
    # This function is unchanged
    return render_template('index.html')


@app.route('/stock-issues')
def stock_issues():
    """Render the stock issues report page"""
    return render_template('stock_issues.html')


@app.route('/stock-receipts')
def stock_receipts():
    """Render the stock receipts report page"""
    return render_template('stock_receipts.html')


@app.route('/stock-issues-date-range')
def stock_issues_date_range():
    """Render the stock issues by date range report page"""
    return render_template('stock_issues_date_range.html')


@app.route('/stock-receipts-date-range')
def stock_receipts_date_range():
    """Render the stock receipts by date range report page"""
    return render_template('stock_receipts_date_range.html')


@app.route('/transaction-history')
def transaction_history():
    """Render the transaction history page"""
    return render_template('transaction_history.html')


# --- API Routes (Unchanged) ---

@app.route('/api/products', methods=['GET'])
def get_products():
    # Read the Excel file to get additional data
    try:
        excel_df = pd.read_excel('STOCK.xlsx', sheet_name='BALANCE')
        # Create a dictionary for quick lookup
        excel_data = {}
        for _, row in excel_df.iterrows():
            excel_data[row['S.NO.']] = {
                'opening': row['OPENING'],
                'receipt': row['RECEIPT'],
                'issue': row['ISSUE'],
                'balance': row['BALANCE'] if 'BALANCE' in excel_df.columns else None
            }
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        excel_data = {}
    
    # This function is unchanged
    db = get_db()
    cursor = db.cursor()
    query = """
        SELECT 
            p.id, p.sku, p.name, p.unit, 
            COALESCE(SUM(t.quantity_change), 0) AS quantity
        FROM products p
        LEFT JOIN InventoryTransactions t ON p.id = t.product_id
        GROUP BY p.id, p.sku, p.name, p.unit
        ORDER BY p.name;
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    
    # Add Excel data to products
    products = []
    for row in rows:
        product = dict(row)
        # Extract S.NO. from SKU (format: ITEM-{S.NO.})
        try:
            s_no = int(product['sku'].replace('ITEM-', ''))
            if s_no in excel_data:
                product['opening'] = excel_data[s_no]['opening']
                product['receipt'] = excel_data[s_no]['receipt']
                product['issue'] = excel_data[s_no]['issue']
                # Use the BALANCE from Excel if available, otherwise calculate from transactions
                if excel_data[s_no]['balance'] is not None:
                    product['quantity'] = excel_data[s_no]['balance']
            else:
                product['opening'] = 0
                product['receipt'] = 0
                product['issue'] = 0
        except:
            product['opening'] = 0
            product['receipt'] = 0
            product['issue'] = 0
            
        products.append(product)
        
    db.close()
    return jsonify(products)

@app.route('/api/product', methods=['POST'])
def add_product():
    # Validate JSON data
    if request.json is None:
        return jsonify({'error': 'No JSON data provided'}), 400
        
    new_product = request.json
    if 'sku' not in new_product or 'name' not in new_product or 'unit' not in new_product or 'quantity' not in new_product:
        return jsonify({'error': 'Missing required fields: sku, name, unit, quantity'}), 400
    
    sku = new_product['sku']
    name = new_product['name']
    unit = new_product['unit']
    initial_quantity = int(new_product['quantity'])
    # Get date if provided
    transaction_date = new_product.get('date')

    db = get_db()
    cursor = db.cursor()
    try:
        cursor.execute("INSERT INTO products (sku, name, unit) VALUES (?, ?, ?)", (sku, name, unit))
        product_id = cursor.lastrowid
        if initial_quantity != 0:
            note_text = 'Initial stock'
            if transaction_date:
                note_text += f' (Date: {transaction_date})'
            cursor.execute(
                "INSERT INTO InventoryTransactions (product_id, type, quantity_change, notes) VALUES (?, 'initial', ?, ?)",
                (product_id, initial_quantity, note_text)
            )
        db.commit()
    except sqlite3.IntegrityError:
        db.rollback()
        db.close()
        return jsonify({'error': 'SKU already exists'}), 400
    except Exception as e:
        db.rollback()
        db.close()
        return jsonify({'error': str(e)}), 500
    finally:
        if 'db' in locals() and db:
            db.close()
    return jsonify({'message': 'Product added successfully!'}), 201


# --- Enhanced Purchase and Sale Operations ---

@app.route('/api/purchase', methods=['POST'])
def record_purchase():
    """Record a purchase transaction (stock in)"""
    # Validate JSON data
    if request.json is None:
        return jsonify({'error': 'No JSON data provided'}), 400
        
    data = request.json
    product_id = data.get('product_id') if data else None
    quantity = data.get('quantity') if data else None
    supplier = data.get('supplier', '') if data else ''
    notes = data.get('notes', '') if data else ''
    transaction_date = data.get('date') if data else None  # Get date if provided
    
    # Validate input
    if not product_id or not quantity:
        return jsonify({'error': 'Product ID and quantity are required'}), 400
    
    try:
        quantity = int(quantity)
        if quantity <= 0:
            return jsonify({'error': 'Quantity must be a positive number'}), 400
    except ValueError:
        return jsonify({'error': 'Quantity must be a valid number'}), 400
    
    try:
        db = get_db()
        cursor = db.cursor()
        
        # Verify product exists
        cursor.execute("SELECT id, name FROM products WHERE id = ?", (product_id,))
        product = cursor.fetchone()
        if not product:
            db.close()
            return jsonify({'error': 'Product not found'}), 404
        
        # Record the purchase transaction
        note_text = f"Purchase: {quantity} units"
        if supplier:
            note_text += f" from {supplier}"
        if notes:
            note_text += f" - {notes}"
        if transaction_date:
            note_text += f" (Date: {transaction_date})"
            
        cursor.execute(
            "INSERT INTO InventoryTransactions (product_id, type, quantity_change, notes) VALUES (?, 'purchase', ?, ?)",
            (product_id, quantity, note_text)
        )
        db.commit()
        db.close()
        
        return jsonify({
            'message': f'Purchase recorded successfully for {product["name"]}!',
            'transaction_type': 'purchase',
            'quantity': quantity
        }), 201
    except Exception as e:
        return jsonify({'error': f'Failed to record purchase: {str(e)}'}), 500


@app.route('/api/sale', methods=['POST'])
def record_sale():
    """Record a sale transaction (stock out)"""
    # Validate JSON data
    if request.json is None:
        return jsonify({'error': 'No JSON data provided'}), 400
        
    data = request.json
    product_id = data.get('product_id') if data else None
    quantity = data.get('quantity') if data else None
    customer = data.get('customer', '') if data else ''
    notes = data.get('notes', '') if data else ''
    transaction_date = data.get('date') if data else None  # Get date if provided
    
    # Validate input
    if not product_id or not quantity:
        return jsonify({'error': 'Product ID and quantity are required'}), 400
    
    try:
        quantity = int(quantity)
        if quantity <= 0:
            return jsonify({'error': 'Quantity must be a positive number'}), 400
    except ValueError:
        return jsonify({'error': 'Quantity must be a valid number'}), 400
    
    try:
        db = get_db()
        cursor = db.cursor()
        
        # Verify product exists
        cursor.execute("SELECT id, name FROM products WHERE id = ?", (product_id,))
        product = cursor.fetchone()
        if not product:
            db.close()
            return jsonify({'error': 'Product not found'}), 404
            
        # Check current stock level
        cursor.execute("""
            SELECT COALESCE(SUM(quantity_change), 0) AS current_stock
            FROM InventoryTransactions 
            WHERE product_id = ?
        """, (product_id,))
        current_stock_row = cursor.fetchone()
        current_stock = current_stock_row['current_stock'] if current_stock_row else 0
        
        if current_stock < quantity:
            db.close()
            return jsonify({
                'error': f'Insufficient stock. Current stock: {current_stock}, Requested: {quantity}'
            }), 400
        
        # Record the sale transaction
        note_text = f"Sale: {quantity} units"
        if customer:
            note_text += f" to {customer}"
        if notes:
            note_text += f" - {notes}"
        if transaction_date:
            note_text += f" (Date: {transaction_date})"
            
        cursor.execute(
            "INSERT INTO InventoryTransactions (product_id, type, quantity_change, notes) VALUES (?, 'sale', ?, ?)",
            (product_id, -quantity, note_text)
        )
        db.commit()
        db.close()
        
        return jsonify({
            'message': f'Sale recorded successfully for {product["name"]}!',
            'transaction_type': 'sale',
            'quantity': quantity,
            'current_stock': current_stock - quantity
        }), 201
    except Exception as e:
        return jsonify({'error': f'Failed to record sale: {str(e)}'}), 500


@app.route('/api/transactions/<int:product_id>', methods=['GET'])
def get_product_transactions(product_id):
    """Get transaction history for a specific product"""
    try:
        db = get_db()
        cursor = db.cursor()
        
        # Verify product exists
        cursor.execute("SELECT id, name FROM products WHERE id = ?", (product_id,))
        product = cursor.fetchone()
        if not product:
            db.close()
            return jsonify({'error': 'Product not found'}), 404
        
        # Get transaction history
        cursor.execute("""
            SELECT id, type, quantity_change, notes, timestamp
            FROM InventoryTransactions 
            WHERE product_id = ?
            ORDER BY timestamp DESC
        """, (product_id,))
        
        rows = cursor.fetchall()
        transactions = [dict(row) for row in rows] if rows else []
        db.close()
        
        return jsonify({
            'product_id': product_id,
            'product_name': product['name'],
            'transactions': transactions
        })
    except Exception as e:
        return jsonify({'error': f'Failed to fetch transactions: {str(e)}'}), 500


@app.route('/api/products/search', methods=['GET'])
def search_products():
    """Search products by name or SKU"""
    query = request.args.get('q', '').strip()
    
    if not query:
        return jsonify([]), 200
    
    db = get_db()
    cursor = db.cursor()
    
    # Search for products matching the query in name or SKU
    search_term = f"%{query}%"
    cursor.execute("""
        SELECT 
            p.id, p.sku, p.name, p.unit, 
            COALESCE(SUM(t.quantity_change), 0) AS quantity
        FROM products p
        LEFT JOIN InventoryTransactions t ON p.id = t.product_id
        WHERE p.name LIKE ? OR p.sku LIKE ?
        GROUP BY p.id, p.sku, p.name, p.unit
        ORDER BY p.name
    """, (search_term, search_term))
    
    rows = cursor.fetchall()
    products = [dict(row) for row in rows] if rows else []
    db.close()
    
    return jsonify(products)


@app.route('/api/stock-issues', methods=['GET'])
def get_stock_issues():
    """Get stock issues data grouped by date from Excel file"""
    try:
        # Read the Excel file
        df = pd.read_excel('STOCK.xlsx', sheet_name='ISSUE')
        
        # Get all date columns (excluding the first 3 columns: S.NO., DESCRIPTION, UNIT)
        date_columns = list(df.columns[3:])
        
        # Create a list to store issues by date
        issues_by_date = []
        
        # For each date column, collect issues
        for date_col in date_columns:
            # Get rows where this date has issues (convert to numeric first)
            df[date_col] = pd.to_numeric(df[date_col], errors='coerce')
            df[date_col] = df[date_col].fillna(0)
            filtered_df = df[df[date_col] > 0]
            if len(filtered_df) > 0:
                # Parse and format the date as "date.month.year"
                col_date = None
                date_formats = ['%d.%m.%y', '%d/%m/%y', '%Y-%m-%d %H:%M:%S', '%d.%m.%Y', '%Y-%m-%d']
                
                for fmt in date_formats:
                    try:
                        col_date = datetime.datetime.strptime(str(date_col), fmt)
                        break
                    except ValueError:
                        continue
                
                # Format the date or use original if parsing fails
                if col_date:
                    formatted_date = col_date.strftime('%d.%m.%Y')
                else:
                    formatted_date = str(date_col)
                
                # Process each row using iloc
                for i in range(len(filtered_df)):
                    row = filtered_df.iloc[i]
                    issues_by_date.append({
                        'date': formatted_date,  # Use formatted date
                        's_no': int(row['S.NO.']),  # S.NO. column
                        'description': str(row['DESCRIPTION']),  # DESCRIPTION column
                        'unit': str(row['UNIT']),  # UNIT column
                        'quantity': float(row[date_col])  # Date column value
                    })
        
        # Sort by date (this might need adjustment based on actual date format)
        # For now, we'll just return as is
        return jsonify(issues_by_date)
    except Exception as e:
        return jsonify({'error': f'Failed to fetch stock issues by date: {str(e)}'}), 500


@app.route('/api/stock-receipts', methods=['GET'])
def get_stock_receipts():
    """Get stock receipts data grouped by date from Excel file"""
    try:
        # Read the Excel file
        df = pd.read_excel('STOCK.xlsx', sheet_name='RECEIPT')
        
        # Get all date columns (excluding the first 3 columns: S.NO., DESCRIPTION, UNIT)
        date_columns = list(df.columns[3:-1])  # Exclude 'TOTAL' column at the end
        
        # Create a list to store receipts by date
        receipts_by_date = []
        
        # For each date column, collect receipts
        for date_col in date_columns:
            # Get rows where this date has receipts (convert to numeric first)
            df[date_col] = pd.to_numeric(df[date_col], errors='coerce')
            df[date_col] = df[date_col].fillna(0)
            filtered_df = df[df[date_col] > 0]
            if len(filtered_df) > 0:
                # Parse and format the date as "date.month.year"
                col_date = None
                date_formats = ['%d.%m.%y', '%d/%m/%y', '%Y-%m-%d %H:%M:%S', '%d.%m.%Y', '%Y-%m-%d']
                
                for fmt in date_formats:
                    try:
                        col_date = datetime.datetime.strptime(str(date_col), fmt)
                        break
                    except ValueError:
                        continue
                
                # Format the date or use original if parsing fails
                if col_date:
                    formatted_date = col_date.strftime('%d.%m.%Y')
                else:
                    formatted_date = str(date_col)
                
                # Process each row using iloc
                for i in range(len(filtered_df)):
                    row = filtered_df.iloc[i]
                    receipts_by_date.append({
                        'date': formatted_date,  # Use formatted date
                        's_no': int(row['S.NO.']),  # S.NO. column
                        'description': str(row['DESCRIPTION']),  # DESCRIPTION column
                        'unit': str(row['UNIT']),  # UNIT column
                        'quantity': float(row[date_col])  # Date column value
                    })
        
        # Sort by date (this might need adjustment based on actual date format)
        # For now, we'll just return as is
        return jsonify(receipts_by_date)
    except Exception as e:
        return jsonify({'error': f'Failed to fetch stock receipts by date: {str(e)}'}), 500


@app.route('/api/transaction', methods=['POST'])
def add_transaction():
    # Validate JSON data
    if request.json is None:
        return jsonify({'error': 'No JSON data provided'}), 400
        
    data = request.json
    if 'product_id' not in data or 'type' not in data or 'quantity' not in data:
        return jsonify({'error': 'Missing required fields: product_id, type, quantity'}), 400
    
    product_id = data['product_id']
    tx_type = data['type']
    quantity = int(data['quantity'])
    
    if tx_type == 'purchase':
        quantity_change = quantity
        notes = f"Stock In (Purchase): {quantity} units"
    elif tx_type == 'sale':
        quantity_change = -quantity
        notes = f"Stock Out (Sale): {quantity} units"
    else:
        return jsonify({'error': 'Invalid transaction type'}), 400

    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute(
            "INSERT INTO InventoryTransactions (product_id, type, quantity_change, notes) VALUES (?, ?, ?, ?)",
            (product_id, tx_type, quantity_change, notes)
        )
        db.commit()
        db.close()
        return jsonify({'message': 'Transaction recorded!'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# --- **** UPDATED IMPORT ROUTE **** ---

@app.route('/api/import-stock')
def import_stock():
    # This is the file you have on your computer
    file_path = 'STOCK.xlsx'
    
    # This is the name of the tab (sheet) inside your Excel file
    sheet_name = 'BALANCE' 
    
    # 1. Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File not found at {os.path.abspath(file_path)}", file=sys.stderr)
        return jsonify({'error': f"File '{file_path}' not found. Make sure it's in the same folder as app.py."}), 404

    # 2. Try to read the file
    try:
        # This is the new line that reads the Excel file and "BALANCE" sheet
        df = pd.read_excel(file_path, sheet_name=sheet_name)
    except Exception as e:
        print(f"Error reading Excel file: {e}", file=sys.stderr)
        return jsonify({'error': f"Could not read file: {e}. Is 'openpyxl' installed? Is the sheet named 'BALANCE'?"}), 500

    # 3. Clean the data (same as before)
    try:
        df['OPENING'] = pd.to_numeric(df['OPENING'], errors='coerce')
        valid_df = df[
            ~df['DESCRIPTION'].isin(['0', 0]) &
            df['DESCRIPTION'].notna() &
            df['OPENING'].notna()
        ].copy()
        # Create a new DataFrame with unique S.NO. values to avoid linter issues
        valid_df = valid_df.groupby('S.NO.').first().reset_index()
    except Exception as e:
        print(f"Error cleaning data: {e}", file=sys.stderr)
        return jsonify({'error': f"Error during data cleaning: {e}"}), 500

    
    db = get_db()
    cursor = db.cursor()

    # 4. Clear existing data (same as before)
    try:
        cursor.execute("DELETE FROM InventoryTransactions")
        cursor.execute("DELETE FROM products")
        cursor.execute("DELETE FROM sqlite_sequence WHERE name IN ('products', 'InventoryTransactions')")
        db.commit()
    except Exception as e:
        db.rollback()
        if 'db' in locals() and db:
            db.close()
        return jsonify({'error': f"Error clearing tables: {e}"}), 500

    # 5. Iterate and insert (same as before)
    items_added_count = 0
    for index, row in valid_df.iterrows():
        sku = ""
        name = ""
        try:
            name = str(row['DESCRIPTION'])
            sku = f"ITEM-{row['S.NO.']}" 
            unit = str(row['UNIT'])
            opening_stock = int(row['OPENING'])

            cursor.execute("INSERT INTO products (sku, name, unit) VALUES (?, ?, ?)", (sku, name, unit))
            product_id = cursor.lastrowid
            
            if opening_stock != 0:
                cursor.execute(
                    "INSERT INTO InventoryTransactions (product_id, type, quantity_change, notes) VALUES (?, 'initial', ?, 'Imported opening balance')",
                    (product_id, opening_stock)
                )
            items_added_count += 1
            
        except Exception as e:
            db.rollback()
            if 'db' in locals() and db:
                db.close()
            print(f"--- IMPORT FAILED ---", file=sys.stderr)
            print(f"Failed on row {index} in the Excel file.", file=sys.stderr)
            print(f"SKU: {sku}, Name: {name}", file=sys.stderr)
            print(f"Error: {e}", file=sys.stderr)
            return jsonify({
                'error': f"Import failed on row {index} (SKU: {sku}, Name: {name}). Check terminal for details. Error: {e}"
            }), 500
            
    db.commit()
    if 'db' in locals() and db:
        db.close()

    return jsonify({
        'message': f"Import successful! Added {items_added_count} products from 'STOCK.xlsx'."
    })


@app.route('/api/stock-issues-date-range', methods=['GET'])
def get_stock_issues_date_range():
    """Get stock issues data within a date range from Excel file"""
    try:
        # Get date parameters from query string
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({'error': 'Start date and end date are required'}), 400
        
        # Read the Excel file
        df = pd.read_excel('STOCK.xlsx', sheet_name='ISSUE')
        
        # Get all date columns (excluding the first 3 columns: S.NO., DESCRIPTION, UNIT)
        date_columns = list(df.columns[3:])
        
        # Filter date columns based on the provided date range
        # Convert string dates to datetime for comparison
        try:
            start_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
        
        # Filter date columns that fall within the date range
        filtered_date_columns = []
        for col in date_columns:
            col_str = str(col)
            col_date = None
            
            # Try different date formats
            date_formats = ['%d.%m.%y', '%d/%m/%y', '%Y-%m-%d %H:%M:%S', '%d.%m.%Y', '%Y-%m-%d']
            
            for fmt in date_formats:
                try:
                    col_date = datetime.datetime.strptime(col_str, fmt)
                    break  # If successful, break out of the loop
                except ValueError:
                    continue  # Try the next format
            
            # If we couldn't parse it as a date, skip it
            if col_date is None:
                continue
                
            # Check if the date falls within our range
            if start_dt <= col_date <= end_dt:
                filtered_date_columns.append((col, col_date))  # Store both original and parsed date
        
        # Sort the filtered date columns by date
        filtered_date_columns.sort(key=lambda x: x[1])
        
        # Create a list to store issues within the date range
        issues_in_range = []
        
        # For each filtered date column, collect issues
        for date_col, parsed_date in filtered_date_columns:
            # Format the date as "date.month.year" without time (using period instead of comma)
            formatted_date = parsed_date.strftime('%d.%m.%Y')
            
            # Get rows where this date has issues (convert to numeric first)
            df[date_col] = pd.to_numeric(df[date_col], errors='coerce')
            df[date_col] = df[date_col].fillna(0)
            filtered_df = df[df[date_col] > 0]
            if len(filtered_df) > 0:
                # Process each row using iloc
                for i in range(len(filtered_df)):
                    row = filtered_df.iloc[i]
                    issues_in_range.append({
                        'date': formatted_date,  # Use formatted date
                        's_no': int(row['S.NO.']),  # S.NO. column
                        'description': str(row['DESCRIPTION']),  # DESCRIPTION column
                        'unit': str(row['UNIT']),  # UNIT column
                        'quantity': float(row[date_col])  # Date column value
                    })
        
        return jsonify(issues_in_range)
    except Exception as e:
        return jsonify({'error': f'Failed to fetch stock issues by date range: {str(e)}'}), 500


@app.route('/api/stock-issues-date-range-horizontal', methods=['GET'])
def get_stock_issues_date_range_horizontal():
    """Get stock issues data within a date range from Excel file in horizontal format"""
    try:
        # Get date parameters from query string
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({'error': 'Start date and end date are required'}), 400
        
        # Read the Excel file
        df = pd.read_excel('STOCK.xlsx', sheet_name='ISSUE')
        
        # Get all date columns (excluding the first 3 columns: S.NO., DESCRIPTION, UNIT)
        date_columns = list(df.columns[3:])
        
        # Filter date columns based on the provided date range
        # Convert string dates to datetime for comparison
        try:
            start_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
        
        # Filter date columns that fall within the date range
        filtered_date_columns = []
        for col in date_columns:
            col_str = str(col)
            col_date = None
            
            # Try different date formats
            date_formats = ['%d.%m.%y', '%d/%m/%y', '%Y-%m-%d %H:%M:%S', '%d.%m.%Y', '%Y-%m-%d']
            
            for fmt in date_formats:
                try:
                    col_date = datetime.datetime.strptime(col_str, fmt)
                    break  # If successful, break out of the loop
                except ValueError:
                    continue  # Try the next format
            
            # If we couldn't parse it as a date, skip it
            if col_date is None:
                continue
                
            # Check if the date falls within our range
            if start_dt <= col_date <= end_dt:
                filtered_date_columns.append((col, col_date))  # Store both original and parsed date
        
        # Sort the filtered date columns by date
        filtered_date_columns.sort(key=lambda x: x[1])
        
        # Create a list to store issues within the date range
        issues_in_range = []
        
        # For each filtered date column, collect issues
        for date_col, parsed_date in filtered_date_columns:
            # Format the date as "date.month.year" without time (using period instead of comma)
            formatted_date = parsed_date.strftime('%d.%m.%Y')
            
            # Get rows where this date has issues (convert to numeric first)
            df[date_col] = pd.to_numeric(df[date_col], errors='coerce')
            df[date_col] = df[date_col].fillna(0)
            filtered_df = df[df[date_col] > 0]
            if len(filtered_df) > 0:
                # Process each row using iloc
                for i in range(len(filtered_df)):
                    row = filtered_df.iloc[i]
                    issues_in_range.append({
                        'date': formatted_date,  # Use formatted date
                        's_no': int(row['S.NO.']),  # S.NO. column
                        'description': str(row['DESCRIPTION']),  # DESCRIPTION column
                        'unit': str(row['UNIT']),  # UNIT column
                        'quantity': float(row[date_col])  # Date column value
                    })
        
        return jsonify(issues_in_range)
    except Exception as e:
        return jsonify({'error': f'Failed to fetch stock issues by date range: {str(e)}'}), 500


@app.route('/api/stock-receipts-date-range-horizontal', methods=['GET'])
def get_stock_receipts_date_range_horizontal():
    """Get stock receipts data within a date range from Excel file in horizontal format"""
    try:
        # Get date parameters from query string
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({'error': 'Start date and end date are required'}), 400
        
        # Read the Excel file
        df = pd.read_excel('STOCK.xlsx', sheet_name='RECEIPT')
        
        # Get all date columns (excluding the first 3 columns: S.NO., DESCRIPTION, UNIT)
        date_columns = list(df.columns[3:-1])  # Exclude 'TOTAL' column at the end
        
        # Filter date columns based on the provided date range
        # Convert string dates to datetime for comparison
        try:
            start_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
        
        # Filter date columns that fall within the date range
        filtered_date_columns = []
        for col in date_columns:
            col_str = str(col)
            col_date = None
            
            # Try different date formats
            date_formats = ['%d.%m.%y', '%d/%m/%y', '%Y-%m-%d %H:%M:%S', '%d.%m.%Y', '%Y-%m-%d']
            
            for fmt in date_formats:
                try:
                    col_date = datetime.datetime.strptime(col_str, fmt)
                    break  # If successful, break out of the loop
                except ValueError:
                    continue  # Try the next format
            
            # If we couldn't parse it as a date, skip it
            if col_date is None:
                continue
                
            # Check if the date falls within our range
            if start_dt <= col_date <= end_dt:
                filtered_date_columns.append((col, col_date))  # Store both original and parsed date
        
        # Sort the filtered date columns by date
        filtered_date_columns.sort(key=lambda x: x[1])
        
        # Create a list to store receipts within the date range
        receipts_in_range = []
        
        # For each filtered date column, collect receipts
        for date_col, parsed_date in filtered_date_columns:
            # Format the date as "date.month.year" without time (using period instead of comma)
            formatted_date = parsed_date.strftime('%d.%m.%Y')
            
            # Get rows where this date has receipts (convert to numeric first)
            df[date_col] = pd.to_numeric(df[date_col], errors='coerce')
            df[date_col] = df[date_col].fillna(0)
            filtered_df = df[df[date_col] > 0]
            if len(filtered_df) > 0:
                # Process each row using iloc
                for i in range(len(filtered_df)):
                    row = filtered_df.iloc[i]
                    receipts_in_range.append({
                        'date': formatted_date,  # Use formatted date
                        's_no': int(row['S.NO.']),  # S.NO. column
                        'description': str(row['DESCRIPTION']),  # DESCRIPTION column
                        'unit': str(row['UNIT']),  # UNIT column
                        'quantity': float(row[date_col])  # Date column value
                    })
        
        return jsonify(receipts_in_range)
    except Exception as e:
        return jsonify({'error': f'Failed to fetch stock receipts by date range: {str(e)}'}), 500


@app.route('/api/transaction/<int:transaction_id>', methods=['DELETE'])
def delete_transaction(transaction_id):
    """Delete a specific transaction by ID"""
    try:
        db = get_db()
        cursor = db.cursor()
        
        # First, verify the transaction exists
        cursor.execute("SELECT id, product_id, type, quantity_change, notes FROM InventoryTransactions WHERE id = ?", (transaction_id,))
        transaction = cursor.fetchone()
        
        if not transaction:
            db.close()
            return jsonify({'error': 'Transaction not found'}), 404
        
        # Store transaction details for response
        transaction_details = {
            'id': transaction['id'],
            'product_id': transaction['product_id'],
            'type': transaction['type'],
            'quantity_change': transaction['quantity_change'],
            'notes': transaction['notes']
        }
        
        # Delete the transaction
        cursor.execute("DELETE FROM InventoryTransactions WHERE id = ?", (transaction_id,))
        db.commit()
        db.close()
        
        return jsonify({
            'message': 'Transaction deleted successfully',
            'transaction': transaction_details
        }), 200
    except Exception as e:
        return jsonify({'error': f'Failed to delete transaction: {str(e)}'}), 500


@app.route('/api/all-transactions', methods=['GET'])
def get_all_transactions():
    """Get all transactions with product information"""
    try:
        db = get_db()
        cursor = db.cursor()
        
        query = """
            SELECT 
                t.id, t.product_id, t.type, t.quantity_change, t.notes, t.timestamp,
                p.sku as product_sku, p.name as product_name
            FROM InventoryTransactions t
            JOIN products p ON t.product_id = p.id
            ORDER BY t.timestamp DESC
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        transactions = [dict(row) for row in rows] if rows else []
        db.close()
        
        return jsonify(transactions)
    except Exception as e:
        return jsonify({'error': f'Failed to fetch transactions: {str(e)}'}), 500


@app.route('/api/transactions/search', methods=['GET'])
def search_transactions():
    """Search transactions by product name or SKU"""
    query = request.args.get('q', '').strip()
    
    if not query:
        return jsonify([]), 200
    
    try:
        db = get_db()
        cursor = db.cursor()
        
        search_term = f"%{query}%"
        query_sql = """
            SELECT 
                t.id, t.product_id, t.type, t.quantity_change, t.notes, t.timestamp,
                p.sku as product_sku, p.name as product_name
            FROM InventoryTransactions t
            JOIN products p ON t.product_id = p.id
            WHERE p.name LIKE ? OR p.sku LIKE ?
            ORDER BY t.timestamp DESC
        """
        cursor.execute(query_sql, (search_term, search_term))
        
        rows = cursor.fetchall()
        transactions = [dict(row) for row in rows] if rows else []
        db.close()
        
        return jsonify(transactions)
    except Exception as e:
        return jsonify({'error': f'Failed to search transactions: {str(e)}'}), 500

# --- Run the App ---

if __name__ == '__main__':
    init_db()
    app.run(debug=True)