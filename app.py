import sqlite3
from flask import Flask, render_template, request, jsonify
import datetime
import pandas as pd
import os
import sys
import openpyxl
import re
from dateutil import parser as date_parser

# --- Helper function to parse dates from Excel ---

def parse_excel_date(date_value):
    """Parse various date formats from Excel cells"""
    if pd.isna(date_value):
        return None
        
    if isinstance(date_value, datetime.datetime):
        return date_value.date()
        
    if isinstance(date_value, str):
        # Handle various date formats
        date_value = date_value.strip()
        if not date_value:
            return None
            
        # Try common formats first
        formats = [
            '%d.%m.%y',    # 2.7.25
            '%d/%m/%y',    # 18/7/25
            '%d.%m.%Y',    # 19.07.2025
            '%d/%m/%Y',    # 18/07/2025
        ]
        
        for fmt in formats:
            try:
                return datetime.datetime.strptime(date_value, fmt).date()
            except ValueError:
                continue
                
        # Try dateutil parser as fallback
        try:
            parsed_date = date_parser.parse(date_value, dayfirst=True)
            return parsed_date.date()
        except:
            pass
            
    return None

# --- App & Database Setup ---

app = Flask(__name__)
DB_NAME = 'inventory.db'

def get_db():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
        # Create products table with new columns
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                unit TEXT,
                opening INTEGER DEFAULT 0,
                receipt INTEGER DEFAULT 0,
                issue INTEGER DEFAULT 0,
                balance INTEGER DEFAULT 0
            )
        ''')
        
        # Add new columns to existing products table if they don't exist
        try:
            cursor.execute('ALTER TABLE products ADD COLUMN opening INTEGER DEFAULT 0')
        except sqlite3.OperationalError:
            pass  # Column already exists
            
        try:
            cursor.execute('ALTER TABLE products ADD COLUMN receipt INTEGER DEFAULT 0')
        except sqlite3.OperationalError:
            pass  # Column already exists
            
        try:
            cursor.execute('ALTER TABLE products ADD COLUMN issue INTEGER DEFAULT 0')
        except sqlite3.OperationalError:
            pass  # Column already exists
            
        try:
            cursor.execute('ALTER TABLE products ADD COLUMN balance INTEGER DEFAULT 0')
        except sqlite3.OperationalError:
            pass  # Column already exists
        
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
    try:
        db = get_db()
        cursor = db.cursor()
        query = """
            SELECT 
                p.id, p.sku, p.name, p.unit, p.opening, p.receipt, p.issue, p.balance
            FROM products p
            ORDER BY p.name;
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        products = []
        for row in rows:
            product = dict(row)
            products.append(product)
            
        db.close()
        return jsonify(products)
    except Exception as e:
        print(f"Error in get_products: {e}")
        import traceback
        traceback.print_exc()
        # Return empty array instead of error
        return jsonify([])

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
    
    # Get opening, receipt, issue values if provided
    opening = int(new_product.get('opening', 0))
    receipt = int(new_product.get('receipt', 0))
    issue = int(new_product.get('issue', 0))

    db = get_db()
    cursor = db.cursor()
    try:
        cursor.execute("INSERT INTO products (sku, name, unit, opening, receipt, issue) VALUES (?, ?, ?, ?, ?, ?)", (sku, name, unit, opening, receipt, issue))
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
            p.id, p.sku, p.name, p.unit, p.opening, p.receipt, p.issue, p.balance
        FROM products p
        WHERE p.name LIKE ? OR p.sku LIKE ?
        ORDER BY p.name
    """, (search_term, search_term))
    
    rows = cursor.fetchall()
    products = [dict(row) for row in rows] if rows else []
    db.close()
    
    return jsonify(products)


@app.route('/api/stock-issues', methods=['GET'])
def get_stock_issues():
    """Get stock issues data grouped by date from database"""
    try:
        db = get_db()
        cursor = db.cursor()
        
        # Get all sale transactions with product information
        # Also include initial transactions as they represent the opening stock
        query = """
            SELECT 
                t.id, t.product_id, t.type, t.quantity_change, t.notes, t.timestamp,
                p.sku, p.name, p.unit
            FROM InventoryTransactions t
            JOIN products p ON t.product_id = p.id
            WHERE t.type = 'sale' OR t.type = 'initial'
            ORDER BY t.timestamp DESC
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Group by date and format data
        issues_by_date = []
        for row in rows:
            # Extract date from timestamp (format: YYYY-MM-DD HH:MM:SS)
            timestamp = row['timestamp']
            if timestamp:
                # Parse timestamp and format as DD.MM.YYYY
                date_obj = datetime.datetime.strptime(timestamp.split(' ')[0], '%Y-%m-%d')
                formatted_date = date_obj.strftime('%d.%m.%Y')
            else:
                formatted_date = 'Unknown Date'
            
            # For stock issues, we want to show negative quantities as positive values
            # Initial transactions are positive (opening stock), but for issues we might want to show them as well
            # Let's include initial transactions as they represent the opening stock (which can be considered as "available for issue")
            issues_by_date.append({
                'date': formatted_date,
                's_no': row['product_id'],  # Using product_id as S.NO. for database consistency
                'description': row['name'],
                'unit': row['unit'],
                'quantity': abs(row['quantity_change'])  # Make it positive for display
            })
        
        db.close()
        return jsonify(issues_by_date)
    except Exception as e:
        print(f"Error in get_stock_issues: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Failed to fetch stock issues by date: {str(e)}'}), 500


@app.route('/api/stock-receipts', methods=['GET'])
def get_stock_receipts():
    """Get stock receipts data grouped by date from database"""
    try:
        db = get_db()
        cursor = db.cursor()
        
        # Get all purchase transactions with product information
        # Also include initial transactions as they represent the opening stock
        query = """
            SELECT 
                t.id, t.product_id, t.type, t.quantity_change, t.notes, t.timestamp,
                p.sku, p.name, p.unit
            FROM InventoryTransactions t
            JOIN products p ON t.product_id = p.id
            WHERE t.type = 'purchase' OR t.type = 'initial'
            ORDER BY t.timestamp DESC
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Group by date and format data
        receipts_by_date = []
        for row in rows:
            # Extract date from timestamp (format: YYYY-MM-DD HH:MM:SS)
            timestamp = row['timestamp']
            if timestamp:
                # Parse timestamp and format as DD.MM.YYYY
                date_obj = datetime.datetime.strptime(timestamp.split(' ')[0], '%Y-%m-%d')
                formatted_date = date_obj.strftime('%d.%m.%Y')
            else:
                formatted_date = 'Unknown Date'
            
            # For stock receipts, we want to show positive quantities (inflows)
            # Initial transactions are positive (opening stock), which can be considered as receipts
            # Purchase transactions are also positive
            receipts_by_date.append({
                'date': formatted_date,
                's_no': row['product_id'],  # Using product_id as S.NO. for database consistency
                'description': row['name'],
                'unit': row['unit'],
                'quantity': row['quantity_change']  # Already positive for purchases and initial
            })
        
        db.close()
        return jsonify(receipts_by_date)
    except Exception as e:
        print(f"Error in get_stock_receipts: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Failed to fetch stock receipts by date: {str(e)}'}), 500


@app.route('/api/stock-issues-date-range', methods=['GET'])
def get_stock_issues_date_range():
    """Get stock issues data within a date range from database"""
    try:
        # Get date parameters from query string
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({'error': 'Start date and end date are required'}), 400
        
        db = get_db()
        cursor = db.cursor()
        
        # Get sale transactions within date range with product information
        query = """
            SELECT 
                t.id, t.product_id, t.type, t.quantity_change, t.notes, t.timestamp,
                p.sku, p.name, p.unit
            FROM InventoryTransactions t
            JOIN products p ON t.product_id = p.id
            WHERE t.type = 'sale' 
            AND DATE(t.timestamp) BETWEEN ? AND ?
            ORDER BY t.timestamp DESC
        """
        cursor.execute(query, (start_date, end_date))
        rows = cursor.fetchall()
        
        # Format data
        issues_in_range = []
        for row in rows:
            # Extract date from timestamp (format: YYYY-MM-DD HH:MM:SS)
            timestamp = row['timestamp']
            if timestamp:
                # Parse timestamp and format as DD.MM.YYYY
                date_obj = datetime.datetime.strptime(timestamp.split(' ')[0], '%Y-%m-%d')
                formatted_date = date_obj.strftime('%d.%m.%Y')
            else:
                formatted_date = 'Unknown Date'
            
            issues_in_range.append({
                'date': formatted_date,
                's_no': row['product_id'],  # Using product_id as S.NO. for database consistency
                'description': row['name'],
                'unit': row['unit'],
                'quantity': abs(row['quantity_change'])  # Make it positive for display
            })
        
        db.close()
        return jsonify(issues_in_range)
    except Exception as e:
        print(f"Error in get_stock_issues_date_range: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Failed to fetch stock issues by date range: {str(e)}'}), 500


@app.route('/api/stock-issues-date-range-horizontal', methods=['GET'])
def get_stock_issues_date_range_horizontal():
    """Get stock issues data within a date range from database in horizontal format"""
    try:
        # Get date parameters from query string
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({'error': 'Start date and end date are required'}), 400
        
        db = get_db()
        cursor = db.cursor()
        
        # Get sale transactions within date range with product information
        query = """
            SELECT 
                t.id, t.product_id, t.type, t.quantity_change, t.notes, t.timestamp,
                p.sku, p.name, p.unit
            FROM InventoryTransactions t
            JOIN products p ON t.product_id = p.id
            WHERE t.type = 'sale' 
            AND DATE(t.timestamp) BETWEEN ? AND ?
            ORDER BY t.timestamp DESC
        """
        cursor.execute(query, (start_date, end_date))
        rows = cursor.fetchall()
        
        # Format data
        issues_in_range = []
        for row in rows:
            # Extract date from timestamp (format: YYYY-MM-DD HH:MM:SS)
            timestamp = row['timestamp']
            if timestamp:
                # Parse timestamp and format as DD.MM.YYYY
                date_obj = datetime.datetime.strptime(timestamp.split(' ')[0], '%Y-%m-%d')
                formatted_date = date_obj.strftime('%d.%m.%Y')
            else:
                formatted_date = 'Unknown Date'
            
            issues_in_range.append({
                'date': formatted_date,
                's_no': row['product_id'],  # Using product_id as S.NO. for database consistency
                'description': row['name'],
                'unit': row['unit'],
                'quantity': abs(row['quantity_change'])  # Make it positive for display
            })
        
        db.close()
        return jsonify(issues_in_range)
    except Exception as e:
        print(f"Error in get_stock_issues_date_range_horizontal: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Failed to fetch stock issues by date range: {str(e)}'}), 500


@app.route('/api/stock-receipts-date-range-horizontal', methods=['GET'])
def get_stock_receipts_date_range_horizontal():
    """Get stock receipts data within a date range from database in horizontal format"""
    try:
        # Get date parameters from query string
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({'error': 'Start date and end date are required'}), 400
        
        db = get_db()
        cursor = db.cursor()
        
        # Get purchase transactions within date range with product information
        query = """
            SELECT 
                t.id, t.product_id, t.type, t.quantity_change, t.notes, t.timestamp,
                p.sku, p.name, p.unit
            FROM InventoryTransactions t
            JOIN products p ON t.product_id = p.id
            WHERE t.type = 'purchase' 
            AND DATE(t.timestamp) BETWEEN ? AND ?
            ORDER BY t.timestamp DESC
        """
        cursor.execute(query, (start_date, end_date))
        rows = cursor.fetchall()
        
        # Format data
        receipts_in_range = []
        for row in rows:
            # Extract date from timestamp (format: YYYY-MM-DD HH:MM:SS)
            timestamp = row['timestamp']
            if timestamp:
                # Parse timestamp and format as DD.MM.YYYY
                date_obj = datetime.datetime.strptime(timestamp.split(' ')[0], '%Y-%m-%d')
                formatted_date = date_obj.strftime('%d.%m.%Y')
            else:
                formatted_date = 'Unknown Date'
            
            receipts_in_range.append({
                'date': formatted_date,
                's_no': row['product_id'],  # Using product_id as S.NO. for database consistency
                'description': row['name'],
                'unit': row['unit'],
                'quantity': row['quantity_change']  # Already positive for purchases
            })
        
        db.close()
        return jsonify(receipts_in_range)
    except Exception as e:
        print(f"Error in get_stock_receipts_date_range_horizontal: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Failed to fetch stock receipts by date range: {str(e)}'}), 500


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
    
    # 1. Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File not found at {os.path.abspath(file_path)}", file=sys.stderr)
        return jsonify({'error': f"File '{file_path}' not found. Make sure it's in the same folder as app.py."}), 404

    # 2. Try to read all sheets
    try:
        # Read all three sheets
        df_balance = pd.read_excel(file_path, sheet_name='BALANCE')
        df_issue = pd.read_excel(file_path, sheet_name='ISSUE')
        df_receipt = pd.read_excel(file_path, sheet_name='RECEIPT')
    except Exception as e:
        print(f"Error reading Excel file: {e}", file=sys.stderr)
        return jsonify({'error': f"Could not read file: {e}. Is 'openpyxl' installed? Are the sheets named correctly?"}), 500

    # 3. Clean the BALANCE data
    try:
        df_balance['OPENING'] = pd.to_numeric(df_balance['OPENING'], errors='coerce')
        valid_balance_df = df_balance[
            ~df_balance['DESCRIPTION'].isin(['0', 0]) &
            df_balance['DESCRIPTION'].notna() &
            df_balance['OPENING'].notna()
        ].copy()
        # Create a new DataFrame with unique S.NO. values to avoid linter issues
        valid_balance_df = valid_balance_df.groupby('S.NO.').first().reset_index()
    except Exception as e:
        print(f"Error cleaning BALANCE data: {e}", file=sys.stderr)
        return jsonify({'error': f"Error during BALANCE data cleaning: {e}"}), 500

    db = get_db()
    cursor = db.cursor()

    # 4. Clear existing data
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

    # 5. Import products from BALANCE sheet
    items_added_count = 0
    product_mapping = {}  # Map S.NO. to product_id
    
    for index, row in valid_balance_df.iterrows():
        sku = ""
        name = ""
        try:
            name = str(row['DESCRIPTION'])
            s_no = row['S.NO.']
            sku = f"ITEM-{s_no}" 
            unit = str(row['UNIT'])
            opening_stock = int(row['OPENING']) if pd.notna(row['OPENING']) else 0
            receipt = int(row['RECEIPT']) if pd.notna(row['RECEIPT']) else 0
            issue = int(row['ISSUE']) if pd.notna(row['ISSUE']) else 0
            # Read balance from Excel file
            balance = int(row['BALANCE']) if pd.notna(row['BALANCE']) else 0

            cursor.execute("INSERT INTO products (sku, name, unit, opening, receipt, issue, balance) VALUES (?, ?, ?, ?, ?, ?, ?)", (sku, name, unit, opening_stock, receipt, issue, balance))
            product_id = cursor.lastrowid
            product_mapping[s_no] = product_id  # Store mapping for later use
            
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

    # 6. Import transactions from ISSUE sheet (stock issues/sales)
    try:
        # Process ISSUE sheet - these are stock issues (negative quantities)
        for index, row in df_issue.iterrows():
            if pd.isna(row['S.NO.']) or pd.isna(row['DESCRIPTION']):
                continue
                
            s_no = row['S.NO.']
            if s_no not in product_mapping:
                continue  # Skip if product not found
                
            product_id = product_mapping[s_no]
            description = str(row['DESCRIPTION'])
            
            # Process each date column in the ISSUE sheet
            for col_name in df_issue.columns[3:]:  # Skip first 3 columns (S.NO., DESCRIPTION, UNIT)
                quantity = row[col_name]
                if pd.isna(quantity) or quantity == 0:
                    continue
                    
                try:
                    # Try to convert to float, skip if it's not a number
                    quantity = float(quantity)
                    if quantity <= 0:
                        continue
                        
                    # Parse the date from column name
                    transaction_date = parse_excel_date(col_name)
                    if not transaction_date:
                        continue
                        
                    # Format date for database
                    date_str = transaction_date.strftime('%Y-%m-%d')
                    
                    # Create sale transaction (negative quantity for issues)
                    notes = f"Stock issue on {transaction_date.strftime('%d.%m.%Y')}"
                    cursor.execute(
                        "INSERT INTO InventoryTransactions (product_id, type, quantity_change, notes, timestamp) VALUES (?, 'sale', ?, ?, ?)",
                        (product_id, -int(quantity), notes, date_str)
                    )
                except (ValueError, TypeError) as e:
                    # Skip non-numeric values like 'TOTAL'
                    print(f"Skipping non-numeric value in ISSUE sheet for product {s_no}, column {col_name}: {quantity}")
                    continue
                except Exception as e:
                    print(f"Warning: Could not process ISSUE transaction for product {s_no}, date {col_name}: {e}")
                    continue
                    
    except Exception as e:
        print(f"Warning: Error processing ISSUE sheet: {e}")

    # 7. Import transactions from RECEIPT sheet (stock receipts/purchases)
    try:
        # Process RECEIPT sheet - these are stock receipts (positive quantities)
        for index, row in df_receipt.iterrows():
            if pd.isna(row['S.NO.']) or pd.isna(row['DESCRIPTION']):
                continue
                
            s_no = row['S.NO.']
            if s_no not in product_mapping:
                continue  # Skip if product not found
                
            product_id = product_mapping[s_no]
            description = str(row['DESCRIPTION'])
            
            # Process each date column in the RECEIPT sheet
            for col_name in df_receipt.columns[3:]:  # Skip first 3 columns (S.NO., DESCRIPTION, UNIT)
                quantity = row[col_name]
                if pd.isna(quantity) or quantity == 0:
                    continue
                    
                try:
                    # Try to convert to float, skip if it's not a number
                    quantity = float(quantity)
                    if quantity <= 0:
                        continue
                        
                    # Parse the date from column name
                    transaction_date = parse_excel_date(col_name)
                    if not transaction_date:
                        continue
                        
                    # Format date for database
                    date_str = transaction_date.strftime('%Y-%m-%d')
                    
                    # Create purchase transaction (positive quantity for receipts)
                    notes = f"Stock receipt on {transaction_date.strftime('%d.%m.%Y')}"
                    cursor.execute(
                        "INSERT INTO InventoryTransactions (product_id, type, quantity_change, notes, timestamp) VALUES (?, 'purchase', ?, ?, ?)",
                        (product_id, int(quantity), notes, date_str)
                    )
                except (ValueError, TypeError) as e:
                    # Skip non-numeric values like 'TOTAL'
                    print(f"Skipping non-numeric value in RECEIPT sheet for product {s_no}, column {col_name}: {quantity}")
                    continue
                except Exception as e:
                    print(f"Warning: Could not process RECEIPT transaction for product {s_no}, date {col_name}: {e}")
                    continue
                    
    except Exception as e:
        print(f"Warning: Error processing RECEIPT sheet: {e}")

    db.commit()
    if 'db' in locals() and db:
        db.close()

    return jsonify({
        'message': f"Import successful! Added {items_added_count} products from 'STOCK.xlsx' with transaction data."
    })


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


@app.route('/api/transactions/filter', methods=['GET'])
def filter_transactions():
    """Filter transactions by search term and/or date range"""
    try:
        db = get_db()
        cursor = db.cursor()
        
        # Get query parameters
        search_query = request.args.get('q', '').strip()
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        # Base query
        base_query = """
            SELECT 
                t.id, t.product_id, t.type, t.quantity_change, t.notes, t.timestamp,
                p.sku as product_sku, p.name as product_name
            FROM InventoryTransactions t
            JOIN products p ON t.product_id = p.id
        """
        
        # Build WHERE conditions
        conditions = []
        params = []
        
        # Add search condition if provided
        if search_query:
            conditions.append("(p.name LIKE ? OR p.sku LIKE ?)")
            params.extend([f"%{search_query}%", f"%{search_query}%"])
        
        # Add date range conditions if provided
        if start_date:
            conditions.append("DATE(t.timestamp) >= ?")
            params.append(start_date)
            
        if end_date:
            conditions.append("DATE(t.timestamp) <= ?")
            params.append(end_date)
        
        # Construct full query
        query = base_query
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY t.timestamp DESC"
        
        # Execute query
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        transactions = [dict(row) for row in rows] if rows else []
        db.close()
        
        return jsonify(transactions)
    except Exception as e:
        return jsonify({'error': f'Failed to filter transactions: {str(e)}'}), 500

# --- Run the App ---

if __name__ == '__main__':
    init_db()
    app.run(debug=True)