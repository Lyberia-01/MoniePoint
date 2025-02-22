#This is my solution using SQLite and Python
import sqlite3
import os
from collections import defaultdict

conn = sqlite3.connect('monieshop_transactions.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    staf_id INTEGER,
    trans_time TEXT,
    sales_amount REAL
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    FOREIGN KEY (transaction_id) REFERENCES transactions(id)
)
''')


def processTransaction(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            # I decided to just split with the ',' instead of regex
            parts = line.strip().split(',')
            #print(parts)
            if len(parts) == 4:
                staf_id = int(parts[0])
                trans_time = parts[1]
                prods_sold = parts[2].strip('[]')
                sales_amount = float(parts[3])

                #Sending this to the transactions table
                cursor.execute('''
                    INSERT INTO transactions (staf_id, trans_time, sales_amount)
                    VALUES (?, ?, ?)
                ''', (staf_id, trans_time, sales_amount))
                transaction_id = cursor.lastrowid

                # Sending this to the products table, split this by the '|' into a list
                for product in prods_sold.split('|'):
                    product_id, quantity = map(int, product.split(':'))
                    cursor.execute('''
                        INSERT INTO products (transaction_id, product_id, quantity)
                        VALUES (?, ?, ?)
                    ''', (transaction_id, product_id, quantity))

#So this will process all transaction files in the same folder as my python file
for filename in os.listdir('.'):
    #I used a test case for 2025 so it starts with '2025' and ends with you know? lol (.txt)
    if filename.startswith('2025-') and filename.endswith('.txt'):
        processTransaction(filename)

conn.commit()


def highestSalesVolume():
    cursor.execute('''
    SELECT DATE(trans_time) AS date, COUNT(*) AS transaction_count
    FROM transactions
    GROUP BY date
    ORDER BY transaction_count DESC
    LIMIT 1
    ''')
    highest_sales_volume_day = cursor.fetchone()
    if highest_sales_volume_day:
        print(f"Highest sales volume in a day: {highest_sales_volume_day[0]} with {highest_sales_volume_day[1]} transactions")
    else:
        print('No Sales volume on to the next.')



def highestSalesValue():
    cursor.execute('''
    SELECT DATE(trans_time) AS date, SUM(sales_amount) AS total_sales
    FROM transactions
    GROUP BY date
    ORDER BY total_sales DESC
    LIMIT 1
    ''')
    highest_sales_value_day = cursor.fetchone()
    if highest_sales_value_day:
      print(f"Highest sales value in a day: {highest_sales_value_day[0]} with total sales of {highest_sales_value_day[1]:.2f}")
    else:
      print('No sales value for the day Thank you.')



def mostSoldProduct():
    cursor.execute('''
    SELECT product_id, SUM(quantity) AS total_quantity
    FROM products
    GROUP BY product_id
    ORDER BY total_quantity DESC
    LIMIT 1
    ''')
    most_sold_product = cursor.fetchone()
    if most_sold_product:
        print(f"Most sold product ID by volume: {most_sold_product[0]} with {most_sold_product[1]} units sold")
    else:
        print('No sold Product. Thank you')


def highestStaffSales():
    cursor.execute('''
    SELECT strftime('%Y-%m', trans_time) AS month, staf_id, SUM(sales_amount) AS total_sales
    FROM transactions
    GROUP BY month, staf_id
    ORDER BY month, total_sales DESC
    ''')
    monthly_sales = defaultdict(lambda: (None, 0))
    for row in cursor.fetchall():
        month, staf_id, total_sales = row
        if total_sales > monthly_sales[month][1]:
            monthly_sales[month] = (staf_id, total_sales)

    print("Highest sales staff ID for each month:")
    for month, (staf_id, total_sales) in monthly_sales.items():
        print(f"{month}: Sales Staff ID {staf_id} with total sales of {total_sales:.2f}")



def highestHour():
    cursor.execute('''
    SELECT strftime('%H', trans_time) AS hour, COUNT(*) AS transaction_count
    FROM transactions
    GROUP BY hour
    ORDER BY transaction_count DESC
    LIMIT 1
    ''')
    peak_hour = cursor.fetchone()
    if peak_hour:
      print(f"Highest hour of the day by average transaction volume: {peak_hour[0]}:00 with {peak_hour[1]} transactions on average")
    else:
      print('No peak hour for the Day')


#Solution to Question 1. what is the Highest sales volume in a day
highestSalesVolume()
#Solution to Question 2. What is Highest sales value in a day
highestSalesValue()
#Solution to Question 3. What is Most sold product ID by volume
mostSoldProduct()
#Solution to Question 4. What is the Highest sales staff ID for each month
highestStaffSales()
#Solution to Question 5. What is Highest hour of the day by average transaction volume
highestHour()

def clearDb():
    cursor.execute('DELETE FROM transactions')
    cursor.execute('DELETE FROM products')
    conn.commit()
    #I used this approach to keep the table structure.
    print('All data has been cleared from db')

#I decided to clear the DB Afterwards.
clearDb()

# The End Thank you.
conn.close()
