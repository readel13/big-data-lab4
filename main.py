from lib import establish_connection, process_data

tables_ordered = ['accounts', 'products', 'transactions']

def main():
    conn = establish_connection()
    [process_data(table, conn) for table in tables_ordered]
    conn.close()
    

if __name__ == "__main__":
    main()
