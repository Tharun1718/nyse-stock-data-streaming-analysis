import socket   
import time     
import csv

# Defining hostname and port
HOST = 'localhost'
PORT = 9999
CSV_FILE = 'stock_data.csv'

# Reads stock data from the CSV file and returns it as a list
def read_stock_data():
    with open(CSV_FILE, 'r') as file:
        reader = csv.reader(file)
        data = list(reader)
    return data

# Generator function to continuosly yield stock data rows with a delay of 1 second
def stream_stock_data(data):
    while True:
        for row in data:
            stock_data = ','.join(row) + '\n'
            yield stock_data
            time.sleep(1)

# Setting up TCP server
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
    server_socket.bind((HOST, PORT))    # Binding server to host and port
    server_socket.listen()              # Start listening for incoming connections

    print(f"TCP server listening on {HOST}:{PORT}")

    while True:
        conn, addr = server_socket.accept()     # Accepting incoming connection
        print(f"Connected by {addr}")           # Printing address of the client

        try:
            stock_data = read_stock_data()
            data_stream = stream_stock_data(stock_data)     # Creating a data stream
            
            while True:
                conn.sendall(next(data_stream).encode())    # Sending data to the client
        except StopIteration:
            print("All data streamed")
        except KeyboardInterrupt:
            print("Server interrupted")
            break
