# Stock Data Streaming Server

This project implements a simple TCP server in Python that streams stock data from a CSV file to clients. Clients can connect to the server to receive real-time stock data updates.

## Getting Started

To use this project, you'll need Python installed on your system. Additionally, ensure you have Jupyter Notebook or any other PySpark environment set up for running the client.

### Prerequisites

- Python 3.x
- Jupyter Notebook (for running the client)

### Installation

1. Clone this repository to your local machine:

    ```bash
    git clone https://github.com/Tharun1718/stock-data-streaming.git
    ```

2. Navigate to the project directory:

    ```bash
    cd stock-data-streaming
    ```

### Usage

1. Start the server by running `tcp_server.py`:

    ```bash
    python tcp_server.py
    ```

   This will start the TCP server and begin streaming stock data to clients.

2. Run the client in Jupyter Notebook or any PySpark environment to connect to the server and receive the streamed data.

### Customization

- You can modify the `stock_data.csv` file to include your own stock data.
- Adjust the server host and port in `tcp_server.py` if needed.
- Extend the functionality of the client to process and visualize the streamed data according to your requirements.

## Acknowledgments

- This project was inspired by the need for real-time stock data streaming solutions.
- Thanks to the Python community for providing excellent resources and libraries for networking and data processing.
