import socket
import os

# Cấu hình server
HOST = '127.0.0.1'  # Địa chỉ localhost
PORT = 65432        # Port để lắng nghe

def start_server():
    # Tạo socket TCP/IP
    # socket.AF_INET: sử dụng IPv4
    # socket.SOCK_STREAM: sử dụng TCP
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Cho phép tái sử dụng địa chỉ ngay lập tức
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    # Gắn socket với địa chỉ và port
    # bind() liên kết socket với địa chỉ cụ thể
    server_socket.bind((HOST, PORT))
    
    # Lắng nghe kết nối đến
    # listen(1) cho phép tối đa 1 kết nối chờ trong hàng đợi
    server_socket.listen(1)
    print(f"[SERVER] Listening on {HOST}:{PORT}")
    
    try:
        while True:
            # Chấp nhận kết nối từ client
            # accept() trả về socket mới cho kết nối và địa chỉ của client
            client_socket, client_address = server_socket.accept()
            print(f"[SERVER] Connection from {client_address}")
            
            # Nhận tên file (tối đa 1024 bytes)
            filename = client_socket.recv(1024).decode('utf-8')
            print(f"[SERVER] Receiving file: {filename}")
            
            # Tạo tên file mới để lưu (thêm prefix "received_")
            received_filename = f"received_{filename}"
            
            # Mở file để ghi dữ liệu nhận được
            with open(received_filename, 'wb') as f:
                print("[SERVER] Receiving data...")
                while True:
                    # Nhận dữ liệu theo từng chunk 4096 bytes
                    # recv() đọc dữ liệu từ socket
                    data = client_socket.recv(4096)
                    if not data:
                        # Nếu không còn dữ liệu, thoát vòng lặp
                        break
                    # Ghi dữ liệu vào file
                    f.write(data)
            
            print(f"[SERVER] File received successfully: {received_filename}")
            
            # Đóng kết nối với client
            client_socket.close()
            print("[SERVER] Connection closed\n")
            
    except KeyboardInterrupt:
        print("\n[SERVER] Shutting down server...")
    finally:
        # Đóng server socket
        server_socket.close()

if __name__ == "__main__":
    start_server()
