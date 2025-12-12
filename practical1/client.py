import socket
import os

# Cấu hình kết nối đến server
HOST = '127.0.0.1'  # Địa chỉ server
PORT = 65432        # Port của server

def send_file(filename):
    # Kiểm tra file có tồn tại không
    if not os.path.exists(filename):
        print(f"[CLIENT] Error: File '{filename}' does not exist!")
        return
    
    # Tạo socket TCP/IP
    # socket.AF_INET: sử dụng IPv4
    # socket.SOCK_STREAM: sử dụng TCP
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        # Kết nối đến server
        # connect() thiết lập kết nối TCP với server
        client_socket.connect((HOST, PORT))
        print(f"[CLIENT] Connected to server {HOST}:{PORT}")
        
        # Gửi tên file đến server
        # encode() chuyển string thành bytes
        client_socket.send(os.path.basename(filename).encode('utf-8'))
        print(f"[CLIENT] Sending file: {filename}")
        
        # Mở file và gửi nội dung
        with open(filename, 'rb') as f:
            # Đọc và gửi file theo từng chunk 4096 bytes
            while True:
                data = f.read(4096)
                if not data:
                    # Nếu hết dữ liệu, thoát vòng lặp
                    break
                # send() gửi dữ liệu qua socket
                client_socket.send(data)
        
        print("[CLIENT] File sent successfully!")
        
    except ConnectionRefusedError:
        print("[CLIENT] Error: Cannot connect to server. Make sure the server is running.")
    except Exception as e:
        print(f"[CLIENT] Error: {e}")
    finally:
        # Đóng kết nối
        # close() đóng socket và giải phóng tài nguyên
        client_socket.close()
        print("[CLIENT] Connection closed")

if __name__ == "__main__":
    # File cần gửi
    file_to_send = "test_file.txt"
    send_file(file_to_send)
