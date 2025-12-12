import grpc
import os
import sys

# Thêm đường dẫn generated vào sys.path để import được module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'generated'))

import file_transfer_pb2
import file_transfer_pb2_grpc

# Cấu hình kết nối đến server
HOST = '127.0.0.1'
PORT = 50051
CHUNK_SIZE = 4096  # Kích thước mỗi chunk (4KB)

def generate_file_chunks(filename):
    """
    Generator function để đọc file và tạo stream các FileChunk
    Đây là client-side streaming - client gửi nhiều messages đến server
    """
    # Kiểm tra file có tồn tại không
    if not os.path.exists(filename):
        print(f"[CLIENT] Error: File '{filename}' does not exist!")
        return
    
    file_basename = os.path.basename(filename)
    file_size = os.path.getsize(filename)
    print(f"[CLIENT] Preparing to send file: {file_basename} ({file_size} bytes)")
    
    # Đọc file theo chunks và yield từng chunk
    with open(filename, 'rb') as f:
        chunk_number = 0
        while True:
            # Đọc một chunk dữ liệu
            chunk_data = f.read(CHUNK_SIZE)
            
            if not chunk_data:
                # Hết dữ liệu, gửi chunk cuối cùng (empty) với flag is_last=True
                yield file_transfer_pb2.FileChunk(
                    filename=file_basename,
                    content=b'',
                    is_last=True
                )
                break
            
            chunk_number += 1
            print(f"[CLIENT] Sending chunk {chunk_number} ({len(chunk_data)} bytes)")
            
            # Tạo FileChunk message và yield
            yield file_transfer_pb2.FileChunk(
                filename=file_basename,
                content=chunk_data,
                is_last=False
            )

def upload_file(filename):
    """
    Upload file đến server sử dụng gRPC
    """
    # Tạo gRPC channel để kết nối đến server
    # insecure_channel: kết nối không mã hóa (cho development)
    with grpc.insecure_channel(f'{HOST}:{PORT}') as channel:
        # Tạo stub (client) từ channel
        # Stub cung cấp các RPC methods để gọi
        stub = file_transfer_pb2_grpc.FileTransferServiceStub(channel)
        
        try:
            print(f"[CLIENT] Connecting to server {HOST}:{PORT}")
            
            # Gọi RPC method UploadFile với stream chunks
            # generate_file_chunks() trả về iterator của FileChunk messages
            response = stub.UploadFile(generate_file_chunks(filename))
            
            # Xử lý response từ server
            if response.success:
                print(f"[CLIENT] Success: {response.message}")
            else:
                print(f"[CLIENT] Failed: {response.message}")
                
        except grpc.RpcError as e:
            print(f"[CLIENT] RPC Error: {e.code()} - {e.details()}")
        except Exception as e:
            print(f"[CLIENT] Error: {e}")

if __name__ == '__main__':
    # File cần gửi
    file_to_send = "test_file.txt"
    upload_file(file_to_send)
