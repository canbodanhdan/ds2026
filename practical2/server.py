import grpc
from concurrent import futures
import os
import sys

# Thêm đường dẫn generated vào sys.path để import được module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'generated'))

import file_transfer_pb2
import file_transfer_pb2_grpc

# Cấu hình server
HOST = '127.0.0.1'
PORT = 50051

class FileTransferServicer(file_transfer_pb2_grpc.FileTransferServiceServicer):
    """
    Servicer class implement RPC methods được định nghĩa trong .proto file
    """
    
    def UploadFile(self, request_iterator, context):
        """
        RPC method để nhận file từ client
        request_iterator: stream các FileChunk từ client
        context: gRPC context
        Returns: UploadResponse
        """
        filename = None
        received_data = bytearray()
        
        try:
            # Nhận từng chunk từ client stream
            for chunk in request_iterator:
                if filename is None:
                    filename = chunk.filename
                    print(f"[SERVER] Receiving file: {filename}")
                
                # Gộp dữ liệu từ các chunks
                received_data.extend(chunk.content)
                
                # Kiểm tra xem đã nhận hết chưa
                if chunk.is_last:
                    print(f"[SERVER] Received last chunk")
                    break
            
            # Lưu file với prefix "received_"
            if filename:
                output_filename = f"received_{filename}"
                output_path = os.path.join(os.path.dirname(__file__), output_filename)
                
                # Ghi dữ liệu vào file
                with open(output_path, 'wb') as f:
                    f.write(received_data)
                
                print(f"[SERVER] File saved successfully: {output_filename}")
                print(f"[SERVER] Total bytes received: {len(received_data)}")
                
                # Trả về response thành công
                return file_transfer_pb2.UploadResponse(
                    success=True,
                    message=f"File {filename} uploaded successfully ({len(received_data)} bytes)"
                )
            else:
                return file_transfer_pb2.UploadResponse(
                    success=False,
                    message="No filename received"
                )
                
        except Exception as e:
            print(f"[SERVER] Error: {e}")
            return file_transfer_pb2.UploadResponse(
                success=False,
                message=f"Error: {str(e)}"
            )

def serve():
    """
    Khởi động gRPC server
    """
    # Tạo gRPC server với thread pool
    # ThreadPoolExecutor quản lý các threads để xử lý requests đồng thời
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Đăng ký servicer với server
    # add_FileTransferServiceServicer_to_server được generate tự động từ .proto
    file_transfer_pb2_grpc.add_FileTransferServiceServicer_to_server(
        FileTransferServicer(), server
    )
    
    # Bind server với địa chỉ và port
    server.add_insecure_port(f'{HOST}:{PORT}')
    
    # Start server
    server.start()
    print(f"[SERVER] gRPC Server started on {HOST}:{PORT}")
    print("[SERVER] Waiting for clients...")
    
    try:
        # Giữ server chạy
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\n[SERVER] Shutting down server...")
        server.stop(0)

if __name__ == '__main__':
    serve()
