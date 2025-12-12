import threading
import sys
from collections import defaultdict

NUM_MAPPERS = 3  # Số luồng xử lý song song

global_results = {} # Dictionary lưu kết quả
global_count = 0

# Khi một luồng đang ghi dữ liệu, các luồng khác phải đợi
lock = threading.Lock()


def mapper(args):

    thread_id = args['id']
    text_chunk = args['chunk']
    
    # BƯỚC 1: Đếm từ cục bộ (mỗi luồng đếm riêng)
    local_counts = defaultdict(int)
    
    # Tách văn bản thành danh sách các từ (phân cách bởi khoảng trắng)
    words = text_chunk.split()
    
    # Duyệt qua từng từ và tăng số đếm lên 1
    for word in words:
        local_counts[word] += 1  # Nếu từ chưa tồn tại, defaultdict tự tạo với giá trị 0
    
    print(f"[Mapper {thread_id}] Found {len(local_counts)} unique words.")
    
    # BƯỚC 2: Gộp kết quả vào bộ nhớ dùng chung (CRITICAL SECTION)
    global global_results, global_count
    
    lock.acquire()  # Chỉ cho 1 luồng vào tại một thời điểm
    try:
        # Gộp kết quả từ local_counts vào global_results
        for word, count in local_counts.items():
            if word in global_results:
                global_results[word] += count
            else:
                global_results[word] = count
                global_count += 1
    finally: # Đảm bảo luôn thực thi kể cả lỗi xảy ra
        lock.release()


def main():
    # BƯỚC 1: Đọc file input
    try:
        with open("input.txt", "r") as fp:
            full_text = fp.read()
    except FileNotFoundError:
        print("Cannot open file input.txt", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error reading file: {e}", file=sys.stderr)
        return 1
    
    # BƯỚC 2: Chia văn bản và tạo các luồng
    threads = []  # Danh sách chứa các thread object
    words = full_text.split()
    
    # Tính số từ mỗi luồng sẽ xử lý (chia đều)
    words_per_chunk = len(words) // NUM_MAPPERS
    
    print("--- STARTING MAPREDUCE WITH PYTHON MULTITHREADING ---")
    
    # Tạo NUM_MAPPERS luồng, mỗi luồng xử lý một phần văn bản
    for i in range(NUM_MAPPERS):
        start_idx = i * words_per_chunk
        # Luồng cuối cùng nhận tất cả từ còn lại (kể cả phần dư)
        if i == NUM_MAPPERS - 1:
            chunk_words = words[start_idx:]
        else:
            chunk_words = words[start_idx:start_idx + words_per_chunk]
        # Ghép list từ thành chuỗi văn bản
        chunk = ' '.join(chunk_words)
        args = {'id': i + 1, 'chunk': chunk}
        
        # Tạo thread mới
        thread = threading.Thread(target=mapper, args=(args,))
        threads.append(thread)
        thread.start()
    
    # BƯỚC 3: Đợi tất cả luồng hoàn thành (ĐỒNG BỘ HÓA)
    for thread in threads:
        thread.join() #Đảm bảo tất cả thread xử lý xong trước khi in kết quả
    
    # BƯỚC 4: In kết quả
    print("\n--- FINAL RESULTS ---")

    for word, count in sorted(global_results.items()):
        print(f"{word}: {count}")
    
    return 0

# Chỉ chạy khi file này được thực thi trực tiếp
# (không chạy khi file này được import vào file khác)
if __name__ == "__main__":
    sys.exit(main())

