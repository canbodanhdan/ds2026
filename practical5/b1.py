from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import threading

# Lock để tránh in chồng chéo khi nhiều thread
Lock = threading.Lock()


# =======================
# MAPPER
# =======================
def mapper(line):
    path = line.strip()
    if not path:
        return None

    return ("longest", (len(path), path))


# =======================
# REDUCER
# =======================
def reducer(key, values):
    # values: list[(length, path)]
    return max(values)


# =======================
# MAP TASK (1 THREAD / FILE)
# =======================
def map_task(filename):
    """
    Mỗi thread xử lý 1 file input
    """
    results = []

    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                # gọi hàm mapper cho mỗi dòng
                output = mapper(line)
                if output:
                    # lưu key, value vào kết quả thread
                    results.append(output)

                    key, (length, path) = output
                    with Lock:
                        # in thông tin đường dẫn và độ dài
                        print(f"[{filename}] Line {line_num:3d} | Length={length:3d} | {path}")

    except FileNotFoundError:
        with Lock:
            print(f"❌ Không tìm thấy file: {filename}")

    return results


# =======================
# MAPREDUCE (MULTITHREADING)
# =======================
# khởi tạo xử lý luồng
def mapreduce_multithreading(input_files):
    map_output = []

    # -------- MAP PHASE (parallel) --------
    # tối đa 8 threads hoặc ít hơn nếu có ít file hơn
    with ThreadPoolExecutor(max_workers=min(8, len(input_files))) as executor:
        # khởi chạy các task map song song
        futures = [executor.submit(map_task, f) for f in input_files]

        # lấy kết quả khi các thread hoàn thành
        for future in as_completed(futures):
            # gom kết quả mapper từ tất cả thread
            map_output.extend(future.result())

    if not map_output:
        print("❌ Không có dữ liệu hợp lệ")
        return

    # -------- SHUFFLE PHASE --------
    shuffled = defaultdict(list)
    # gom tất cả theo key (longest)
    for key, value in map_output:
        shuffled[key].append(value)

    # -------- REDUCE PHASE --------
    # xử lý từng key
    for key, values in shuffled.items():
        # tìm longest path
        max_length, longest_path = reducer(key, values)

        
        print(f"Độ dài lớn nhất: {max_length} ký tự")
        print("Đường dẫn dài nhất:")
        print(f"  {longest_path}")


# =======================
# MAIN
# =======================
def main():
    if len(sys.argv) < 2:
        print("❌ Cần ít nhất 1 file input")
        sys.exit(1)

    input_files = sys.argv[1:]
    mapreduce_multithreading(input_files)


if __name__ == "__main__":
    main()
