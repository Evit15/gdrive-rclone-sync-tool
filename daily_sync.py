import os
import datetime
import logging
import subprocess
from rclone_python import rclone
import hashlib
import json
from typing import List, Dict, Optional
import argparse

# Cấu hình toàn cục
CONFIG = {
    "MAX_TRANSFER_GB": 500,
    "LOG_DIR": "logs",
    "CACHE_DIR": "cache",
    "RCLONE_ARGS": [
        "--drive-chunk-size", "64M",
        "--tpslimit", "4",
        "--transfers", "2",
        "--log-level", "DEBUG",
        "--retries", "3",
        "--retries-sleep", "5s"
    ]
}

logger = logging.getLogger("rclone_sync")
logger.setLevel(logging.DEBUG)

def setup_logging():
    os.makedirs(CONFIG["LOG_DIR"], exist_ok=True)
    log_file = os.path.join(CONFIG["LOG_DIR"], f"daily_sync_{datetime.date.today()}.log")
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

def sanitize_path(remote_path: str) -> str:
    return hashlib.md5(remote_path.encode()).hexdigest()

def check_remote_exists(remote_path: str) -> bool:
    logger.debug(f"🔍 Kiểm tra sự tồn tại của thư mục: {remote_path}")
    try:
        result = rclone.ls(remote_path, max_depth=1)
        logger.debug(f"✅ Thư mục tồn tại: {remote_path}")
        return True
    except Exception as e:
        logger.error(f"❌ Thư mục không tồn tại hoặc lỗi khi kiểm tra {remote_path}: {str(e)}")
        return False

def get_cached_files(remote_path: str, is_source: bool = True) -> List[Dict]:
    os.makedirs(CONFIG["CACHE_DIR"], exist_ok=True)
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    wire_path = sanitize_path(remote_path)
    cache_type = 'source' if is_source else 'dest'
    cache_file = os.path.join(CONFIG["CACHE_DIR"], f"{wire_path}_{cache_type}_{today}.json")
    
    if os.path.exists(cache_file):
        logger.info(f"📦 Sử dụng cache từ: {cache_file}")
        with open(cache_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    # Kiểm tra sự tồn tại của thư mục gốc
    if not check_remote_exists(remote_path):
        if is_source:
            logger.error(f"❌ Bỏ qua đồng bộ vì thư mục nguồn không tồn tại: {remote_path}")
            return []
        else:
            logger.info(f"📁 Thư mục đích không tồn tại: {remote_path}. Trả về cache rỗng")
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump([], f, ensure_ascii=False, indent=2)
            return []
    
    logger.info(f"🔄 Tạo mới cache cho: {remote_path} ({cache_type})")
    try:
        items = rclone.ls(remote_path, max_depth=9999, args=["--hash"])
        files = [item for item in items if not item.get("IsDir", False)]
        
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(files, f, ensure_ascii=False, indent=2)
        return files
    except Exception as e:
        logger.error(f"❌ Lỗi khi liệt kê file: {str(e)}")
        return []

def get_file_hash(remote_path: str, algo: str = 'md5') -> Optional[str]:
    logger.debug(f"🔍 Đang lấy hash của {remote_path}")
    cmd = ["rclone", "hashsum", algo, remote_path]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', check=True)
        if result.stdout:
            hash_value = result.stdout.split(maxsplit=1)[0]
            logger.debug(f"✅ Hash của {remote_path}: {hash_value}")
            return hash_value
        logger.warning(f"⚠️ Không có hash trả về cho {remote_path}")
        return None
    except subprocess.CalledProcessError as e:
        logger.warning(f"⚠️ Không thể lấy hash của {remote_path}: {e.stderr}")
        return None

def is_quota_exceeded(error_msg: str) -> bool:
    keywords = ['quotaExceeded', 'userRateLimitExceeded', '403', '429', 'Rate Limit']
    return any(k.lower() in error_msg.lower() for k in keywords)

def file_exists_at_dest(dest_path: str, file_path: str) -> bool:
    file_name = os.path.basename(file_path)
    dir_path = os.path.dirname(dest_path) or ""
    
    logger.debug(f"🔍 Đang kiểm tra file tồn tại tại đích: {dest_path}")
    cmd = ["rclone", "lsf", dir_path]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', check=True)
        files = result.stdout.splitlines()
        logger.debug(f"📋 Danh sách file trong {dir_path}: {files}")
        exists = file_name in files
        logger.debug(f"✅ Kết quả kiểm tra {dest_path}: {'tồn tại' if exists else 'không tồn tại'}")
        return exists
    except subprocess.CalledProcessError as e:
        if "directory not found" in e.stderr:
            logger.info(f"📁 Thư mục đích {dir_path} chưa tồn tại")
            return False
        logger.warning(f"⚠️ Không thể kiểm tra file tồn tại tại {dest_path}: {e.stderr}")
        return False

def run_rclone_copy(src_path: str, dest_path: str) -> tuple[bool, str]:
    cmd = ["rclone", "copyto", src_path, dest_path] + CONFIG["RCLONE_ARGS"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', check=True)
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, f"Exit code {e.returncode}: {e.stderr}"

def delete_file(remote_path: str) -> bool:
    logger.debug(f"🗑️ Đang xóa file: {remote_path}")
    cmd = ["rclone", "delete", remote_path]
    try:
        subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', check=True)
        logger.debug(f"✅ Đã xóa file: {remote_path}")
        return True
    except subprocess.CalledProcessError as e:
        logger.warning(f"⚠️ Không thể xóa file {remote_path}: {e.stderr}")
        return False

def get_files_to_copy(source: str, destination: str) -> List[Dict]:
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    cache_file = os.path.join(CONFIG["CACHE_DIR"], f"sync_list_{sanitize_path(source)}_{today}.json")
    
    if os.path.exists(cache_file):
        logger.info(f"📦 Sử dụng danh sách file cần copy từ: {cache_file}")
        with open(cache_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    source_files = get_cached_files(source, is_source=True)
    if not source_files and not check_remote_exists(source):
        logger.error(f"❌ Không có file để đồng bộ từ {source}")
        return []
        
    dest_files = get_cached_files(destination, is_source=False)
    files_to_copy = []
    
    # Create dictionary for faster lookup
    dest_files_dict = {f['Path']: f for f in dest_files}
    
    for src_file in source_files:
        src_path = src_file['Path']
        src_hash = src_file.get('Hashes', {}).get('md5', '')
        
        # Check if file exists in destination
        dest_file = dest_files_dict.get(src_path)
        
        if dest_file:
            # Compare hashes if file exists in both
            dest_hash = dest_file.get('Hashes', {}).get('md5', '')
            if src_hash and dest_hash and src_hash != dest_hash:
                logger.info(f"🔄 Hash khác nhau, sẽ copy: {src_path} (src: {src_hash}, dest: {dest_hash})")
                delete_file(f"{destination}/{src_path}")
                files_to_copy.append(src_file)
        else:
            logger.info(f"📌 File chưa tồn tại ở đích: {src_path}")
            files_to_copy.append(src_file)
    
    # Save the list to cache
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(files_to_copy, f, ensure_ascii=False, indent=2)
    
    return files_to_copy

def parse_transfers(transfer_args: List[str]) -> List[Dict]:
    transfers = []
    for transfer in transfer_args:
        try:
            source, destination = transfer.split(',')
            transfers.append({"SOURCE": source.strip(), "DESTINATION": destination.strip()})
        except ValueError:
            logger.error(f"❌ Định dạng transfer không hợp lệ: {transfer}. Cần định dạng 'source,destination'")
    return transfers
def extract_remote_name(remote_path: str) -> str:
    """
    Trích xuất phần remote từ chuỗi kiểu 'remote:path/to/file'
    """
    if ":" not in remote_path:
        raise ValueError("Chuỗi không hợp lệ: thiếu dấu ':' để phân tách remote.")
    return remote_path.split(":", 1)[0]

def get_gdrive_free_space_percent_from_path(remote_path: str) -> tuple[bool, float | str]:
    try:
        remote = extract_remote_name(remote_path)
        result = subprocess.run(
            ["rclone", "about", f"{remote}:", "--json"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=True
        )
        data = json.loads(result.stdout)
        total = int(data["total"])
        free = int(data["free"])
        percent_free = (free / total) * 100 if total > 0 else 0
        return True, percent_free, free
    except ValueError as ve:
        return False, f"Lỗi chuỗi input: {ve}"
    except subprocess.CalledProcessError as e:
        return False, f"Lỗi chạy rclone: {e.stderr.strip()}"
    except (KeyError, ValueError, json.JSONDecodeError) as e:
        return False, f"Lỗi xử lý dữ liệu JSON: {e}"

def sync_files():
    setup_logging()
    total_copied = 0
    total_size_copied = 0
    stop_time = datetime.datetime.now().replace(hour=23, minute=0, second=0, microsecond=0)
    logger.info(f"⏰ Script sẽ dừng lúc {stop_time.strftime('%H:%M:%S')}")

    parser = argparse.ArgumentParser(description="Rclone sync script with command-line transfers")
    parser.add_argument('--transfers', nargs='+', required=True, 
                       help="List of source,destination pairs (e.g., 'GDrive60TB:PhimHoatHinh,30TB:PhimHoatHinh')")
    args = parser.parse_args()

    transfers = parse_transfers(args.transfers)
    if not transfers:
        logger.error("❌ Không có cặp source-destination hợp lệ được cung cấp")
        return

    for transfer in transfers:
        source = transfer["SOURCE"]
        destination = transfer["DESTINATION"]
        success, percent,free_space = get_gdrive_free_space_percent_from_path(destination)
        if not success:
            logger.error(f"❌ Không thể lấy thông tin dung lượng trống từ {destination}: {percent}")
            continue
        if percent < 2:
            logger.error(f"❌ Dung lượng trống trên {destination} dưới 2%: {percent:.2f}%")
            continue
        logger.info(f"🔄 Bắt đầu đồng bộ từ {source} đến {destination}")
        
        # Kiểm tra sự tồn tại của thư mục nguồn
        if not check_remote_exists(source):
            logger.error(f"❌ Bỏ qua đồng bộ vì thư mục nguồn không tồn tại: {source}")
            continue
            
        files = get_files_to_copy(source, destination)
        if not files:
            source_files = get_cached_files(source, is_source=True)
            if source_files:
                logger.info(f"✅ Không có file cần copy từ {source}: tất cả file đã tồn tại ở đích")
            else:
                logger.error(f"❌ Không thể lấy danh sách file từ {source}")
            continue
        
        logger.info(f"📂 Tổng số file cần xử lý từ {source}: {len(files)}")

        for file in files:
            current_time = datetime.datetime.now()
            if current_time >= stop_time:
                logger.info(f"⏰ Đã đến {current_time.strftime('%H:%M:%S')}, dừng script")
                logger.info(f"🏁 Hoàn tất - Copied: {total_copied} files, Size: {total_size_copied/(1024**3):.2f} GB")
                return
            
            logger.info(f"📏 Tổng kích thước đã copy: {total_size_copied/(1024**3):.2f} GB")
            if total_size_copied / (1024 ** 3) >= CONFIG["MAX_TRANSFER_GB"]:
                logger.info(f"📦 Đạt giới hạn {CONFIG['MAX_TRANSFER_GB']}GB")
                return
            

            src_path = f"{source}/{file['Path']}"
            dest_path = f"{destination}/{file['Path']}"
            file_size = file.get("Size", 0)

            if (total_size_copied + file_size) > free_space:
                logger.error(f"❌ Không đủ dung lượng trống trên {destination} để copy file: {file['Path']}")
                continue
            else:
                logger.info(f"✅ Dung lượng trống ({free_space/(1024**2):.2f} MB) đủ để copy file: {file['Path']} with size {file_size/(1024**2):.2f} MB")
            # Kiểm tra file nguồn
            try:
                src_exists = bool(rclone.ls(src_path))
                if not src_exists:
                    logger.error(f"❌ File nguồn không tồn tại: {src_path}")
                    continue
            except Exception as e:
                logger.error(f"❌ Lỗi khi kiểm tra file nguồn {src_path}: {str(e)}")
                continue

            logger.info(f"🚚 Đang copy: {file['Path']} ({file_size/(1024**2):.2f} MB)")
            success, error_msg = run_rclone_copy(src_path, dest_path)
            
            if success:
                src_hash = get_file_hash(src_path)
                dest_hash = get_file_hash(dest_path)
                if src_hash and dest_hash and src_hash == dest_hash:
                    logger.info(f"✅ Copy thành công: {file['Path']} (hash: {src_hash})")
                    total_copied += 1
                    total_size_copied += file_size
                else:
                    logger.error(f"❌ Hash không khớp: {file['Path']} (src: {src_hash}, dest: {dest_hash})")
                    if delete_file(dest_path):
                        logger.info(f"🗑️ Đã xóa file lỗi: {file['Path']}")
                        logger.info(f"🔄 Thử copy lại: {file['Path']}")
                        success, error_msg = run_rclone_copy(src_path, dest_path)
                        if success:
                            src_hash = get_file_hash(src_path)
                            dest_hash = get_file_hash(dest_path)
                            if src_hash and dest_hash and src_hash == dest_hash:
                                logger.info(f"✅ Copy lại thành công: {file['Path']} (hash: {src_hash})")
                                total_copied += 1
                                total_size_copied += file_size
                            else:
                                logger.error(f"❌ Copy lại thất bại, hash vẫn không khớp: {file['Path']}")
                        else:
                            logger.error(f"❌ Lỗi khi copy lại {file['Path']}: {error_msg}")
                    else:
                        logger.warning(f"⚠️ Không thể xóa file lỗi: {file['Path']}")
            else:
                logger.error(f"❌ Lỗi khi copy {file['Path']}: {error_msg}")
                if is_quota_exceeded(error_msg):
                    logger.error(f"❌ Dừng do vượt quota: {error_msg}")
                    return
                continue

    logger.info(f"🏁 Hoàn tất - Copied: {total_copied} files, Size: {total_size_copied/(1024**3):.2f} GB")

if __name__ == "__main__":
    sync_files()