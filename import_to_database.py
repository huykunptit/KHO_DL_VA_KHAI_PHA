import pandas as pd
import mysql.connector
from mysql.connector import errorcode

# ==================== CẤU HÌNH ====================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',  # Thay password nếu có
    'raise_on_warnings': True
}

DB_NAME = 'bank_db_new'
TABLE_NAME = 'customer_data'
CSV_FILE = 'bank_clean.csv'

# ==================== ĐỌC VÀ XỬ LÝ CSV ====================
print("=" * 60)
print("📂 BƯỚC 1: Đọc và xử lý dữ liệu CSV")
print("=" * 60)

try:
    df = pd.read_csv(CSV_FILE)
    print(f"✅ Đọc thành công: {len(df)} dòng, {len(df.columns)} cột")
    print(f"📋 Các cột: {', '.join(df.columns.tolist())}")
except FileNotFoundError:
    raise SystemExit(f"❌ Không tìm thấy file: {CSV_FILE}")
except Exception as e:
    raise SystemExit(f"❌ Lỗi đọc CSV: {e}")

# Chuyển đổi yes/no thành 1/0
df['housing'] = df['housing'].map({'yes': 1, 'no': 0}).fillna(0).astype(int)
df['loan'] = df['loan'].map({'yes': 1, 'no': 0}).fillna(0).astype(int)

# Xử lý missing values
df = df.fillna({
    'age': 0,
    'balance': 0.0,
    'campaign': 0
})

print(f"✅ Dữ liệu đã xử lý: {df.shape[0]} dòng × {df.shape[1]} cột")
print(f"\nMẫu 3 dòng đầu:")
print(df.head(3).to_string(index=False))

# ==================== KẾT NỐI MYSQL ====================
print("\n" + "=" * 60)
print("🔌 BƯỚC 2: Kết nối MySQL Server")
print("=" * 60)

try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("✅ Kết nối MySQL thành công")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        raise SystemExit("❌ Lỗi: Username hoặc password không đúng")
    else:
        raise SystemExit(f"❌ Lỗi kết nối MySQL: {err}")

# ==================== TẠO DATABASE ====================
print("\n" + "=" * 60)
print(f"🗄️  BƯỚC 3: Tạo Database '{DB_NAME}'")
print("=" * 60)

try:
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} DEFAULT CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_unicode_ci';")
    print(f"✅ Database '{DB_NAME}' đã được tạo/đã tồn tại")
    
    # Chuyển sang database mới
    conn.database = DB_NAME
    print(f"✅ Đã chuyển sang database '{DB_NAME}'")
    
except mysql.connector.Error as err:
    cursor.close()
    conn.close()
    raise SystemExit(f"❌ Lỗi tạo database: {err}")

# ==================== TẠO BẢNG ====================
print("\n" + "=" * 60)
print(f"📋 BƯỚC 4: Tạo bảng '{TABLE_NAME}'")
print("=" * 60)

create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    age INT NOT NULL COMMENT 'Tuổi khách hàng',
    balance DOUBLE COMMENT 'Số dư tài khoản',
    housing TINYINT COMMENT 'Có vay mua nhà (1=Yes, 0=No)',
    loan TINYINT COMMENT 'Có khoản vay cá nhân (1=Yes, 0=No)',
    campaign INT COMMENT 'Số lần tiếp xúc trong chiến dịch',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Thời gian tạo record',
    INDEX idx_age (age),
    INDEX idx_balance (balance)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Dữ liệu khách hàng ngân hàng';
"""

try:
    # Kiểm tra bảng có tồn tại không
    cursor.execute(f"SHOW TABLES LIKE '{TABLE_NAME}';")
    table_exists = cursor.fetchone()
    
    if table_exists:
        print(f"⚠️  Bảng '{TABLE_NAME}' đã tồn tại")
        user_input = input("   Bạn có muốn XÓA và tạo lại? (yes/no): ").strip().lower()
        if user_input == 'yes':
            cursor.execute(f"DROP TABLE {TABLE_NAME};")
            print(f"🗑️  Đã xóa bảng cũ")
            cursor.execute(create_table_sql)
            print(f"✅ Đã tạo lại bảng '{TABLE_NAME}'")
        else:
            print(f"⏭️  Giữ nguyên bảng cũ, dữ liệu sẽ được THÊM VÀO")
    else:
        cursor.execute(create_table_sql)
        print(f"✅ Bảng '{TABLE_NAME}' đã được tạo mới")
        
except mysql.connector.Error as err:
    cursor.close()
    conn.close()
    raise SystemExit(f"❌ Lỗi tạo bảng: {err}")

# ==================== IMPORT DỮ LIỆU ====================
print("\n" + "=" * 60)
print("⏳ BƯỚC 5: Import dữ liệu vào MySQL")
print("=" * 60)

insert_sql = f"""
INSERT INTO {TABLE_NAME} (age, balance, housing, loan, campaign)
VALUES (%s, %s, %s, %s, %s)
"""

# Chuyển DataFrame sang list of tuples (convert numpy types sang Python types)
data_tuples = []
for _, row in df[['age', 'balance', 'housing', 'loan', 'campaign']].iterrows():
    data_tuples.append((
        int(row['age']),
        float(row['balance']),
        int(row['housing']),
        int(row['loan']),
        int(row['campaign'])
    ))

try:
    batch_size = 500
    total_rows = len(data_tuples)
    inserted_count = 0
    
    print(f"📊 Tổng số dòng cần import: {total_rows}")
    print(f"📦 Kích thước batch: {batch_size}")
    print()
    
    for i in range(0, total_rows, batch_size):
        batch = data_tuples[i:i + batch_size]
        cursor.executemany(insert_sql, batch)
        conn.commit()
        
        inserted_count += len(batch)
        progress = (inserted_count / total_rows) * 100
        print(f"  ↳ [{progress:6.2f}%] Đã import {inserted_count:,}/{total_rows:,} dòng")
    
    print(f"\n✅ Import thành công {total_rows:,} dòng dữ liệu!")
    
except mysql.connector.Error as err:
    conn.rollback()
    print(f"\n❌ Lỗi khi import dữ liệu: {err}")
    print("🔄 Đã rollback các thay đổi")
    cursor.close()
    conn.close()
    raise SystemExit()

# ==================== XÁC NHẬN DỮ LIỆU ====================
print("\n" + "=" * 60)
print("🔍 BƯỚC 6: Kiểm tra dữ liệu đã import")
print("=" * 60)

try:
    # Đếm tổng số dòng
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};")
    total_count = cursor.fetchone()[0]
    print(f"📊 Tổng số dòng trong bảng: {total_count:,}")
    
    # Thống kê cơ bản
    cursor.execute(f"""
        SELECT 
            MIN(age) as min_age,
            MAX(age) as max_age,
            AVG(age) as avg_age,
            MIN(balance) as min_balance,
            MAX(balance) as max_balance,
            AVG(balance) as avg_balance
        FROM {TABLE_NAME};
    """)
    stats = cursor.fetchone()
    print(f"\n📈 Thống kê:")
    print(f"   • Tuổi: min={stats[0]}, max={stats[1]}, avg={stats[2]:.1f}")
    print(f"   • Số dư: min={stats[3]:,.0f}, max={stats[4]:,.0f}, avg={stats[5]:,.0f}")
    
    # Hiển thị 5 dòng mới nhất
    cursor.execute(f"SELECT * FROM {TABLE_NAME} ORDER BY id DESC LIMIT 5;")
    print(f"\n📝 5 dòng mới nhất:")
    print(f"{'ID':<6} {'Age':<6} {'Balance':<12} {'Housing':<8} {'Loan':<6} {'Campaign':<10}")
    print("-" * 60)
    for row in cursor.fetchall():
        print(f"{row[0]:<6} {row[1]:<6} {row[2]:<12.0f} {row[3]:<8} {row[4]:<6} {row[5]:<10}")
    
except mysql.connector.Error as err:
    print(f"⚠️  Không thể verify dữ liệu: {err}")

# ==================== ĐÓNG KẾT NỐI ====================
cursor.close()
conn.close()

print("\n" + "=" * 60)
print("🎉 HOÀN THÀNH!")
print("=" * 60)
print(f"✅ Database: {DB_NAME}")
print(f"✅ Bảng: {TABLE_NAME}")
print(f"✅ Số dòng: {total_rows:,}")
print(f"\n💡 Bạn có thể truy vấn bằng:")
print(f"   USE {DB_NAME};")
print(f"   SELECT * FROM {TABLE_NAME} LIMIT 10;")
print("=" * 60)