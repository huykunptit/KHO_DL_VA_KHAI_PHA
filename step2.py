import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import errorcode
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

# ==================== CẤU HÌNH ====================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'bank_db_new'
}

TABLE_NAME = 'customer_data'
OUTPUT_FILE = 'Cluster.csv'
MAX_CLUSTERS = 10  # Số cụm tối đa để test

# ==================== KẾT NỐI DATABASE ====================
print("=" * 70)
print("🔌 BƯỚC 1: Kết nối Database")
print("=" * 70)

try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print(f"✅ Đã kết nối database '{DB_CONFIG['database']}' thành công")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        raise SystemExit("❌ Lỗi: Username hoặc password không đúng")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        raise SystemExit(f"❌ Database '{DB_CONFIG['database']}' không tồn tại")
    else:
        raise SystemExit(f"❌ Lỗi kết nối: {err}")

# ==================== TẢI DỮ LIỆU ====================
print("\n" + "=" * 70)
print("📊 BƯỚC 2: Tải dữ liệu từ MySQL")
print("=" * 70)

query = f"""
SELECT id, age, balance, housing, loan, campaign
FROM {TABLE_NAME}
ORDER BY id
"""

try:
    df = pd.read_sql(query, conn)
    print(f"✅ Đã tải {len(df):,} dòng dữ liệu")
    print(f"📋 Các cột: {', '.join(df.columns.tolist())}")
    
    # Hiển thị thông tin cơ bản
    print(f"\n📈 Thống kê mô tả:")
    print(df.describe().round(2))
    
except Exception as e:
    cursor.close()
    conn.close()
    raise SystemExit(f"❌ Lỗi tải dữ liệu: {e}")

# Đóng kết nối MySQL (không cần nữa)
cursor.close()
conn.close()

# ==================== CHUẨN HÓA DỮ LIỆU ====================
print("\n" + "=" * 70)
print("🔧 BƯỚC 3: Chuẩn hóa dữ liệu (Z-score Normalization)")
print("=" * 70)

# Chọn các feature để clustering (bỏ ID)
features = ['age', 'balance', 'housing', 'loan', 'campaign']
X = df[features].copy()

print(f"📊 Features sử dụng: {', '.join(features)}")
print(f"📏 Kích thước dữ liệu: {X.shape[0]} dòng × {X.shape[1]} cột")

# Chuẩn hóa Z-score
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

print(f"✅ Đã chuẩn hóa dữ liệu bằng Z-score")
print(f"   • Mean = 0, Std = 1 cho mỗi feature")
print(f"\n📊 Dữ liệu sau chuẩn hóa (5 dòng đầu):")
df_scaled = pd.DataFrame(X_scaled, columns=features)
print(df_scaled.head().round(3))

# ==================== ELBOW METHOD ====================
print("\n" + "=" * 70)
print("📈 BƯỚC 4: Xác định số cụm tối ưu (Elbow Method)")
print("=" * 70)

inertias = []
K_range = range(2, MAX_CLUSTERS + 1)

print(f"🔍 Đang test từ {min(K_range)} đến {max(K_range)} cụm...")

for k in K_range:
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    kmeans.fit(X_scaled)
    inertias.append(kmeans.inertia_)
    print(f"   K={k:2d} → Inertia = {kmeans.inertia_:,.2f}")

# Vẽ biểu đồ Elbow
plt.figure(figsize=(10, 6))
plt.plot(K_range, inertias, 'bo-', linewidth=2, markersize=8)
plt.xlabel('Số cụm (K)', fontsize=12)
plt.ylabel('Inertia (Within-cluster sum of squares)', fontsize=12)
plt.title('Elbow Method - Xác định số cụm tối ưu', fontsize=14, fontweight='bold')
plt.grid(True, alpha=0.3)
plt.xticks(K_range)

# Đánh dấu điểm "elbow" (tính sơ bộ)
# Tìm điểm có độ giảm thay đổi lớn nhất
differences = np.diff(inertias)
second_diff = np.diff(differences)
elbow_point = np.argmax(second_diff) + 2  # +2 vì K bắt đầu từ 2

plt.axvline(x=elbow_point, color='r', linestyle='--', linewidth=2, label=f'Elbow tại K={elbow_point}')
plt.legend(fontsize=10)
plt.tight_layout()
plt.savefig('elbow_method.png', dpi=300, bbox_inches='tight')
print(f"\n✅ Đã lưu biểu đồ Elbow: elbow_method.png")
print(f"💡 Gợi ý số cụm tối ưu: K = {elbow_point}")

# ==================== ÁP DỤNG K-MEANS ====================
print("\n" + "=" * 70)
print("🎯 BƯỚC 5: Áp dụng K-means Clustering")
print("=" * 70)

# Cho phép người dùng chọn K hoặc dùng K tự động
user_choice = input(f"\n📝 Chọn số cụm (Enter để dùng K={elbow_point}): ").strip()
optimal_k = int(user_choice) if user_choice.isdigit() else elbow_point

print(f"\n🔄 Đang chạy K-means với K = {optimal_k}...")

# Chạy K-means
kmeans_final = KMeans(n_clusters=optimal_k, random_state=42, n_init=20, max_iter=300)
clusters = kmeans_final.fit_predict(X_scaled)

# Thêm nhãn cluster vào DataFrame gốc
df['Cluster'] = clusters

print(f"✅ Hoàn thành phân cụm!")
print(f"   • Số cụm: {optimal_k}")
print(f"   • Inertia: {kmeans_final.inertia_:,.2f}")
print(f"   • Số lần lặp: {kmeans_final.n_iter_}")

# ==================== PHÂN TÍCH KẾT QUẢ ====================
print("\n" + "=" * 70)
print("📊 BƯỚC 6: Phân tích kết quả phân cụm")
print("=" * 70)

# Số lượng khách hàng trong mỗi cụm
cluster_counts = df['Cluster'].value_counts().sort_index()
print(f"\n📈 Phân bố khách hàng theo cụm:")
for cluster_id, count in cluster_counts.items():
    percentage = (count / len(df)) * 100
    print(f"   Cluster {cluster_id}: {count:5,} khách hàng ({percentage:5.2f}%)")

# Thống kê trung bình mỗi cụm
print(f"\n📊 Đặc điểm trung bình của mỗi cụm:")
cluster_stats = df.groupby('Cluster')[features].mean().round(2)
print(cluster_stats)

# Mô tả chi tiết từng cụm
print(f"\n📝 Mô tả chi tiết từng cụm:")
for cluster_id in range(optimal_k):
    cluster_data = df[df['Cluster'] == cluster_id]
    print(f"\n{'='*60}")
    print(f"Cluster {cluster_id} ({len(cluster_data)} khách hàng)")
    print(f"{'='*60}")
    print(cluster_data[features].describe().round(2))

# ==================== XUẤT KẾT QUẢ ====================
print("\n" + "=" * 70)
print("💾 BƯỚC 7: Xuất kết quả ra file CSV")
print("=" * 70)

# Sắp xếp theo ID để dễ đối chiếu
df_output = df.sort_values('id').reset_index(drop=True)

# Xuất file
try:
    df_output.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    print(f"✅ Đã xuất file: {OUTPUT_FILE}")
    print(f"📊 Số dòng: {len(df_output):,}")
    print(f"📋 Các cột: {', '.join(df_output.columns.tolist())}")
    
    # Hiển thị mẫu 10 dòng đầu
    print(f"\n📝 10 dòng đầu tiên trong file CSV:")
    print(df_output.head(10).to_string(index=False))
    
except Exception as e:
    raise SystemExit(f"❌ Lỗi xuất file: {e}")

# ==================== TẠO BÁO CÁO TÓM TẮT ====================
print("\n" + "=" * 70)
print("📋 BÁO CÁO TÓM TẮT")
print("=" * 70)

summary = f"""
╔═══════════════════════════════════════════════════════════╗
║           KẾT QUẢ PHÂN CỤM K-MEANS                        ║
╚═══════════════════════════════════════════════════════════╝

📊 THÔNG TIN CHUNG:
   • Tổng số khách hàng: {len(df):,}
   • Số cụm (K): {optimal_k}
   • Features sử dụng: {', '.join(features)}
   • Phương pháp chuẩn hóa: Z-score (StandardScaler)
   • Inertia: {kmeans_final.inertia_:,.2f}

📈 PHÂN BỐ CỤM:
"""

for cluster_id, count in cluster_counts.items():
    percentage = (count / len(df)) * 100
    bar = '█' * int(percentage / 2)
    summary += f"   Cluster {cluster_id}: {bar} {count:,} ({percentage:.1f}%)\n"

summary += f"""
📁 FILE ĐÃ TẠO:
   • {OUTPUT_FILE} - Dữ liệu với nhãn Cluster
   • elbow_method.png - Biểu đồ Elbow Method

💡 HƯỚNG DẪN SỬ DỤNG:
   • Mở file {OUTPUT_FILE} để xem kết quả
   • Mỗi khách hàng có thêm cột 'Cluster' (0 đến {optimal_k-1})
   • Phân tích đặc điểm mỗi cụm để đưa ra chiến lược marketing
"""

print(summary)

# Lưu báo cáo
with open('clustering_report.txt', 'w', encoding='utf-8') as f:
    f.write(summary)
    f.write("\n\nĐẶC ĐIỂM TRUNG BÌNH CÁC CỤM:\n")
    f.write("=" * 80 + "\n")
    f.write(cluster_stats.to_string())

print("✅ Đã lưu báo cáo: clustering_report.txt")

print("\n" + "=" * 70)
print("🎉 HOÀN THÀNH!")
print("=" * 70)