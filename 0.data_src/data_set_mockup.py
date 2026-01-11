import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# --- 1. CONFIGURATION AND PRODUCT MASTER DATA ---

# กำหนดเป้าหมายและวันที่
TARGET_ROWS = 50000
CURRENT_DATE = (datetime.now() - timedelta(days=4)).replace(hour=0, minute=0, second=0, microsecond=0) # ใช้วันที่ปัจจุบัน
CURRENT_DATE_STR = CURRENT_DATE.strftime("%Y%m%d")
OUTPUT_FILENAME = f"sales_transaction_data_{CURRENT_DATE_STR}.csv"

# **แก้ไขส่วนนี้: เพิ่มจำนวนร้านค้าเป็น 10 สาขา**
STORE_IDS = [f'S{i:02}' for i in range(1, 11)] # S01, S02, ..., S10

# ข้อมูลสินค้า 20 รายการ (ใช้ชุดเดิม)
PRODUCT_MASTER = [
    # Base_Sales_Unit (base) คือ ยอดขายเฉลี่ยต่อ Transaction (ต่อหน่วย)
    {'id': 'P001', 'name': 'Cola', 'price': 20.00, 'base': 5.5, 'prob_weight': 0.10},
    {'id': 'P004', 'name': 'Drinking Water 1.5 Litre', 'price': 15.00, 'base': 4.0, 'prob_weight': 0.10},
    {'id': 'P008', 'name': 'Instant Cup Nooddle', 'price': 15.00, 'base': 3.5, 'prob_weight': 0.08},
    {'id': 'P019', 'name': 'Cigarette Pack', 'price': 120.00, 'base': 1.0, 'prob_weight': 0.08},

    {'id': 'P003', 'name': 'Ice cream', 'price': 15.00, 'base': 3.0, 'prob_weight': 0.07},
    {'id': 'P006', 'name': 'Beer can', 'price': 55.00, 'base': 2.5, 'prob_weight': 0.07},
    {'id': 'P011', 'name': 'Potato Chips', 'price': 30.00, 'base': 3.0, 'prob_weight': 0.06},
    {'id': 'P014', 'name': 'Loaf of Bread', 'price': 35.00, 'base': 1.5, 'prob_weight': 0.05},
    {'id': 'P015', 'name': 'Bar Soap', 'price': 15.00, 'base': 1.0, 'prob_weight': 0.05},
    {'id': 'P016', 'name': 'Toothpaste', 'price': 55.00, 'base': 1.0, 'prob_weight': 0.05},

    {'id': 'P002', 'name': 'Electrolyte Drink', 'price': 25.00, 'base': 1.0, 'prob_weight': 0.03},
    {'id': 'P005', 'name': 'Roll-on Deodorant', 'price': 45.00, 'base': 1.0, 'prob_weight': 0.03},
    {'id': 'P007', 'name': 'Instant Coffee', 'price': 10.00, 'base': 1.5, 'prob_weight': 0.03},
    {'id': 'P009', 'name': 'Cold Medicine', 'price': 35.00, 'base': 1.0, 'prob_weight': 0.03},
    {'id': 'P010', 'name': 'Folding Umbrella', 'price': 100.00, 'base': 1.0, 'prob_weight': 0.01},
    {'id': 'P012', 'name': 'Mosquito Repellent', 'price': 65.00, 'base': 1.0, 'prob_weight': 0.03},
    {'id': 'P013', 'name': 'Candle', 'price': 10.00, 'base': 1.0, 'prob_weight': 0.01},
    {'id': 'P017', 'name': 'Fish Sauce', 'price': 30.00, 'base': 1.0, 'prob_weight': 0.03},
    {'id': 'P018', 'name': 'Jasmine Rice 1 Kg', 'price': 40.00, 'base': 1.0, 'prob_weight': 0.03},
    {'id': 'P020', 'name': 'Dishwashing Liquid', 'price': 30.00, 'base': 1.0, 'prob_weight': 0.03},
]

df_products = pd.DataFrame(PRODUCT_MASTER)

# ดึง List และ Weight สำหรับการสุ่มสินค้า
PRODUCT_IDS = df_products['id'].tolist()
PROBABILITIES = df_products['prob_weight'].tolist()
PROBABILITIES = np.array(PROBABILITIES) / np.sum(PROBABILITIES) # Normalize

print(f"--- เริ่มสร้างข้อมูลธุรกรรมการขาย: {TARGET_ROWS:,} แถว ---")
print(f"จำนวนร้านค้า: {len(STORE_IDS)} สาขา")

# --- 2. CREATE 50,000 SALES TRANSACTIONS ---

# 2.1 สร้างโครงสร้างข้อมูลพื้นฐาน 50,000 แถว
# **กำหนดความน่าจะเป็นของร้านค้า (S01-S03 ขายดีกว่า S08-S10)**
# 10 Stores: [0.12, 0.14, 0.06, 0.08, 0.09, 0.09, 0.15, 0.08, 0.13, 0.06] (รวมกันได้ 1.00)
STORE_PROBABILITIES = [0.12, 0.14, 0.06, 0.08, 0.09, 0.09, 0.15, 0.08, 0.13, 0.06]

df_transactions = pd.DataFrame({
    'transaction_id': [f"TRX{i+1:07}" for i in range(TARGET_ROWS)],
    # สุ่ม Store ID ตาม Weight ที่กำหนด
    'store_id': np.random.choice(STORE_IDS, size=TARGET_ROWS, p=STORE_PROBABILITIES),
    # สุ่ม Product ID ตาม Weight ที่กำหนด
    'product_id': np.random.choice(PRODUCT_IDS, size=TARGET_ROWS, p=PROBABILITIES) 
})

# 2.2 Join ข้อมูลราคา/Base Sales
df_transactions = df_transactions.merge(df_products[['id', 'name', 'price', 'base']], 
                                        left_on='product_id', 
                                        right_on='id', 
                                        how='left')
df_transactions.rename(columns={'name': 'product_name', 'price': 'unit_price', 'base': 'base_units'}, inplace=True)
df_transactions.drop(columns=['id'], inplace=True)

# 2.3 คำนวณ Units Sold และ Sale Date/Time
def calculate_units_and_time(row):
    # สุ่ม Units Sold: Normal Distribution (Mean=base_units, SD=20%)
    sd_units = row['base_units'] * 0.20
    units = np.random.normal(loc=row['base_units'], scale=sd_units)
    
    units_sold = int(max(1, round(units))) # จำนวนเต็มบวกเท่านั้น

    # สุ่มเวลาขาย: ในช่วง 08:00 - 22:00 น.
    time_offset = timedelta(seconds=np.random.randint(8 * 3600, 22 * 3600))
    sale_datetime = (CURRENT_DATE + time_offset).strftime("%Y%m%d %H:%M:%S")

    return pd.Series([units_sold, sale_datetime])

df_transactions[['units_sold', 'sale_date']] = df_transactions.apply(calculate_units_and_time, axis=1)

# 2.4 คำนวณ Sales Amount และจัดระเบียบ Field
df_transactions['sales_amount'] = round(df_transactions['units_sold'] * df_transactions['unit_price'], 2)

# จัดเรียง Field ตามที่ผู้ใช้ต้องการ
df_final = df_transactions[['transaction_id', 'store_id', 'sale_date', 
                            'product_id', 'product_name', 'units_sold', 'sales_amount']]

# --- 3. EXPORT TO CSV ---
try:
    df_final.to_csv(OUTPUT_FILENAME, index=False, encoding='utf-8-sig')
    print("-" * 50)
    print(f"✅ สร้างข้อมูลสำเร็จ! บันทึกไฟล์ที่: {os.path.abspath(OUTPUT_FILENAME)}")
    print(f"จำนวนแถวทั้งหมด: {len(df_final):,} แถว")
    print(f"ตัวอย่าง Store ID: {df_final['store_id'].unique()}")
    print("-" * 50)
    print("ตัวอย่างข้อมูล 5 แถวแรก:")
    print(df_final.head())
except Exception as e:
    print(f"เกิดข้อผิดพลาดในการบันทึกไฟล์ CSV: {e}")