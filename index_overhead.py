# 인덱스 오버헤드 측정 스크립

import psycopg2, time, io
from faker import Faker

# 2번째 password 수정(postgresql)
DB_CONFIG = {
    "host": "localhost", "database": "sql_tuning",
    "user": "postgres", "password": "password", "port": "5432"
}
fake = Faker()

def measure_insert_speed(label):
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT max(order_id) FROM orders")
    start_id = (cur.fetchone()[0] or 0) + 1
    
    print(f"[{label}] 10만 건 삽입 시작...")
    order_buffer = io.StringIO()
    for i in range(start_id, start_id + 100000):
        u_id = fake.random_int(min=1, max=100000)
        order_buffer.write(f"{i}\t{u_id}\t{fake.date_time_this_year()}\t{fake.pydecimal(left_digits=5, right_digits=2, positive=True)}\tPENDING\n")
    
    order_buffer.seek(0)
    start_time = time.time()
    cur.copy_from(order_buffer, 'orders', columns=('order_id', 'user_id', 'order_date', 'total_amount', 'status'))
    end_time = time.time()
    
    duration = round(end_time - start_time, 4)
    print(f"[{label}] 소요 시간: {duration}초\n")
    conn.close()
    return duration

time_with_3_idx = measure_insert_speed("인덱스 3개")

print("인덱스 2개 추가 중...")
conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = True
cur = conn.cursor()
cur.execute("CREATE INDEX idx_orders_date ON orders(order_date);")
cur.execute("CREATE INDEX idx_orders_amount ON orders(total_amount);")
conn.close()

time_with_5_idx = measure_insert_speed("인덱스 5개")

print(f"결과 요약: 인덱스 추가 후 삽입 속도가 {round((time_with_5_idx/time_with_3_idx - 1) * 100, 2)}% 느려짐")
