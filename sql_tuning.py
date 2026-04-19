import psycopg2
from faker import Faker
import io
import time

# password 수정 필요
DB_CONFIG = {
    "host": "localhost",
    "database": "sql_tuning",
    "user": "postgres",
    "password": "password", 
    "port": "5432"
}

fake = Faker()

def generate_data():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        
        print("0. 기존 테이블 초기화 중...")
        cur.execute("DROP TABLE IF EXISTS orders;")
        cur.execute("DROP TABLE IF EXISTS users;")
        
        cur.execute("""
            CREATE TABLE users (
                user_id INT PRIMARY KEY,
                username VARCHAR(50),
                email VARCHAR(100),
                created_at TIMESTAMP
            );
        """)
        cur.execute("""
            CREATE TABLE orders (
                order_id INT PRIMARY KEY,
                user_id INT,
                order_date TIMESTAMP,
                total_amount DECIMAL(12, 2),
                status VARCHAR(20)
            );
        """)
        print("테이블 재생성 완료!")

        print("1/2: Users 데이터 생성 중...")
        user_buffer = io.StringIO()
        for i in range(1, 100001):
            user_buffer.write(f"{i}\t{fake.user_name()}\t{fake.email()}\t{fake.date_time_this_decade()}\n")
        user_buffer.seek(0)
        cur.copy_from(user_buffer, 'users', columns=('user_id', 'username', 'email', 'created_at'))
        print(" - Users 완료!")

        print("2/2: Orders 데이터 생성 중 (약 2~3분 소요)...")
        total_orders = 5000000
        batch_size = 1000000
        
        start_time = time.time()
        for j in range(0, total_orders, batch_size):
            order_buffer = io.StringIO()
            for i in range(j + 1, j + batch_size + 1):
                user_id = fake.random_int(min=1, max=100000)
                order_date = fake.date_time_this_year()
                amount = fake.pydecimal(left_digits=5, right_digits=2, positive=True)
                status = fake.random_element(elements=('COMPLETED', 'SHIPPED', 'CANCELLED', 'PENDING'))
                order_buffer.write(f"{i}\t{user_id}\t{order_date}\t{amount}\t{status}\n")
            
            order_buffer.seek(0)
            cur.copy_from(order_buffer, 'orders', columns=('order_id', 'user_id', 'order_date', 'total_amount', 'status'))
            print(f"   - {j + batch_size}건 완료...")

        print(f"\n모든 데이터 생성 완료! 소요 시간: {round(time.time() - start_time, 2)}초")

    except Exception as e:
        print(f"\n에러 발생: {e}")
    finally:
        if 'conn' in locals():
            cur.close()
            conn.close()

if __name__ == "__main__":
    generate_data()