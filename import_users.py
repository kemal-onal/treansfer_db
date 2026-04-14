import pandas as pd
import bcrypt
import mysql.connector

# Veritabanı bağlantısı
def get_db_connection():
    return mysql.connector.connect(
        host='db',
        user='root',
        password='password',
        database='transferdb'
    )

# Şifre hashleme fonksiyonu
def hash_password(plain_text_password):
    return bcrypt.hashpw(str(plain_text_password).encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

conn = get_db_connection()
cursor = conn.cursor()

try:
    print("Excel dosyası okunuyor...")
    # Excel dosyasını tek seferde yüklüyoruz
    xls = pd.ExcelFile('initial_data.xlsx')

    # 1. DB Managers
    print("DB Managerlar ekleniyor...")
    df_db = pd.read_excel(xls, 'DB Managers').where(pd.notna, None)
    for index, row in df_db.iterrows():
        cursor.execute("INSERT IGNORE INTO DB_Manager (username, password) VALUES (%s, %s)",
                       (row['username'], hash_password(row['password'])))

    # 2. Managers
    print("Managerlar ekleniyor...")
    df_man = pd.read_excel(xls, 'Managers').where(pd.notna, None)
    for index, row in df_man.iterrows():
        cursor.execute("INSERT IGNORE INTO Person (person_ID, name, surname, nationality, date_of_birth) VALUES (%s, %s, %s, %s, %s)",
                       (row['manager_id'], row['name'], row['surname'], row['nationality'], row['date_of_birth']))
        cursor.execute("INSERT IGNORE INTO Manager (person_ID, username, password, preferred_formation, experience_level) VALUES (%s, %s, %s, %s, %s)",
                       (row['manager_id'], row['username'], hash_password(row['password']), row['preferred_formation'], row['experience_level']))

    # 3. Referees
    print("Hakemler ekleniyor...")
    df_ref = pd.read_excel(xls, 'Referees').where(pd.notna, None)
    for index, row in df_ref.iterrows():
        cursor.execute("INSERT IGNORE INTO Person (person_ID, name, surname, nationality, date_of_birth) VALUES (%s, %s, %s, %s, %s)",
                       (row['referee_id'], row['name'], row['surname'], row['nationality'], row['date_of_birth']))
        cursor.execute("INSERT IGNORE INTO Referee (person_ID, username, password, license_level, years_of_experience) VALUES (%s, %s, %s, %s, %s)",
                       (row['referee_id'], row['username'], hash_password(row['password']), row['license_level'], row['years_of_experience']))

    # 4. Players
    print("Oyuncular ekleniyor...")
    df_play = pd.read_excel(xls, 'Players').where(pd.notna, None)
    for index, row in df_play.iterrows():
        cursor.execute("INSERT IGNORE INTO Person (person_ID, name, surname, nationality, date_of_birth) VALUES (%s, %s, %s, %s, %s)",
                       (row['player_id'], row['name'], row['surname'], row['nationality'], row['date_of_birth']))
        cursor.execute("INSERT IGNORE INTO Player (person_ID, username, password, market_value, main_position, strong_foot, height) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                       (row['player_id'], row['username'], hash_password(row['password']), row['market_value'], row['main_position'], row['strong_foot'], row['height']))

    conn.commit()
    print("✅ Tüm kullanıcılar başarıyla şifrelenip veritabanına eklendi!")

except Exception as e:
    print(f"❌ Bir hata oluştu: {e}")
finally:
    cursor.close()
    conn.close()
