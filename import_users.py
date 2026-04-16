import pandas as pd
import bcrypt
import mysql.connector

def get_db_connection():
    return mysql.connector.connect(
        host='db',
        user='root',
        password='password',
        database='transferdb'
    )

def hash_password(plain_text_password):
    return bcrypt.hashpw(str(plain_text_password).encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

conn = get_db_connection()
cursor = conn.cursor()

try:
    print("Excel dosyası okunuyor... (Bu işlem birkaç saniye sürebilir)")
    xls = pd.ExcelFile('initial_data.xlsx')

    # 1. DB MANAGERS
    print("1/11 - DB Managerlar ekleniyor...")
    df_db = pd.read_excel(xls, 'DB Managers').where(pd.notna, None)
    for _, row in df_db.iterrows():
        try:
            cursor.execute("INSERT IGNORE INTO DB_Manager (username, password) VALUES (%s, %s)",
                           (row['username'], hash_password(row['password'])))
        except Exception as e: print(f"  Uyarı (DB_Manager): {e}")

    # 2. STADIUMS
    print("2/11 - Stadyumlar ekleniyor...")
    df_stad = pd.read_excel(xls, 'Stadiums').where(pd.notna, None)
    for _, row in df_stad.iterrows():
        try:
            cursor.execute("INSERT IGNORE INTO Stadium (stadium_ID, stadium_name, city, capacity) VALUES (%s, %s, %s, %s)",
                           (row['stadium_id'], row['stadium_name'], row['city'], row['capacity']))
        except Exception as e: print(f"  Uyarı (Stadium): {e}")

    # 3. MANAGERS
    print("3/11 - Teknik Direktörler ekleniyor...")
    df_man = pd.read_excel(xls, 'Managers').where(pd.notna, None)
    for _, row in df_man.iterrows():
        try:
            cursor.execute("INSERT IGNORE INTO Person (person_ID, name, surname, nationality, date_of_birth) VALUES (%s, %s, %s, %s, %s)",
                           (row['manager_id'], row['name'], row['surname'], row['nationality'], row['date_of_birth']))
            cursor.execute("INSERT IGNORE INTO Manager (person_ID, username, password, preferred_formation, experience_level) VALUES (%s, %s, %s, %s, %s)",
                           (row['manager_id'], row['username'], hash_password(row['password']), row['preferred_formation'], row['experience_level']))
        except Exception as e: print(f"  Uyarı (Manager): {e}")

    # 4. REFEREES
    print("4/11 - Hakemler ekleniyor...")
    df_ref = pd.read_excel(xls, 'Referees').where(pd.notna, None)
    for _, row in df_ref.iterrows():
        try:
            cursor.execute("INSERT IGNORE INTO Person (person_ID, name, surname, nationality, date_of_birth) VALUES (%s, %s, %s, %s, %s)",
                           (row['referee_id'], row['name'], row['surname'], row['nationality'], row['date_of_birth']))
            cursor.execute("INSERT IGNORE INTO Referee (person_ID, username, password, license_level, years_of_experience) VALUES (%s, %s, %s, %s, %s)",
                           (row['referee_id'], row['username'], hash_password(row['password']), row['license_level'], row['years_of_experience']))
        except Exception as e: print(f"  Uyarı (Referee): {e}")

    # 5. PLAYERS
    print("5/11 - Oyuncular ekleniyor...")
    df_play = pd.read_excel(xls, 'Players').where(pd.notna, None)
    for _, row in df_play.iterrows():
        try:
            cursor.execute("INSERT IGNORE INTO Person (person_ID, name, surname, nationality, date_of_birth) VALUES (%s, %s, %s, %s, %s)",
                           (row['player_id'], row['name'], row['surname'], row['nationality'], row['date_of_birth']))
            cursor.execute("INSERT IGNORE INTO Player (person_ID, username, password, market_value, main_position, strong_foot, height) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                           (row['player_id'], row['username'], hash_password(row['password']), row['market_value'], row['main_position'], row['strong_foot'], row['height']))
        except Exception as e: print(f"  Uyarı (Player): {e}")

    # 6. CLUBS
    print("6/11 - Kulüpler ekleniyor...")
    df_club = pd.read_excel(xls, 'Clubs').where(pd.notna, None)
    for _, row in df_club.iterrows():
        try:
            cursor.execute("INSERT IGNORE INTO Club (club_ID, club_name, stadium_ID, foundation_year, manager_ID) VALUES (%s, %s, %s, %s, %s)",
                           (row['club_id'], row['club_name'], row['stadium_id'], row['foundation_year'], row['manager_id']))
        except Exception as e: print(f"  Uyarı (Club - {row['club_name']}): {e}")

    # 7. CONTRACTS
    print("7/11 - Sözleşmeler ekleniyor...")
    df_con = pd.read_excel(xls, 'Contracts').where(pd.notna, None)
    for _, row in df_con.iterrows():
        try:
            cursor.execute("INSERT IGNORE INTO Contract (contract_id, player_id, club_id, start_date, end_date, weekly_wage, contract_type) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                           (row['contract_id'], row['player_id'], row['club_id'], row['start_date'], row['end_date'], row['weekly_wage'], row['contract_type']))
        except Exception as e: print(f"  Trigger Uyarı (Contract ID {row['contract_id']}): {e}")

    # 8. TRANSFERS
    print("8/11 - Transfer Geçmişi ekleniyor...")
    df_trans = pd.read_excel(xls, 'Transfer Records').where(pd.notna, None)
    for _, row in df_trans.iterrows():
        try:
            cursor.execute("INSERT IGNORE INTO Transfer (transfer_id, player_id, from_club_id, to_club_id, transfer_date, transfer_fee, transfer_type) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                           (row['transfer_id'], row['player_id'], row['from_club_id'], row['to_club_id'], row['transfer_date'], row['transfer_fee'], row['transfer_type']))
        except Exception as e: print(f"  Trigger Uyarı (Transfer ID {row['transfer_id']}): {e}")

    # 9. COMPETITIONS
    print("9/11 - Turnuvalar ekleniyor...")
    df_comp = pd.read_excel(xls, 'Competitions').where(pd.notna, None)
    for _, row in df_comp.iterrows():
        try:
            cursor.execute("INSERT IGNORE INTO Competition (competition_ID, name, season, country, competition_type) VALUES (%s, %s, %s, %s, %s)",
                           (row['competition_id'], row['name'], row['season'], row['country'], row['competition_type']))
        except Exception as e: print(f"  Uyarı (Competition): {e}")

    # 10. MATCHES
    print("10/11 - Maçlar ekleniyor...")
    df_match = pd.read_excel(xls, 'Matches').where(pd.notna, None)
    for _, row in df_match.iterrows():
        try:
            cursor.execute("INSERT IGNORE INTO `Match` (match_ID, competition_ID, home_club_ID, away_club_ID, stadium_ID, referee_ID, match_date, match_time, attendance, home_goals, away_goals) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                           (row['match_id'], row['competition_id'], row['home_club_id'], row['away_club_id'], row['stadium_id'], row['referee_id'], row['match_date'], row['match_time'], row['attendance'], row['home_goals'], row['away_goals']))
        except Exception as e: print(f"  Trigger Uyarı (Match ID {row['match_id']}): {e}")

    # 11. MATCH STATS
    print("11/11 - Maç İstatistikleri ekleniyor...")
    df_ms = pd.read_excel(xls, 'Match Stats').where(pd.notna, None)
    for _, row in df_ms.iterrows():
        try:
            is_starter = 1 if row['is_starter'] in [True, 'True', 1, '1'] else 0
            cursor.execute("INSERT IGNORE INTO Match_Stats (match_ID, player_id, club_id, is_starter, minutes_played, position_played, goals, assists, yellow_cards, red_cards, rating) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                           (row['match_id'], row['player_id'], row['club_id'], is_starter, row['minutes_played'], row['position_played'], row['goals'], row['assists'], row['yellow_cards'], row['red_cards'], row['rating']))
        except Exception as e: print(f"  Trigger Uyarı (Match Stats): {e}")

    conn.commit()
    print("\n✅ İŞLEM TAMAM! Sağlam veriler yüklendi. Hatalı Excel satırları yukarıda uyarı olarak listelendi.")

except Exception as e:
    print(f"❌ BEKLENMEYEN BİR DIŞ HATA OLUŞTU: {e}")
finally:
    cursor.close()
    conn.close()