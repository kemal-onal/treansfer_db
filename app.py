from flask import Flask, render_template, request, redirect, url_for, session, flash
import mysql.connector
import bcrypt
import re

app = Flask(__name__)
app.secret_key = 'cmpe321_super_secret_key'  # Oturum güvenliği için

# 1. VERİTABANI BAĞLANTISI
def get_db_connection():
    return mysql.connector.connect(
        host='db',          # Docker-compose servis adı
        user='root',
        password='password',
        database='transferdb'
    )

# 2. ŞİFRE POLİTİKASI KONTROLÜ
def is_password_strong(password):
    if len(password) < 8: return False
    if not re.search(r"[a-z]", password): return False
    if not re.search(r"[A-Z]", password): return False
    if not re.search(r"[0-9]", password): return False
    if not re.search(r"[\W_]", password): return False
    return True

# 3. ANA SAYFA YÖNLENDİRMESİ
@app.route('/')
def index():
    if 'user_id' in session:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

# 4. KAYIT SİSTEMİ
@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        role = request.form['role']
        username = request.form['username']
        raw_password = request.form['password']
        
        if not is_password_strong(raw_password):
            flash("Şifre en az 8 karakter olmalı; büyük/küçük harf, rakam ve özel karakter içermelidir.")
            return redirect(url_for('register'))
            
        hashed_password = bcrypt.hashpw(raw_password.encode('utf-8'), bcrypt.gensalt())
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Not: Gerçek senaryoda önce Person tablosuna ekleme yapılıp person_ID alınmalıdır.
            # Şimdilik mantığı göstermek adına doğrudan role göre ekleme yapıyoruz.
            if role == 'Player':
                query = "INSERT INTO Player (username, password) VALUES (%s, %s)"
                cursor.execute(query, (username, hashed_password.decode('utf-8')))
            
            conn.commit()
            flash("Kayıt başarılı! Giriş yapabilirsiniz.")
            return redirect(url_for('login'))
        except mysql.connector.Error as err:
            flash(f"Veritabanı Hatası: {err}")
        finally:
            cursor.close()
            conn.close()
            
    return render_template('register.html')

# 5. GİRİŞ SİSTEMİ
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        role = request.form['role']
        username = request.form['username']
        password_attempt = request.form['password']
        
        # Rol tablosu ve ID kolonu eşleştirmesi
        role_map = {
            'Player': ('Player', 'person_ID'),
            'Manager': ('Manager', 'person_ID'),
            'Referee': ('Referee', 'person_ID'),
            'DBManager': ('DB_Manager', 'username')
        }
        
        if role not in role_map:
            flash("Geçersiz rol seçimi.")
            return redirect(url_for('login'))
            
        table_name, id_column = role_map[role]
        
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # SQL Injection önlemi için Parameterized Query
        query = f"SELECT * FROM {table_name} WHERE username = %s"
        cursor.execute(query, (username,))
        user = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if user and bcrypt.checkpw(password_attempt.encode('utf-8'), user['password'].encode('utf-8')):
            session['username'] = user['username']
            session['role'] = role
            session['user_id'] = user[id_column]
            return redirect(url_for('dashboard'))
        else:
            flash("Hatalı kullanıcı adı, şifre veya rol seçimi.")
            
    return render_template('login.html')

# 6. PANEL (DASHBOARD) SİSTEMİ
@app.route('/dashboard')
def dashboard():
    if 'username' not in session:
        return redirect(url_for('login'))
        
    role = session['role']
    user_id = session['user_id']
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        if role == 'Manager':
            # Menajerin kulübünü bul
            cursor.execute("SELECT * FROM Club WHERE manager_ID = %s", (user_id,))
            club = cursor.fetchone()
            
            if club:
                # Yaklaşan maçları (skoru girilmemiş) getir
                cursor.execute("""
                    SELECT m.*, comp.name as comp_name, h.club_name as home_name, a.club_name as away_name 
                    FROM `Match` m 
                    JOIN Competition comp ON m.competition_ID = comp.competition_ID
                    JOIN Club h ON m.home_club_ID = h.club_ID
                    JOIN Club a ON m.away_club_ID = a.club_ID
                    WHERE (m.home_club_ID = %s OR m.away_club_ID = %s) AND m.home_goals IS NULL
                """, (club['club_ID'], club['club_ID']))
                matches = cursor.fetchall()

                # Aktif sözleşmeli oyuncuları getir
                cursor.execute("""
                    SELECT p.person_ID, pe.name, pe.surname, p.main_position 
                    FROM Player p 
                    JOIN Person pe ON p.person_ID = pe.person_ID
                    JOIN Contract c ON p.person_ID = c.player_id
                    WHERE c.club_id = %s AND CURDATE() BETWEEN c.start_date AND c.end_date
                """, (club['club_ID'],))
                players = cursor.fetchall()
                
                return render_template('dashboard_manager.html', club=club, matches=matches, players=players)
            else:
                return "Herhangi bir kulüple ilişiğiniz bulunamadı."
        elif role == 'Referee':
            # Hakemin atandığı ve henüz skoru girilmemiş (home_goals IS NULL) maçları getir
            cursor.execute("""
                SELECT m.*, comp.name as comp_name, h.club_name as home_name, a.club_name as away_name 
                FROM `Match` m 
                JOIN Competition comp ON m.competition_ID = comp.competition_ID
                JOIN Club h ON m.home_club_ID = h.club_ID
                JOIN Club a ON m.away_club_ID = a.club_ID
                WHERE m.referee_ID = %s AND m.home_goals IS NULL
            """, (user_id,))
            matches = cursor.fetchall()
            return render_template('dashboard_referee.html', username=session['username'], matches=matches)
        # Diğer roller için şablon yönlendirmesi
        return render_template(f'dashboard_{role.lower()}.html', username=session['username'])

    except Exception as e:
        return f"Dashboard yüklenirken hata oluştu: {str(e)}"
    finally:
        cursor.close()
        conn.close()

# 7. KADRO SEÇİM KAYDI (MANAGER ÖZEL)
@app.route('/set_squad', methods=['POST'])
def set_squad():
    if session.get('role') != 'Manager':
        return "Yetkisiz Erişim", 403

    match_id = request.form.get('match_id')
    club_id = request.form.get('club_id')
    selected_players = request.form.getlist('players')
    starters = request.form.getlist('starters')

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Önce mevcut kadro seçimini temizle
        cursor.execute("DELETE FROM Match_Stats WHERE match_id = %s AND club_id = %s", (match_id, club_id))

        for p_id in selected_players:
            is_starter = 1 if p_id in starters else 0
            # Veritabanı tetikleyicileri (Trigger) burada devreye girer
            cursor.execute("""
                INSERT INTO Match_Stats (match_id, player_id, club_id, is_starter, position_played)
                VALUES (%s, %s, %s, %s, 'To be set')
            """, (match_id, p_id, club_id, is_starter))
        
        conn.commit()
        flash("Kadro başarıyla güncellendi!")
    except mysql.connector.Error as err:
        conn.rollback()
        # Trigger'dan gelen hata mesajını yakalayıp kullanıcıya gösteriyoruz
        flash(f"Kısıtlama Hatası: {err.msg}")
    finally:
        cursor.close()
        conn.close()

    return redirect(url_for('dashboard'))

# HAKEM: MAÇ İSTATİSTİKLERİ GİRİŞ EKRANI
@app.route('/enter_stats/<int:match_id>')
def enter_stats(match_id):
    if session.get('role') != 'Referee':
        return "Yetkisiz Erişim", 403
        
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    # Maç detaylarını getir
    cursor.execute("""
        SELECT m.*, h.club_name as home_name, a.club_name as away_name 
        FROM `Match` m 
        JOIN Club h ON m.home_club_ID = h.club_ID
        JOIN Club a ON m.away_club_ID = a.club_ID
        WHERE m.match_ID = %s
    """, (match_id,))
    match_info = cursor.fetchone()
    
    # Bu maç için menajerler tarafından kadroya alınmış oyuncuları getir
    cursor.execute("""
        SELECT ms.*, pe.name, pe.surname, c.club_name
        FROM Match_Stats ms
        JOIN Person pe ON ms.player_id = pe.person_ID
        JOIN Club c ON ms.club_id = c.club_ID
        WHERE ms.match_id = %s
    """, (match_id,))
    players = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    # Eğer menajerler henüz kadro kurmadıysa hakemi uyar
    if not players:
        flash("Bu maç için takımlar henüz kadro bildiriminde bulunmamış. Önce menajerler kadro seçmeli.")
        return redirect(url_for('dashboard'))
        
    return render_template('enter_stats.html', match=match_info, players=players)

# HAKEM: İSTATİSTİKLERİ VERİTABANINA KAYDETME
@app.route('/save_stats', methods=['POST'])
def save_stats():
    if session.get('role') != 'Referee':
        return "Yetkisiz Erişim", 403

    match_id = request.form.get('match_id')
    home_goals = request.form.get('home_goals')
    away_goals = request.form.get('away_goals')
    attendance = request.form.get('attendance')

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # 1. Maçın genel skorunu ve seyirci sayısını güncelle (Raw SQL)
        cursor.execute("""
            UPDATE `Match` 
            SET home_goals = %s, away_goals = %s, attendance = %s 
            WHERE match_ID = %s
        """, (home_goals, away_goals, attendance, match_id))

        # 2. Her bir oyuncunun formdan gelen bireysel istatistiklerini güncelle
        # Formdaki input isimlerini player_id'ye göre dinamik alıyoruz
        player_ids = request.form.getlist('player_id')
        for p_id in player_ids:
            minutes = request.form.get(f'minutes_{p_id}')
            goals = request.form.get(f'goals_{p_id}')
            assists = request.form.get(f'assists_{p_id}')
            yellows = request.form.get(f'yellow_{p_id}')
            reds = request.form.get(f'red_{p_id}')
            rating = request.form.get(f'rating_{p_id}')

            cursor.execute("""
                UPDATE Match_Stats 
                SET minutes_played = %s, goals = %s, assists = %s, 
                    yellow_cards = %s, red_cards = %s, rating = %s
                WHERE match_ID = %s AND player_id = %s
            """, (minutes, goals, assists, yellows, reds, rating, match_id, p_id))

        conn.commit()
        flash("Maç istatistikleri ve skor başarıyla kaydedildi!")
    except mysql.connector.Error as err:
        conn.rollback()
        # Kural ihlali (örneğin 2 sarı kart girip kırmızı girmemek) burada patlar!
        flash(f"Veritabanı Kısıtlama Hatası: {err.msg}")
    finally:
        cursor.close()
        conn.close()

    return redirect(url_for('dashboard'))

# 8. ÇIKIŞ
@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

# 9. HATA YAKALAMA
@app.errorhandler(500)
def internal_error(error):
    return "<h3>Sunucu Hatası (500)</h3><p>Lütfen veritabanı bağlantısını ve tabloları kontrol edin.</p>", 500

@app.errorhandler(Exception)
def handle_exception(e):
    return f"<h3>Beklenmeyen Hata:</h3><p>{str(e)}</p>", 500

if __name__ == '__main__':
    # host='0.0.0.0' Codespaces ve tablet erişimi için zorunludur
    app.run(host='0.0.0.0', port=5000, debug=True)