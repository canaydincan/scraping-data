import pandas as pd
import re
import sqlite3
import time
import matplotlib.pyplot as plt
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import phonenumbers
from difflib import SequenceMatcher
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium.common.exceptions import TimeoutException

# Kara listeye alınacak URL anahtar kelimeleri
BLACKLISTED_SITES = ["linkedin.com", "instagram.com", "facebook.com", "twitter.com", "tiktok.com"]
AD_KEYWORDS = ["sponsor", "ad", "promoted", "advertisement"]

# SQLite veritabanına bağlanma ve tablo oluşturma
def create_database():
    conn = sqlite3.connect('company_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS company_contacts (
            company_name TEXT,
            website_url TEXT,
            email TEXT,
            phone TEXT
        )
    ''')
    conn.commit()
    conn.close()

# Excel dosyasını okuma fonksiyonu
def read_excel(file_path):
    df = pd.read_excel(file_path)
    return df['Firma İsmi'].tolist()

# Veriyi veritabanına kaydetme
def save_to_database(conn, cursor, company_name, website_url, email, phone):
    cursor.execute('''
        INSERT INTO company_contacts (company_name, website_url, email, phone)
        VALUES (?, ?, ?, ?)
    ''', (company_name, website_url, email, phone))
    conn.commit()

# Telefon numarasını doğrulama
def validate_phone_number(phone):
    try:
        parsed_number = phonenumbers.parse(phone, None)
        if phonenumbers.is_valid_number(parsed_number):
            return phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
    except phonenumbers.NumberParseException:
        return None
    return None

# Şirket ismi ve URL karşılaştırması için string benzerliği hesaplayan fonksiyon
def get_similarity(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

# Öncelikli ve ikincil anahtar kelimeler
primary_keywords = ['Contact', 'CONTACT', 'Contact Us', 'CONTACT US', 'İletişim', 'İLETİŞİM', 'Bize Ulaş', 'BİZE ULAŞ', 'Kontakt']
secondary_keywords = ['About', 'ABOUT', 'About Us', 'ABOUT US', 'Support', 'SUPPORT', 'Get in Touch', 'GET IN TOUCH', 
                      'Reach Us', 'REACH US', 'Connect', 'CONNECT', 'Connect Us', 'CONNECT US','KONTAKT', 
                      'Über Uns', 'ÜBER UNS']

def fetch_contact_info(driver):
    # "Contact", "About" gibi bağlantıları arayın
    contact_links = driver.find_elements(By.XPATH, """
        //a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'contact') or
            contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'support') or
            contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'contact us') or
            contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'iletişim') or
            contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'bize ulaşın') or
            contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'connect us') or
            contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'connect') or
            contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'get in touch') or
            contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'kontakt') or                                         
            contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'reach us')]
    """)

    if not contact_links:
        contact_links = driver.find_elements(By.XPATH, """
            //a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'about') or
                contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'hakkımızda') or
                contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'hakkımda') or
                contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'über uns')]
        """)

    email = "Siteden Bu Bilgiye Ulaşılamadı"
    phone = "Siteden Bu Bilgiye Ulaşılamadı"

    email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
    phone_pattern = r"(?:\+?(\d{1,3})[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}"

   # 1. Öncelikle "Contact" ve "About" sayfalarında arama yapın
    if contact_links:
        for link in contact_links:
            try:
                driver.get(link.get_attribute("href"))
                WebDriverWait(driver, 4).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                time.sleep(random.uniform(0.5, 1.8))  # Yüklenmesi için bekle

                page_content = driver.page_source
                soup = BeautifulSoup(page_content, "html.parser")

                # 2. mailto: ve tel: bağlantılarından e-posta ve telefon al
                mailto_links = soup.select('a[href^=mailto]')
                tel_links = soup.select('a[href^=tel]')

                emails = [link['href'].replace("mailto:", "") for link in mailto_links]
                phones = [link['href'].replace("tel:", "") for link in tel_links]

                # Regex ile de arama yap
                emails += re.findall(email_pattern, soup.get_text())
                phones += re.findall(phone_pattern, soup.get_text())

                if emails and email == "Siteden Bu Bilgiye Ulaşılamadı":
                    email = emails[0]

                if phones and phone == "Siteden Bu Bilgiye Ulaşılamadı":
                    for number in phones:
                        validated_number = validate_phone_number(number)
                        if validated_number:
                            phone = validated_number
                            break
            except Exception as e:
                print(f"Bağlantıya gidilirken hata oluştu: {e}")

    # 3. Eğer Contact veya About sayfasında bulunamazsa, ana sayfa ve footer'da arayın
    if email == "Siteden Bu Bilgiye Ulaşılamadı" or phone == "Siteden Bu Bilgiye Ulaşılamadı":
        page_content = driver.page_source
        soup = BeautifulSoup(page_content, "html.parser")

        # mailto bağlantılarından e-posta adreslerini ayıkla
        mailto_links = soup.select('a[href^=mailto]')
        tel_links = soup.select('a[href^=tel]')

        emails = [link['href'].replace("mailto:", "") for link in mailto_links]
        phones = [link['href'].replace("tel:", "") for link in tel_links]

        # Ana sayfadaki metin içinde regex ile e-posta ve telefon arama
        emails += re.findall(email_pattern, soup.get_text())
        phones += re.findall(phone_pattern, soup.get_text())
        
        if emails and email == "Siteden Bu Bilgiye Ulaşılamadı":
            email = emails[0]

        if phones and phone == "Siteden Bu Bilgiye Ulaşılamadı":
            for number in phones:
                validated_number = validate_phone_number(number)
                if validated_number:
                    phone = validated_number
                    break

        # Footer kısmında arama yapabilmek için sayfayı kaydırma
        if email == "Siteden Bu Bilgiye Ulaşılamadı" or phone == "Siteden Bu Bilgiye Ulaşılamadı":
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)  # Sayfanın kayması için bekle

            try:
                footer_content = driver.find_element(By.TAG_NAME, "footer").get_attribute("innerHTML")
                soup_footer = BeautifulSoup(footer_content, "html.parser")

                if email == "Siteden Bu Bilgiye Ulaşılamadı":
                    emails_footer = re.findall(email_pattern, soup_footer.get_text())
                    if emails_footer:
                        email = emails_footer[0]

                if phone == "Siteden Bu Bilgiye Ulaşılamadı":
                    phones_footer = re.findall(phone_pattern, soup_footer.get_text())
                    for number in phones_footer:
                        validated_number = validate_phone_number(number)
                        if validated_number:
                            phone = validated_number
                            break
            except Exception as e:
                print(f"Ana sayfa alt kısmında arama yapılamadı: {e}")

    return email, phone


# Reklam olup olmadığını kontrol eden fonksiyon
def is_advertisement(url_text):
    return any(keyword in url_text.lower() for keyword in AD_KEYWORDS)

# Google araması yapma
def google_search(driver, firma_ismi, timeout=3):
    search_query = firma_ismi + " website"
    try:
        driver.get("https://www.google.com")
        search_box = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.NAME, "q"))
        )
        search_box.send_keys(search_query)
        search_box.send_keys(Keys.RETURN)

        website_url, email, phone = None, None, None

        try:
            # İlk 5 arama sonucunu al, kara listedeki siteleri ve reklamları filtrele
            search_results = WebDriverWait(driver, timeout).until(
                EC.presence_of_all_elements_located((By.XPATH, "(//a/h3/..)[position() <= 5]"))
            )

            # Şirket ismi ile en benzer URL'yi bul
            best_match = None
            highest_similarity = 0

            for result in search_results:
                result_url = result.get_attribute("href")
                # Sosyal medya ve reklam içeren siteleri filtrele
                if any(social in result_url for social in BLACKLISTED_SITES) or is_advertisement(result_url):
                    continue

                # URL ve şirket ismi benzerliğini karşılaştır
                similarity = get_similarity(firma_ismi, result_url)
                if similarity > highest_similarity:
                    highest_similarity = similarity
                    best_match = result_url

            # En benzer siteyi seç
            website_url = best_match

            # Geçerli bir site bulduysa devam et
            if website_url:
                driver.get(website_url)
                try:
                    WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                    email, phone = fetch_contact_info(driver)
                except TimeoutException:
                    print(f"{firma_ismi} için site yüklenemedi. Zaman aşımı nedeniyle geçiliyor.")
                    email, phone = "Yüklenemedi", "Yüklenemedi"

        except TimeoutException:
            print(f"{firma_ismi}: İletişim bilgileri çekilemedi - Zaman aşımı")

    except TimeoutException:
        print(f"{firma_ismi}: Google araması yapılamadı - Zaman aşımı")
        website_url, email, phone = None, "Yüklenemedi", "Yüklenemedi"

    return website_url, email, phone


# Verileri görselleştirme
def visualize_data(db_name="company_data.db"):
    conn = sqlite3.connect(db_name)
    df = pd.read_sql_query("SELECT * FROM company_contacts", conn)
    conn.close()
    missing_email = df['email'].isnull().sum()
    missing_phone = df['phone'].isnull().sum()
    plt.bar(['Eksik E-posta', 'Eksik Telefon'], [missing_email, missing_phone])
    plt.title("Eksik İletişim Bilgileri Sayısı")
    plt.ylabel("Eksik Sayısı")
    plt.show()

def google_search_threaded(company_name, conn, cursor):
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    website, email, phone = google_search(driver, company_name)
    if website or email or phone:
        save_to_database(conn, cursor, company_name, website, email, phone)
        print(f"Company Name: {company_name}, Website: {website}, Email: {email}, Phone: {phone}")
    driver.quit()

# Ana fonksiyon
def main():
    create_database()
    conn = sqlite3.connect("company_data.db", check_same_thread=False)
    cursor = conn.cursor()

    file_path = "company_list.xlsx"
    company_names = read_excel(file_path)

    # Thread havuzu ile Google arama işlemlerini paralel hale getir
    with ThreadPoolExecutor(max_workers=12) as executor:  # max_workers ihtiyaca göre ayarlanabilir
        futures = [executor.submit(google_search_threaded, company, conn, cursor) for company in company_names]

        # Tamamlanan her iş parçacığını bekleyip hata durumunu kontrol ediyoruz
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Bir hata oluştu: {e}")

    conn.close()
    visualize_data()

if __name__ == "__main__":
    main() 
