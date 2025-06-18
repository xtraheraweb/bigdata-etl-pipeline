from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import re
import json
import os

# URL dasar dari situs berita
base_url = "http://www.iqplus.info"

# Konfigurasi untuk berbagai jenis berita
news_configs = {
    "market_news": {
        "start_url": base_url + "/news/market_news/go-to-page,1.html",
        "url_pattern": base_url + "/news/market_news/go-to-page,{}.html",
        "output_file": "market_news.json"
    },
    "stock_news": {
        "start_url": base_url + "/news/stock_news/go-to-page,0.html",
        "url_pattern": base_url + "/news/stock_news/go-to-page,{}.html",
        "output_file": "stock_news.json"
    }
}

# Buat folder output jika belum ada
output_dir = "output"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Konfigurasi opsi untuk Selenium agar berjalan tanpa GUI
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-web-security")
chrome_options.add_argument("--allow-running-insecure-content")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-plugins")
chrome_options.add_argument("--disable-images")
chrome_options.add_argument("--disable-javascript")

# FIXED: Use system chromium-driver instead of ChromeDriverManager
print("🔧 Using system chromium-driver...")

try:
    # Try using system chromium-driver directly
    service = Service('/usr/bin/chromedriver')  # System chromedriver path
    driver = webdriver.Chrome(service=service, options=chrome_options)
    print("✅ Successfully initialized Chrome with system driver")
except Exception as e1:
    print(f"⚠️ System chromedriver failed: {e1}")
    try:
        # Fallback: try chromium binary directly
        chrome_options.binary_location = '/usr/bin/chromium'
        service = Service('/usr/bin/chromedriver')
        driver = webdriver.Chrome(service=service, options=chrome_options)
        print("✅ Successfully initialized Chromium with system driver")
    except Exception as e2:
        print(f"⚠️ Chromium fallback failed: {e2}")
        try:
            # Last resort: try without service specification
            driver = webdriver.Chrome(options=chrome_options)
            print("✅ Successfully initialized Chrome without service specification")
        except Exception as e3:
            print(f"❌ All Chrome initialization methods failed:")
            print(f"   - System chromedriver: {e1}")
            print(f"   - Chromium fallback: {e2}")
            print(f"   - No service: {e3}")
            raise Exception("Failed to initialize Chrome/Chromium driver")

wait = WebDriverWait(driver, 10)

# Fungsi untuk mendapatkan nomor halaman terakhir dari pagination
def get_last_page(start_url):
    driver.get(start_url)
    try:
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "nav")))
        soup = BeautifulSoup(driver.page_source, "html.parser")
        nav_span = soup.find("span", class_="nav")
        
        if nav_span:
            last_page_link = nav_span.find_all("a")[-2]
            if last_page_link and last_page_link.text.isdigit():
                return int(last_page_link.text)
    except Exception as e:
        print(f"Error mendapatkan halaman terakhir dari {start_url}: {e}")
    return 1  

# Fungsi untuk mengambil isi artikel berdasarkan URL yang diberikan
def scrape_article_content(article_url):
    full_url = base_url + article_url if not article_url.startswith("http") else article_url
    try:
        driver.get(full_url)
        wait.until(EC.presence_of_element_located((By.ID, "zoomthis")))
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        zoom_div = soup.find("div", id="zoomthis")
        if not zoom_div:
            print(f"Konten artikel tidak ditemukan di {full_url}")
            return None, None
        
        date_element = zoom_div.find("small")
        date_text = date_element.text.strip() if date_element else "Tanggal tidak tersedia"
        
        if date_element:
            date_element.extract()
        
        title_element = zoom_div.find("h3")
        if title_element:
            title_element.extract()
        
        zoom_control = zoom_div.find("div", attrs={"align": "right"})
        if zoom_control:
            zoom_control.extract()
        
        content = zoom_div.get_text(strip=True)
        content = re.sub(r'\s+', ' ', content).strip()
        
        return date_text, content
    except Exception as e:
        print(f"Error saat scraping artikel {full_url}: {e}")
        return None, None

# Fungsi untuk mengambil daftar berita dari satu halaman
def scrape_page(url):
    driver.get(url)
    try:
        wait.until(EC.presence_of_element_located((By.ID, "load_news")))
        time.sleep(2)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        news_list = soup.select_one("#load_news .box ul.news")
        if not news_list:
            print("Elemen berita tidak ditemukan.")
            return []
        
        news_items = news_list.find_all("li")
        scraped_data = []
        
        for index, item in enumerate(news_items):
            time_text = item.find("b").text.strip() if item.find("b") else "Tidak ada waktu"
            title_tag = item.find("a")
            
            if title_tag and title_tag.has_attr("href"):
                title = title_tag.text.strip()
                link = title_tag["href"]
                full_link = base_url + link if not link.startswith("http") else link
                
                print(f"Scraping artikel {index+1}/{len(news_items)}: {title}")
                article_date, article_content = scrape_article_content(link)
                
                scraped_data.append({
                    "judul": title, 
                    "waktu": time_text, 
                    "link": full_link,
                    "tanggal_artikel": article_date,
                    "konten": article_content
                })
                
                time.sleep(1)
        
        return scraped_data
    except Exception as e:
        print(f"Error saat scraping {url}: {e}")
        return []

# Fungsi untuk scraping semua halaman untuk jenis berita tertentu
def scrape_news_type(news_type, config):
    print(f"\n=== Memulai scraping {news_type.upper()} ===")
    
    # Dapatkan halaman terakhir
    last_page = 2  # Ganti dengan 2 untuk testing, seharusnya menggunakan get_last_page(config["start_url"])
    print(f"Halaman terakhir untuk {news_type}: {last_page}")
    
    # Tentukan start page berdasarkan jenis berita
    start_page = 0 if news_type == "stock_news" else 1
    
    # Loop untuk mengunjungi setiap halaman
    all_articles = []
    for page in range(start_page, last_page + 1):
        page_url = config["url_pattern"].format(page)
        print(f"Scraping halaman: {page_url}")
        all_articles.extend(scrape_page(page_url))
    
    # Simpan data ke file JSON di folder output
    output_path = os.path.join(output_dir, config["output_file"])
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_articles, f, ensure_ascii=False, indent=4)
    
    print(f"Data {news_type} disimpan dalam '{output_path}'")
    print(f"Total artikel {news_type}: {len(all_articles)}")
    
    return all_articles

# Main execution
try:
    # Scrape market news
    market_articles = scrape_news_type("market_news", news_configs["market_news"])
    
    # Scrape stock news
    stock_articles = scrape_news_type("stock_news", news_configs["stock_news"])
    
    print(f"\n=== RINGKASAN SCRAPING ===")
    print(f"Total artikel market news: {len(market_articles)}")
    print(f"Total artikel stock news: {len(stock_articles)}")
    print(f"File disimpan di folder: {output_dir}/")

except Exception as e:
    print(f"Error dalam proses scraping: {e}")

finally:
    # Tutup browser Selenium
    try:
        driver.quit()
        print("Scraping selesai.")
    except:
        print("Browser sudah ditutup.")