from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
from selenium.common.exceptions import TimeoutException, WebDriverException
from datetime import datetime
import time
import os
import re
import json

def _setup_chrome_options(download_dir=None):
    """Setup common Chrome options including headless mode and user-agent."""
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--allow-running-insecure-content")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    if download_dir:
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safeBrowse.enabled": False
        })
    return chrome_options

def _apply_stealth(driver):
    """Apply selenium-stealth settings to the driver."""
    stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True)

def _init_chrome_driver(chrome_options):
    """Initialize Chrome driver with fallback methods"""
    initialization_attempts = [
        # Method 1: System chromedriver
        lambda: webdriver.Chrome(service=Service('/usr/bin/chromedriver'), options=chrome_options),
        # Method 2: No service specification
        lambda: webdriver.Chrome(options=chrome_options),
        # Method 3: Chromium with explicit binary
        lambda: _init_chromium_fallback(chrome_options),
    ]
    
    for i, attempt in enumerate(initialization_attempts, 1):
        try:
            print(f"🔧 Trying Chrome driver initialization method {i}...")
            driver = attempt()
            print(f"✅ Chrome driver initialized successfully with method {i}")
            return driver
        except Exception as e:
            print(f"⚠️ Method {i} failed: {str(e)}")
            if i == len(initialization_attempts):
                print(f"❌ All Chrome driver initialization methods failed")
                raise Exception(f"Failed to initialize Chrome driver after {len(initialization_attempts)} attempts")

def _init_chromium_fallback(chrome_options):
    """Fallback chromium initialization"""
    chrome_options.binary_location = '/usr/bin/chromium'
    try:
        return webdriver.Chrome(service=Service('/usr/bin/chromedriver'), options=chrome_options)
    except:
        return webdriver.Chrome(options=chrome_options)

def scrape_idx_emiten_names(download_dir, year, max_companies=15):
    """
    Scrapes emiten (company) codes from IDX website for the specified year (Audit reports).
    Saves the list of company names to a text file in download_dir.
    
    Args:
        download_dir (str): Directory to save the output file
        year (int): Year to scrape data for
        max_companies (int): Maximum number of companies to scrape
    """
    chrome_options = _setup_chrome_options() # No specific download dir for this step
    
    # Start Xvfb virtual display
    os.system("Xvfb :99 -ac &")
    time.sleep(2)  # Give Xvfb time to start
    
    driver = _init_chrome_driver(chrome_options)
    
    try:
        _apply_stealth(driver)
        
        url = "https://www.idx.co.id/id/perusahaan-tercatat/laporan-keuangan-dan-tahunan"
        print(f"Accessing URL: {url} for year {year}")
        
        try:
            driver.set_page_load_timeout(120)
            driver.get(url)
        except TimeoutException:
            print("Page load timed out, continuing with current content")
        
        WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
        print("Page loaded successfully")
        
        print("Simulating human behavior...")
        time.sleep(5)
        
        try:
            print(f"\nMemilih tahun {year} dan jenis laporan 'Audit'...")
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)
            
            # Select the specified year
            year_radio = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, f"input[value='{year}'][type='radio']"))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", year_radio)
            time.sleep(1)
            driver.execute_script("arguments[0].click();", year_radio)
            print(f"✓ Tahun {year} telah dipilih.")
            
            audit_radio = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[value='audit'][type='radio'].form-input--check"))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", audit_radio)
            time.sleep(1)
            driver.execute_script("arguments[0].click();", audit_radio)
            print("✓ Jenis laporan 'Audit' (Tahunan) telah dipilih.")

            apply_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'btn--primary') and contains(text(), 'Terapkan')]"))
            )
            driver.execute_script("arguments[0].click();", apply_button)
            print("✓ Tombol 'Terapkan' telah diklik.")
            
            time.sleep(5) # Wait for content to refresh
            
        except Exception as e:
            print(f"✗ Terjadi kesalahan saat memilih tahun dan jenis laporan: {str(e)}")
            raise
        
        company_names = []
        try:
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".box-title")))
            
            print(f"[TESTING MODE] Akan membatasi proses scraping nama emiten hanya {max_companies} perusahaan saja untuk tahun {year}")
            
            page_number = 1
            while True and len(company_names) < max_companies:
                print(f"Processing page {page_number} for year {year}...")
                company_sections = driver.find_elements(By.XPATH, "//div[contains(@class, 'box-title')]")
                print(f"Found {len(company_sections)} company sections on page {page_number}")
                
                for section in company_sections:
                    if len(company_names) >= max_companies:
                        print(f"[TESTING MODE] Sudah mencapai batas {max_companies} perusahaan untuk tahun {year}, berhenti scraping")
                        break
                        
                    try:
                        company_name_element = section.find_element(By.CSS_SELECTOR, "span.f-20")
                        company_name = company_name_element.text.strip()
                        if company_name and company_name not in company_names:
                            company_names.append(company_name)
                            print(f"Found company on page {page_number}: {company_name} ({len(company_names)}/{max_companies})")
                    except Exception as e:
                        print(f"Error extracting company name from section on page {page_number}: {str(e)}")
                        continue
                
                # If we've reached our limit, break out of the loop
                if len(company_names) >= max_companies:
                    print(f"[TESTING MODE] Sudah mencapai batas {max_companies} perusahaan, berhenti pagination")
                    break
                    
                try:
                    next_button = driver.find_element(By.CSS_SELECTOR, "button.btn-arrow.--next")
                    is_next_clickable = next_button.is_enabled() and "disabled" not in next_button.get_attribute("class")
                    
                    if not is_next_clickable:
                        print(f"Next button is disabled, we're on the last page ({page_number})")
                        break
                    
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", next_button)
                    print(f"Clicked next button to go to page {page_number + 1}")
                    time.sleep(5)
                    page_number += 1
                        
                except Exception as e:
                    print(f"No more pages or error clicking next button: {str(e)}")
                    break
                    
        except Exception as e:
            print(f"Error extracting company names: {str(e)}")
            driver.save_screenshot(f"{download_dir}/extraction_error_overall_{year}.png")
            raise
        
        if company_names:
            print(f"[TESTING MODE] Berhasil scrape {len(company_names)} nama perusahaan untuk tahun {year}")
            output_file = os.path.join(download_dir, f"idx_company_names_{year}.txt")
            with open(output_file, 'w') as f:
                for name in company_names:
                    f.write(f"{name}\n")
            print(f"Saved {len(company_names)} company names to {output_file}")
        else:
            print(f"No company names found for year {year}!")
            raise ValueError(f"No company names found after scraping for year {year}.")

        return company_names
        
    except Exception as e:
        print(f"Error occurred during scraping for year {year}: {str(e)}")
        error_path = os.path.join(download_dir, f"idx_error_scrape_{year}.png")
        try:
            driver.save_screenshot(error_path)
            print(f"Error screenshot saved to {error_path}")
        except:
            print("Failed to save error screenshot")
        raise
    finally:
        driver.quit()
        print(f"Browser closed after scraping names for year {year}.")
        os.system("pkill Xvfb")

def download_instances_selenium(company_names, download_dir, year):
    """
    Download instance.zip files using Selenium for a specific year
    
    Args:
        company_names (list): List of company codes
        download_dir (str): Directory to save downloads
        year (int): Year to process
    """
    if not company_names:
        print(f"No company names provided for year {year}")
        return
    
    print(f"Starting direct Selenium downloads for {len(company_names)} companies for year {year}...")
    
    # Dynamic URL template based on year
    base_url_template = f"https://www.idx.co.id/Portals/0/StaticData/ListedCompanies/Corporate_Actions/New_Info_JSX/Jenis_Informasi/01_Laporan_Keuangan/02_Soft_Copy_Laporan_Keuangan//Laporan%20Keuangan%20Tahun%20{year}/Audit/{{company_code}}/instance.zip"
    
    log_file_path = os.path.join(download_dir, f"download_results_selenium_{year}.log")
    
    successful_downloads = 0
    failed_downloads = 0
    
    # Start Xvfb virtual display
    os.system("Xvfb :99 -ac &")
    time.sleep(2)
    
    with open(log_file_path, 'w') as log_file:
        log_file.write(f"Selenium Download started at: {datetime.now()}\n")
        log_file.write(f"Year: {year}\n")
        log_file.write(f"Total companies to process: {len(company_names)}\n\n")
        
        for idx, company_code in enumerate(company_names, 1):
            try:
                company_code = company_code.strip()
                if not company_code:
                    continue
                
                company_dir = os.path.join(download_dir, company_code)
                if not os.path.exists(company_dir):
                    os.makedirs(company_dir)
                
                chrome_options = _setup_chrome_options(company_dir)
                driver = _init_chrome_driver(chrome_options)
                
                try:
                    _apply_stealth(driver)
                    
                    main_url = "https://www.idx.co.id/id/perusahaan-tercatat/laporan-keuangan-dan-tahunan"
                    print(f"  → Establishing session at main page...")
                    driver.get(main_url)
                    time.sleep(3)
                    
                    download_url = base_url_template.format(company_code=company_code)
                    print(f"[{idx}/{len(company_names)}] Year {year} - Selenium navigating for {company_code} to {download_url}")
                    log_file.write(f"[{idx}/{len(company_names)}] Company: {company_code}\n")
                    log_file.write(f"URL: {download_url}\n")
                    
                    driver.get(download_url)
                    time.sleep(8)
                    
                    expected_zip = os.path.join(company_dir, "instance.zip")
                    company_zip = os.path.join(company_dir, f"{company_code}_instance.zip")
                    
                    downloaded_file = None
                    if os.path.exists(expected_zip):
                        downloaded_file = expected_zip
                    elif os.path.exists(company_zip):
                        downloaded_file = company_zip
                    
                    if downloaded_file:
                        file_size = os.path.getsize(downloaded_file)
                        if file_size > 1000:
                            final_path = os.path.join(company_dir, f"{company_code}_instance_{year}.zip")
                            if downloaded_file != final_path:
                                if os.path.exists(final_path):
                                    os.remove(final_path)
                                os.rename(downloaded_file, final_path)
                            
                            print(f"  ✓ Successfully downloaded {company_code}_instance_{year}.zip - {file_size} bytes")
                            log_file.write(f"Status: Success - {file_size} bytes\n")
                            successful_downloads += 1
                        else:
                            print(f"  ✗ File too small: {file_size} bytes (likely error page) for {company_code}")
                            log_file.write(f"Status: Error - File too small {file_size} bytes\n")
                            failed_downloads += 1
                    else:
                        print(f"  ✗ No file downloaded for {company_code}")
                        log_file.write(f"Status: Error - No file downloaded\n")
                        try:
                            screenshot_path = os.path.join(company_dir, f"{company_code}_error_download_{year}.png")
                            driver.save_screenshot(screenshot_path)
                            print(f"  → Error screenshot saved: {screenshot_path}")
                        except:
                            pass
                        failed_downloads += 1
                
                except Exception as e:
                    print(f"  ✗ Selenium error for {company_code}: {str(e)}")
                    log_file.write(f"Status: Selenium Error - {str(e)}\n")
                    failed_downloads += 1
                
                finally:
                    driver.quit()
                
                log_file.write("-" * 50 + "\n")
                time.sleep(2)
                
            except Exception as e:
                print(f"✗ Error processing {company_code}: {str(e)}")
                log_file.write(f"Status: Processing Error - {str(e)}\n")
                log_file.write("-" * 50 + "\n")
                failed_downloads += 1
        
        summary = f"""
Selenium Download Summary for Year {year}:
----------------------------------------
Total companies: {len(company_names)}
Successful downloads: {successful_downloads}
Failed downloads: {failed_downloads}
Success rate: {(successful_downloads/len(company_names)*100):.1f}%
Completion time: {datetime.now()}
"""
        print(summary)
        log_file.write(summary)
    
    # Clean up Xvfb
    os.system("pkill Xvfb")
    
    print(f"Selenium download process completed for year {year}. See {log_file_path} for details.")

def process_year(year):
    """
    Process data extraction for a specific year
    
    Args:
        year (int): Year to process
    """
    download_dir = os.environ.get('IDX_DOWNLOAD_DIR', '/app/output')
    year_dir = os.path.join(download_dir, str(year))
    os.makedirs(year_dir, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"--- Processing Year {year} ---")
    print(f"{'='*60}")
    
    try:
        # Step 1: Scrape company names for this year
        print(f"\n📋 Step 1: Scraping company names for year {year}")
        company_names = scrape_idx_emiten_names(year_dir, year)
        
        if company_names:
            print(f"✅ Successfully obtained {len(company_names)} company names for year {year}")
            print(f"Companies: {', '.join(company_names)}")
            
            # Step 2: Download instance.zip files
            print(f"\n💾 Step 2: Downloading ZIP files for year {year}")
            download_instances_selenium(company_names, year_dir, year)
            
            print(f"✅ Completed processing for year {year}")
        else:
            print(f"❌ No company names found for year {year}")
            
    except Exception as e:
        print(f"❌ Error processing year {year}: {str(e)}")
        raise

def main():
    """Main execution function"""
    years = [2021, 2022, 2023, 2024, 2025]
    
    print("🚀 Starting IDX Multi-Year Extract Process")
    print(f"Years to process: {years}")
    print("="*80)
    
    successful_years = []
    failed_years = []
    
    for year in years:
        try:
            process_year(year)
            successful_years.append(year)
            print(f"✅ Year {year} completed successfully")
            
            # Add delay between years to avoid overwhelming the server
            if year != years[-1]:  # Don't sleep after the last year
                print(f"⏳ Waiting 10 seconds before processing next year...")
                time.sleep(10)
                
        except Exception as e:
            print(f"❌ Year {year} failed: {str(e)}")
            failed_years.append(year)
            continue
    
    # Final summary
    print("\n" + "="*80)
    print("📊 FINAL SUMMARY")
    print("="*80)
    print(f"✅ Successfully processed years: {successful_years}")
    print(f"❌ Failed years: {failed_years}")
    print(f"📈 Success rate: {len(successful_years)}/{len(years)} ({len(successful_years)/len(years)*100:.1f}%)")
    print("="*80)
    print("🎉 IDX Multi-Year Extract Process Completed")

if __name__ == "__main__":
    main()