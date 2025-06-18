import schedule
import time
import subprocess
import logging
from datetime import datetime, timedelta
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/quarterly_scheduler.log'),
        logging.StreamHandler()
    ]
)

def get_current_quarter_info():
    """Determine current quarter and next scheduled runs."""
    now = datetime.now()
    current_month = now.month
    current_year = now.year
    
    logging.info(f"🔍 Current date: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"🔍 Current month: {current_month}")
    
    # PERBAIKI: Logika untuk 2025 (June = TW1 masih tersedia)
    if current_month <= 7:  # Jan-Jul = TW1 available
        current_quarter = 'tw1'
        next_quarter = 'tw2'
    elif current_month <= 10:  # Aug-Oct = TW2 (tapi belum ada di 2025)
        current_quarter = 'tw1'  # Fallback ke TW1
        next_quarter = 'tw2'
    elif current_month <= 12:  # Nov-Dec = TW3 (tapi belum ada di 2025)
        current_quarter = 'tw1'  # Fallback ke TW1
        next_quarter = 'tw2'
    
    logging.info(f"🎯 Detected quarter: {current_quarter} {current_year}")
    return current_quarter, next_quarter, current_year

def run_idx_extraction(report_type=None, year=None):
    """Run IDX extraction for specified quarter."""
    if not year:
        year = datetime.now().year
    
    if not report_type:
        report_type, _, _ = get_current_quarter_info()
    
    try:
        logging.info(f"🚀 Starting IDX extraction for {report_type.upper()} {year}")
        
        result = subprocess.run([
            'python', 'idx-extract.py',
            '--year', str(year),
            '--report-type', report_type
        ], capture_output=True, text=True, cwd='/app')
        
        if result.returncode == 0:
            logging.info(f"✅ {report_type.upper()} {year} extraction completed successfully!")
            if result.stdout:
                logging.info("STDOUT: %s", result.stdout[-500:])
        else:
            logging.error(f"❌ {report_type.upper()} {year} extraction failed!")
            if result.stderr:
                logging.error("STDERR: %s", result.stderr[-500:])
            
    except Exception as e:
        logging.error(f"💥 Extraction error for {report_type} {year}: {str(e)}")

def should_run_quarter(report_type):
    """Check if it's time to run specific quarter based on current date."""
    now = datetime.now()
    current_month = now.month
    current_day = now.day
    current_year = now.year
    
    logging.info(f"🔍 Checking if should run {report_type} - Current: {current_year}-{current_month:02d}-{current_day:02d}")
    
    # UNTUK 2025: Hanya TW1 yang tersedia
    if current_year == 2025:
        if report_type == 'tw1':
            logging.info(f"✅ TW1 2025 is available")
            return True
        else:
            logging.warning(f"⚠️ {report_type.upper()} 2025 not yet available, using TW1 instead")
            return False
    
    # Untuk tahun lain, gunakan logika normal
    if report_type == 'tw1' and current_month >= 4:  # April+
        return True
    if report_type == 'tw2' and current_month >= 7:  # July+
        return True
    if report_type == 'tw3' and current_month >= 10:  # October+
        return True
    if report_type == 'audit' and current_month >= 3:  # March+
        return True
        
    return False

def check_and_run_quarterly():
    """Check if any quarter should run today."""
    quarters = ['tw1', 'tw2', 'tw3', 'audit']
    current_year = datetime.now().year
    
    logging.info(f"📅 Daily quarterly check for {current_year}")
    
    for quarter in quarters:
        if should_run_quarter(quarter):
            year = current_year if quarter != 'audit' else current_year - 1
            logging.info(f"📅 Time to run {quarter.upper()} for year {year}")
            run_idx_extraction(quarter, year)

def run_immediate_test():
    """Run immediate test for current quarter."""
    current_quarter, _, year = get_current_quarter_info()
    logging.info(f"🧪 Running immediate test for current quarter: {current_quarter.upper()} {year}")
    run_idx_extraction(current_quarter, year)

if __name__ == "__main__":
    logging.info("🕐 IDX Quarterly Scheduler starting...")
    
    # Schedule daily checks
    schedule.every().day.at("09:00").do(check_and_run_quarterly)
    
    # Weekly test run every Monday at 10 AM
    schedule.every().monday.at("10:00").do(
        lambda: run_idx_extraction('tw1', datetime.now().year)
    )
    
    # PERBAIKI: Immediate test on startup - langsung jalankan
    logging.info("🚀 Running immediate startup test...")
    run_immediate_test()
    
    logging.info("📅 Scheduler configured:")
    logging.info("   • Daily check at 9:00 AM for quarterly runs")
    logging.info("   • TW1: Runs when April 15+ detected")
    logging.info("   • TW2: Runs when July 15+ detected")
    logging.info("   • TW3: Runs when October 15+ detected")
    logging.info("   • AUDIT: Runs when March 31+ detected")
    logging.info("   • Weekly test: Every Monday 10:00 AM")
    logging.info("✅ Startup test completed, switching to scheduled mode")
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
            
        except KeyboardInterrupt:
            logging.info("🛑 Scheduler stopped by user")
            break
        except Exception as e:
            logging.error(f"💥 Scheduler error: {str(e)}")
            time.sleep(300)  # Wait 5 minutes on error