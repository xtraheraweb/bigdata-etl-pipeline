# pip install pandas yfinance requests-cache
import os
import pandas as pd
import yfinance as yf
import time
import logging
import json
import requests_cache
from datetime import datetime, timedelta
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('yfinance_fetch.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
EXCEL_FILE_PATH = os.getenv('EXCEL_FILE_PATH', 'tickers.xlsx')
JSON_OUTPUT_PATH = os.getenv('YFINANCE_OUTPUT_PATH', 'tickers_data.json')
YEARS_BACK = 5

# OPTIMIZED RATE LIMITING CONFIGURATION
MAX_STOCKS = 950
MAX_WORKERS = 8  # Parallel processing
DELAY_BETWEEN_REQUESTS = 0.5  # Reduced delay between requests
BATCH_SIZE = 20  # Increased batch size
DELAY_BETWEEN_BATCHES = 10  # Reduced delay between batches
MAX_RETRIES = 2  # Reduced retries for faster processing

# Enable caching to avoid repeated requests
session = requests_cache.CachedSession('yfinance_cache', expire_after=3600)
yf.utils.requests_cache = session

# Thread-safe counter for progress tracking
class ProgressCounter:
    def __init__(self):
        self.lock = threading.Lock()
        self.successful = 0
        self.failed = 0
        self.processed = 0
    
    def increment_success(self):
        with self.lock:
            self.successful += 1
            self.processed += 1
    
    def increment_failure(self):
        with self.lock:
            self.failed += 1
            self.processed += 1
    
    def get_stats(self):
        with self.lock:
            return self.successful, self.failed, self.processed

def calculate_date_range():
    """Calculate start and end dates for the last 5 years"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=YEARS_BACK * 365)
    
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

def read_tickers_from_excel(file_path):
    """Read ticker symbols from Excel file"""
    try:
        df = pd.read_excel(file_path)
        
        # Handle different possible column names
        ticker_column = None
        for col in df.columns:
            if col.lower() in ['ticker', 'symbol', 'code', 'stock']:
                ticker_column = col
                break
        
        if ticker_column is None:
            raise ValueError("No ticker column found. Expected columns: 'Ticker', 'Symbol', 'Code', or 'Stock'")
        
        tickers = df[ticker_column].astype(str).tolist()
        
        # Add .JK suffix for Indonesian stocks if not present
        tickers = [ticker + ".JK" if not ticker.endswith(".JK") else ticker for ticker in tickers]
        
        # Limit to MAX_STOCKS
        tickers = tickers[:MAX_STOCKS]
        
        logger.info(f"Successfully loaded {len(tickers)} tickers from {file_path}")
        return tickers
    
    except Exception as e:
        logger.error(f"Failed to read Excel file {file_path}: {str(e)}")
        raise

def fetch_ticker_data_optimized(ticker, start_date, end_date, counter, progress_callback=None):
    """Optimized fetch data for a single ticker"""
    attempt = 0
    
    while attempt < MAX_RETRIES:
        try:
            attempt += 1
            
            # Minimal delay with randomization
            delay = random.uniform(0.1, 0.3)
            time.sleep(delay)
            
            # Use yfinance with timeout
            stock = yf.Ticker(ticker)
            
            # Get historical data only (skip info for speed)
            hist_data = stock.history(
                start=start_date, 
                end=end_date, 
                timeout=15,  # Reduced timeout
                raise_errors=False  # Don't raise errors for missing data
            )
            
            if hist_data.empty:
                logger.debug(f"No data for {ticker}")
                counter.increment_failure()
                return None
            
            # Convert historical data efficiently
            history_records = []
            for date, row in hist_data.iterrows():
                history_records.append({
                    "Date": date.strftime('%Y-%m-%d'),
                    "Open": float(row['Open']) if pd.notna(row['Open']) else 0.0,
                    "High": float(row['High']) if pd.notna(row['High']) else 0.0,
                    "Low": float(row['Low']) if pd.notna(row['Low']) else 0.0,
                    "Close": float(row['Close']) if pd.notna(row['Close']) else 0.0,
                    "Volume": int(row['Volume']) if pd.notna(row['Volume']) else 0,
                    "Dividends": float(row['Dividends']) if 'Dividends' in row and pd.notna(row['Dividends']) else 0.0,
                    "Stock Splits": float(row['Stock Splits']) if 'Stock Splits' in row and pd.notna(row['Stock Splits']) else 0.0
                })
            
            # Minimal info structure
            data = {
                "info": {"symbol": ticker},
                "history": history_records,
                "fetch_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "data_period": f"{start_date} to {end_date}",
                "total_records": len(history_records)
            }
            
            counter.increment_success()
            
            if progress_callback:
                progress_callback(ticker, True, len(history_records))
            
            return data
            
        except Exception as e:
            error_msg = str(e)
            
            # Handle rate limiting
            if "429" in error_msg or "Too Many Requests" in error_msg:
                if attempt < MAX_RETRIES:
                    wait_time = min(5 * attempt, 15)  # Shorter backoff
                    time.sleep(wait_time)
                    continue
                else:
                    logger.debug(f"Rate limited {ticker} after {MAX_RETRIES} attempts")
                    break
            else:
                # Other errors, retry once quickly
                if attempt < MAX_RETRIES:
                    time.sleep(1)
                    continue
                else:
                    logger.debug(f"Failed {ticker}: {error_msg}")
                    break
    
    counter.increment_failure()
    if progress_callback:
        progress_callback(ticker, False, 0)
    
    return None

def fetch_all_data_parallel(tickers, start_date, end_date):
    """Fetch data for all tickers using parallel processing"""
    all_data = []
    counter = ProgressCounter()
    total_tickers = len(tickers)
    
    logger.info(f"Starting parallel fetch for {total_tickers} tickers")
    logger.info(f"Using {MAX_WORKERS} parallel workers")
    logger.info(f"Date range: {start_date} to {end_date}")
    
    # Progress callback
    def progress_callback(ticker, success, records):
        successful, failed, processed = counter.get_stats()
        if processed % 50 == 0 or processed == total_tickers:
            progress_pct = (processed / total_tickers) * 100
            logger.info(f"Progress: {processed}/{total_tickers} ({progress_pct:.1f}%) | Success: {successful} | Failed: {failed}")
    
    # Process in batches to control memory usage
    batch_results = []
    
    for batch_start in range(0, total_tickers, BATCH_SIZE * MAX_WORKERS):
        batch_end = min(batch_start + (BATCH_SIZE * MAX_WORKERS), total_tickers)
        batch_tickers = tickers[batch_start:batch_end]
        
        batch_num = (batch_start // (BATCH_SIZE * MAX_WORKERS)) + 1
        total_batches = (total_tickers + (BATCH_SIZE * MAX_WORKERS) - 1) // (BATCH_SIZE * MAX_WORKERS)
        
        logger.info(f"\n=== BATCH {batch_num}/{total_batches} ===")
        logger.info(f"Processing {len(batch_tickers)} tickers in parallel...")
        
        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            future_to_ticker = {
                executor.submit(fetch_ticker_data_optimized, ticker, start_date, end_date, counter, progress_callback): ticker 
                for ticker in batch_tickers
            }
            
            # Collect results as they complete
            batch_data = []
            for future in as_completed(future_to_ticker):
                result = future.result()
                if result:
                    batch_data.append(result)
        
        batch_results.extend(batch_data)
        
        # Small delay between batches
        if batch_end < total_tickers:
            time.sleep(DELAY_BETWEEN_BATCHES)
    
    successful, failed, processed = counter.get_stats()
    
    logger.info(f"\n=== FINAL RESULTS ===")
    logger.info(f"Total processed: {processed}")
    logger.info(f"Successful: {successful}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Success rate: {(successful/processed*100):.1f}%")
    
    return batch_results, successful, failed

def save_to_json(data, file_path):
    """Save data to JSON file with compression"""
    try:
        # Custom JSON encoder
        def json_serializer(obj):
            if isinstance(obj, (datetime, pd.Timestamp)):
                return obj.isoformat()
            elif pd.isna(obj):
                return None
            return str(obj)
        
        with open(file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=None, separators=(',', ':'), default=json_serializer)
        
        logger.info(f"✅ Data successfully saved to {file_path}")
        
        # Log file size
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
        logger.info(f"📁 File size: {file_size:.2f} MB")
        
    except Exception as e:
        logger.error(f"Failed to save data to {file_path}: {str(e)}")
        raise

def main():
    """Main execution function"""
    start_time = time.time()
    
    logger.info("=" * 70)
    logger.info("OPTIMIZED YFINANCE DATA FETCHER - 950 STOCKS")
    logger.info("=" * 70)
    
    try:
        # Calculate date range
        start_date, end_date = calculate_date_range()
        logger.info(f"Fetching data from {start_date} to {end_date}")
        
        # Read tickers from Excel
        tickers = read_tickers_from_excel(EXCEL_FILE_PATH)
        
        # Fetch all data with parallel processing
        all_data, successful, failed = fetch_all_data_parallel(tickers, start_date, end_date)
        
        # Save to JSON
        if all_data:
            save_to_json(all_data, JSON_OUTPUT_PATH)
        else:
            logger.error("No data to save!")
            save_to_json([], JSON_OUTPUT_PATH)
            return
        
        # Summary
        execution_time = time.time() - start_time
        
        logger.info("\n" + "=" * 70)
        logger.info("EXECUTION SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Total tickers processed: {len(tickers)}")
        logger.info(f"Successful fetches: {successful}")
        logger.info(f"Failed fetches: {failed}")
        logger.info(f"Success rate: {(successful/len(tickers)*100):.1f}%")
        logger.info(f"Total execution time: {execution_time/60:.1f} minutes")
        logger.info(f"Average time per stock: {execution_time/len(tickers):.2f} seconds")
        logger.info(f"Output file: {JSON_OUTPUT_PATH}")
        
        # Calculate total records
        total_records = sum(len(item.get('history', [])) for item in all_data)
        logger.info(f"Total historical records: {total_records:,}")
        
        if successful > 0:
            logger.info("\n🎉 Data fetching completed successfully!")
            logger.info(f"You can now run your MongoDB loader script with INPUT_PATH={JSON_OUTPUT_PATH}")
        else:
            logger.warning("\n⚠️ No data was successfully fetched. Check your internet connection and ticker symbols.")
        
    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()