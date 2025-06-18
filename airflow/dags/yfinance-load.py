import json
import os
import logging
import time
from datetime import datetime, timedelta
from pymongo import MongoClient, errors, ASCENDING
from pymongo.operations import InsertOne
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# MongoDB connection details
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb+srv://bigdatakecil:bigdata04@xtrahera.m7x7qad.mongodb.net/?retryWrites=true&w=majority&appName=xtrahera')
MONGO_DB = os.environ.get('MONGO_DB', 'tugas_bigdata')
INPUT_FILE_PATH = os.environ.get('INPUT_PATH', '/app/output/tickers_data.json')

# Optimized collections configuration
COLLECTIONS = {
    'yfinance_data_harian': {'period': 'daily', 'stock_limit': 100},  # Most docs
    'yfinance_data_mingguan': {'period': 'weekly', 'stock_limit': 200},
    'yfinance_data_bulanan': {'period': 'monthly', 'stock_limit': 400},
    'yfinance_data_tahunan': {'period': 'yearly', 'stock_limit': 950},  # Least docs
    'yfinance_data_lima_tahun': {'period': '5years', 'stock_limit': 950},
    'yfinance_data_satu_tahun': {'period': '1year', 'stock_limit': 950},
    'yfinance_data_tiga_tahun': {'period': '3years', 'stock_limit': 950}
}

class OptimizedDataProcessor:
    def __init__(self):
        self.lock = threading.Lock()
        self.processed_count = 0
    
    def group_data_by_period(self, history, period_type, symbol):
        """Optimized data grouping by period"""
        grouped_data = {}
        
        for entry in history:
            try:
                date_str = entry.get("Date", "")
                if not date_str:
                    continue
                    
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                
                # Efficient period key generation
                if period_type == 'daily':
                    key = date_str
                    bulan_value = date_str
                elif period_type == 'weekly':
                    # Use ISO week
                    year, week, _ = date_obj.isocalendar()
                    key = f"{year}-W{week:02d}"
                    bulan_value = key
                elif period_type == 'monthly':
                    key = date_str[:7]  # YYYY-MM
                    bulan_value = key
                elif period_type == 'yearly':
                    key = date_str[:4]  # YYYY
                    bulan_value = key
                elif period_type in ['1year', '3years', '5years']:
                    key = date_str[:7]  # Group by month
                    bulan_value = f"{key}-{period_type}"
                else:
                    continue
                
                # Initialize or update group
                if key not in grouped_data:
                    grouped_data[key] = {
                        'entries': [entry],
                        'bulan_value': bulan_value,
                        'start_date': date_str,
                        'end_date': date_str
                    }
                else:
                    grouped_data[key]['entries'].append(entry)
                    if date_str > grouped_data[key]['end_date']:
                        grouped_data[key]['end_date'] = date_str
                        
            except Exception as e:
                continue
        
        return grouped_data
    
    def aggregate_entries_fast(self, entries):
        """Fast aggregation of entries"""
        if not entries:
            return None
            
        if len(entries) == 1:
            entry = entries[0]
            return {
                "Open": float(entry.get("Open", 0)) or 0.0,
                "Close": float(entry.get("Close", 0)) or 0.0,
                "Low": float(entry.get("Low", 0)) or 0.0,
                "High": float(entry.get("High", 0)) or 0.0,
                "AvgVolume": int(entry.get("Volume", 0)) or 0,
                "MaxVolume": int(entry.get("Volume", 0)) or 0,
            }
        
        # Multiple entries - use list comprehensions for speed
        entries.sort(key=lambda x: x.get("Date", ""))
        
        opens = [float(e.get("Open", 0)) for e in entries if e.get("Open") and float(e.get("Open", 0)) > 0]
        closes = [float(e.get("Close", 0)) for e in entries if e.get("Close") and float(e.get("Close", 0)) > 0]
        highs = [float(e.get("High", 0)) for e in entries if e.get("High") and float(e.get("High", 0)) > 0]
        lows = [float(e.get("Low", 0)) for e in entries if e.get("Low") and float(e.get("Low", 0)) > 0]
        volumes = [int(e.get("Volume", 0)) for e in entries if e.get("Volume") and int(e.get("Volume", 0)) > 0]
        
        return {
            "Open": opens[0] if opens else 0.0,
            "Close": closes[-1] if closes else 0.0,
            "High": max(highs) if highs else 0.0,
            "Low": min(lows) if lows else 0.0,
            "AvgVolume": sum(volumes) // len(volumes) if volumes else 0,
            "MaxVolume": max(volumes) if volumes else 0,
        }

def transform_for_collection_optimized(original_data, collection_name, config):
    """Optimized transformation for collection"""
    processor = OptimizedDataProcessor()
    period_type = config['period']
    stock_limit = config['stock_limit']
    
    # Limit stocks based on configuration
    limited_data = original_data[:stock_limit]
    
    logger.info(f"🔄 Processing {collection_name}: {len(limited_data)} stocks, {period_type} periods")
    
    all_documents = []
    
    for stock_data in limited_data:
        try:
            info = stock_data.get("info", {})
            history = stock_data.get("history", [])
            symbol = info.get("symbol", "UNKNOWN")
            
            if not history:
                continue
            
            # Group data by period
            grouped_data = processor.group_data_by_period(history, period_type, symbol)
            
            # Create documents for each period
            for period_key, period_data in grouped_data.items():
                aggregated = processor.aggregate_entries_fast(period_data['entries'])
                if not aggregated:
                    continue
                
                document = {
                    "Bulan": period_data['bulan_value'],
                    "StartDate": period_data['start_date'] + "T00:00:00.000+00:00",
                    "EndDate": period_data['end_date'] + "T00:00:00.000+00:00",
                    "Open": aggregated["Open"],
                    "Close": aggregated["Close"],
                    "Low": aggregated["Low"],
                    "High": aggregated["High"],
                    "AvgVolume": aggregated["AvgVolume"],
                    "MaxVolume": aggregated["MaxVolume"],
                    "ticker": symbol
                }
                
                all_documents.append(document)
                    
        except Exception as e:
            continue
    
    logger.info(f"✅ {collection_name}: {len(all_documents):,} documents prepared")
    return all_documents

def bulk_insert_optimized(collection, documents, collection_name):
    """Optimized bulk insert with better error handling"""
    if not documents:
        return 0
    
    inserted_count = 0
    batch_size = 2000  # Larger batch size for better performance
    total_batches = (len(documents) + batch_size - 1) // batch_size
    
    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(documents))
        batch_docs = documents[start_idx:end_idx]
        
        try:
            # Use bulk_write for better performance
            operations = [InsertOne(doc) for doc in batch_docs]
            result = collection.bulk_write(operations, ordered=False)
            inserted_count += result.inserted_count
            
            if batch_idx % 5 == 0 or batch_idx == total_batches - 1:
                progress = ((batch_idx + 1) / total_batches) * 100
                logger.info(f"  {collection_name}: {progress:.1f}% ({inserted_count:,} docs)")
                
        except Exception as e:
            logger.warning(f"  Batch {batch_idx + 1} error: {str(e)[:100]}")
    
    return inserted_count

def create_indexes_optimized(db, collection_name):
    """Create optimized indexes"""
    try:
        collection = db[collection_name]
        
        # Create compound index for better query performance
        collection.create_index([
            ("ticker", ASCENDING),
            ("Bulan", ASCENDING)
        ])
        
        # Single field indexes
        collection.create_index("ticker")
        collection.create_index("Bulan")
        
        logger.debug(f"Indexes created for {collection_name}")
        
    except Exception as e:
        logger.warning(f"Failed to create indexes for {collection_name}: {str(e)}")

def process_single_collection(args):
    """Process a single collection - for parallel processing"""
    original_data, collection_name, config, mongo_uri, db_name = args
    
    try:
        # Create separate connection for this thread
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
        db = client[db_name]
        collection = db[collection_name]
        
        # Drop existing collection for clean start
        collection.drop()
        
        # Transform data
        documents = transform_for_collection_optimized(original_data, collection_name, config)
        
        # Insert documents
        inserted_count = bulk_insert_optimized(collection, documents, collection_name)
        
        # Create indexes
        create_indexes_optimized(db, collection_name)
        
        client.close()
        
        return {
            'collection': collection_name,
            'documents': inserted_count,
            'success': True
        }
        
    except Exception as e:
        logger.error(f"❌ Error processing {collection_name}: {str(e)}")
        return {
            'collection': collection_name,
            'documents': 0,
            'success': False,
            'error': str(e)
        }

def load_to_all_collections_parallel():
    """Load data to all collections using parallel processing"""
    start_time = time.time()

    logger.info("=" * 70)
    logger.info("🚀 OPTIMIZED PARALLEL MONGODB LOADER")
    logger.info("=" * 70)
    logger.info(f"Loading from: {INPUT_FILE_PATH}")
    logger.info(f"Target database: {MONGO_DB}")
    logger.info(f"Collections: {len(COLLECTIONS)}")

    # Load JSON data
    try:
        with open(INPUT_FILE_PATH, 'r', encoding='utf-8') as f:
            original_data = json.load(f)
        logger.info(f"📁 Loaded {len(original_data)} stock records")
        
    except Exception as e:
        logger.error(f"Failed to read input file: {str(e)}")
        raise

    # Test MongoDB connection
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        client.close()
        logger.info(f"🔗 MongoDB connection verified")
    except Exception as e:
        logger.error(f"MongoDB connection failed: {str(e)}")
        raise

    # Prepare arguments for parallel processing
    process_args = [
        (original_data, collection_name, config, MONGO_URI, MONGO_DB)
        for collection_name, config in COLLECTIONS.items()
    ]

    # Process collections in parallel
    results = []
    max_workers = min(4, len(COLLECTIONS))  # Limit concurrent connections
    
    logger.info(f"Processing {len(COLLECTIONS)} collections with {max_workers} parallel workers...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_collection = {
            executor.submit(process_single_collection, args): args[1] 
            for args in process_args
        }
        
        for future in as_completed(future_to_collection):
            result = future.result()
            results.append(result)
            
            if result['success']:
                logger.info(f"✅ {result['collection']}: {result['documents']:,} documents")
            else:
                logger.error(f"❌ {result['collection']}: {result.get('error', 'Unknown error')}")

    # Calculate summary
    successful_collections = sum(1 for r in results if r['success'])
    total_documents = sum(r['documents'] for r in results)
    elapsed = time.time() - start_time

    # Final summary
    logger.info("\n" + "=" * 70)
    logger.info("📊 FINAL SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Database: {MONGO_DB}")
    logger.info(f"Successful collections: {successful_collections}/{len(COLLECTIONS)}")
    logger.info(f"Total documents: {total_documents:,}")
    logger.info(f"Execution time: {elapsed:.1f} seconds")
    logger.info(f"Documents per second: {total_documents/elapsed:.0f}")
    
    if successful_collections == len(COLLECTIONS):
        logger.info("🎉 ALL COLLECTIONS CREATED SUCCESSFULLY!")
        
        # Clean up input file
        try:
            os.remove(INPUT_FILE_PATH)
            logger.info(f"🗑️ Cleaned up input file")
        except:
            pass
    else:
        logger.warning(f"⚠️ {successful_collections}/{len(COLLECTIONS)} collections successful")

    return {
        "status": "success" if successful_collections == len(COLLECTIONS) else "partial_success",
        "successful_collections": successful_collections,
        "total_documents": total_documents,
        "execution_time_seconds": elapsed,
        "collections_created": [r['collection'] for r in results if r['success']]
    }

if __name__ == "__main__":
    result = load_to_all_collections_parallel()
    
    logger.info(f"\n🏁 Status: {result['status']}")
    logger.info(f"📈 Created: {result['successful_collections']}/{len(COLLECTIONS)} collections")
    logger.info(f"📊 Total: {result['total_documents']:,} documents")
    logger.info(f"⏱️ Time: {result['execution_time_seconds']:.1f} seconds")