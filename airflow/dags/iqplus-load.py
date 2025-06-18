import json
import os
import logging
import time
from datetime import datetime
from pymongo import MongoClient, errors

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# MongoDB connection details via env (default fallback)
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb+srv://bigdatakecil:bigdata04@xtrahera.m7x7qad.mongodb.net/?retryWrites=true&w=majority&appName=xtrahera')
MONGO_DB = os.environ.get('MONGO_DB', 'tugas_bigdata')  # Database tunggal

# Definisi file input dan koleksi target dengan prefix untuk menghindari konflik
COLLECTION_MAPPINGS = {
    "market_news.json": "iqplus_market_news",
    "stock_news.json": "iqplus_stock_news"
}

# Direktori tempat file JSON berada (sesuai volume mapping)
INPUT_DIR = os.environ.get('INPUT_DIR', '/app/output')

def load_to_collection(client, db_name, collection_name, data):
    """
    Load data to specific collection in MongoDB
    """
    try:
        db = client[db_name]
        collection = db[collection_name]

        logger.info(f"📊 Loading data to {db_name}.{collection_name}")

        if not data:
            logger.warning(f"No data to load for {collection_name}")
            return 0

        # Process data in batches for better performance
        batch_size = 1000
        total_batches = (len(data) + batch_size - 1) // batch_size
        insert_count = 0

        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min((batch_idx + 1) * batch_size, len(data))
            batch_data = data[start_idx:end_idx]

            try:
                # Insert batch data
                # Menggunakan insert_many untuk efisiensi
                # ordered=False memungkinkan insertion untuk lanjut meskipun ada error duplikat
                result = collection.insert_many(batch_data, ordered=False)
                insert_count += len(result.inserted_ids)

                if batch_idx % 1 == 0 or batch_idx == total_batches - 1: # Log setiap batch
                    logger.info(f"  Batch {batch_idx + 1}/{total_batches}: {len(result.inserted_ids)} docs inserted")

            except errors.BulkWriteError as bwe:
                # Menangani error duplikat atau error lainnya di batch
                logger.warning(f"  BulkWriteError in batch {batch_idx + 1}: {bwe.details}")
                # Count successfully inserted docs from the error report
                insert_count += bwe.details.get('nInserted', 0)
            except Exception as e:
                logger.error(f"  Error inserting batch {batch_idx + 1}: {str(e)}")

        logger.info(f"✅ {collection_name}: {insert_count:,} documents inserted")
        return insert_count

    except Exception as e:
        logger.error(f"❌ Error loading to {collection_name}: {str(e)}")
        return 0

def process_iqplus_data():
    """
    Main function to load IQPlus data to MongoDB collections.
    """
    start_time = time.time()

    logger.info("=" * 70)
    logger.info("🚀 IQPLUS DATA LOADER TO MONGODB ATLAS")
    logger.info("=" * 70)
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Input directory: {INPUT_DIR}")
    logger.info(f"Target database: {MONGO_DB}")

    # Connect to MongoDB Atlas
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000) # Increased timeout
        client.admin.command('ping') # Test connection
        logger.info(f"🔗 MongoDB Atlas connection successful")
    except errors.ServerSelectionTimeoutError as err:
        logger.error(f"MongoDB Atlas connection failed (timeout): {err}")
        raise
    except Exception as e:
        logger.error(f"MongoDB Atlas connection failed: {str(e)}")
        raise

    total_inserted_docs = 0
    successful_files = 0

    for input_file, collection_name in COLLECTION_MAPPINGS.items():
        full_input_path = os.path.join(INPUT_DIR, input_file)
        logger.info(f"\n🗃️ Processing file: {full_input_path} -> Collection: {collection_name}")

        # Load JSON data
        try:
            if not os.path.exists(full_input_path):
                logger.warning(f"⚠️ Input file not found: {full_input_path}. Skipping.")
                continue

            with open(full_input_path, 'r', encoding='utf-8') as f:
                articles_data = json.load(f)
            logger.info(f"📁 Loaded {len(articles_data)} records from {input_file}")

            # Load to collection
            inserted_count = load_to_collection(client, MONGO_DB, collection_name, articles_data)
            total_inserted_docs += inserted_count

            if inserted_count > 0:
                successful_files += 1

            # Optional: Remove input file after successful load
            # Jika proses transform sudah selesai dan Anda yakin data sudah di-load, bisa dihapus
            try:
                os.remove(full_input_path)
                logger.info(f"🗑️ Deleted input file: {full_input_path}")
            except Exception as e:
                logger.warning(f"Could not delete input file {full_input_path}: {str(e)}")

        except json.JSONDecodeError as e:
            logger.error(f"❌ Error decoding JSON from {full_input_path}: {e}")
        except Exception as e:
            logger.error(f"❌ Failed to process {full_input_path}: {str(e)}")
            continue

    elapsed = time.time() - start_time

    # Summary logging
    logger.info("\n" + "=" * 70)
    logger.info("📊 IQPLUS LOAD SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Database: {MONGO_DB}")
    logger.info(f"Files processed: {len(COLLECTION_MAPPINGS)}")
    logger.info(f"Successful files loaded: {successful_files}")
    logger.info(f"Total documents inserted: {total_inserted_docs:,}")
    logger.info(f"Execution time: {elapsed:.2f} seconds")

    if successful_files == len(COLLECTION_MAPPINGS):
        logger.info("🎉 ALL IQPLUS FILES LOADED SUCCESSFULLY!")
    else:
        logger.warning(f"⚠️  {successful_files}/{len(COLLECTION_MAPPINGS)} files loaded successfully")

    # Close connection
    client.close()
    logger.info("🔌 MongoDB connection closed")

    return {
        "status": "success" if successful_files == len(COLLECTION_MAPPINGS) else "partial_success",
        "total_files": len(COLLECTION_MAPPINGS),
        "successful_files": successful_files,
        "total_documents": total_inserted_docs,
        "execution_time_seconds": elapsed,
        "database": MONGO_DB,
        "collections_loaded": list(COLLECTION_MAPPINGS.values())
    }

if __name__ == "__main__":
    try:
        result = process_iqplus_data()
        logger.info(f"\n🏁 Final Status: {result['status']}")
        logger.info(f"📈 Loaded {result['successful_files']}/{result['total_files']} files")
        logger.info(f"📊 Total documents: {result['total_documents']:,}")
    except Exception as e:
        logger.critical(f"Fatal error during IQPlus data loading: {e}")
        import traceback
        traceback.print_exc()