import os
import zipfile
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import time

def test_mongodb_connection(mongo_uri=None, mongo_host=None, mongo_port=None, mongo_db_name=None):
    """
    Test MongoDB connection and log the results
    
    Returns:
        tuple: (client, db, success_status, error_message)
    """
    try:
        print("🔗 Testing MongoDB connection...")
        
        if mongo_uri and mongo_uri.startswith("mongodb"):
            # Using URI format
            print(f"   📍 Connecting to MongoDB using URI: {mongo_uri}")
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)  # 10 second timeout
        else:
            # Using separate host and port
            connection_string = f"mongodb://{mongo_host}:{mongo_port}/"
            print(f"   📍 Connecting to MongoDB: {connection_string}")
            client = MongoClient(connection_string, serverSelectionTimeoutMS=10000)
        
        # Test the connection
        client.admin.command('ping')
        
        # Get database
        db = client[mongo_db_name]
        
        # Test database access
        db.list_collection_names()
        
        print("   ✅ MongoDB connection successful!")
        print(f"   📊 Database: {mongo_db_name}")
        print(f"   📁 Available collections: {db.list_collection_names()}")
        
        return client, db, True, None
        
    except ConnectionFailure as e:
        error_msg = f"MongoDB connection failed: {str(e)}"
        print(f"   ❌ {error_msg}")
        return None, None, False, error_msg
        
    except ServerSelectionTimeoutError as e:
        error_msg = f"MongoDB server selection timeout: {str(e)}"
        print(f"   ❌ {error_msg}")
        return None, None, False, error_msg
        
    except Exception as e:
        error_msg = f"Unexpected MongoDB error: {str(e)}"
        print(f"   ❌ {error_msg}")
        return None, None, False, error_msg

def convert_xbrl_to_json_and_load_to_mongo(download_dir, mongo_uri=None, mongo_host=None, mongo_port=None, mongo_db_name=None, mongo_collection_name=None):
    """
    Iterates through downloaded XBRL zip files, converts instance.xbrl to JSON,
    and loads the JSON data into MongoDB. Supports the dynamic year-based directory structure.
    """
    print("🚀 Starting XBRL to JSON Conversion and MongoDB Load Process")
    print("=" * 80)
    
    log_path = os.path.join(download_dir, "xbrl_conversion_and_load_results.log")
    successful_processed = 0
    failed_processed = 0
    skipped_files = 0
    total_documents_inserted = 0

    # Test MongoDB connection first
    client, db, connection_success, connection_error = test_mongodb_connection(
        mongo_uri, mongo_host, mongo_port, mongo_db_name
    )
    
    if not connection_success:
        raise Exception(f"Cannot proceed without MongoDB connection: {connection_error}")
    
    collection = db[mongo_collection_name]
    print(f"   🎯 Target collection: {mongo_collection_name}")
    
    # Check if collection exists and show document count
    try:
        existing_count = collection.count_documents({})
        print(f"   📈 Existing documents in collection: {existing_count:,}")
    except Exception as e:
        print(f"   ⚠️  Could not count existing documents: {e}")

    with open(log_path, "w", encoding="utf-8") as log:
        log.write(f"XBRL to JSON Conversion and MongoDB Load started: {datetime.now()}\n")
        log.write(f"MongoDB Connection: {'SUCCESS' if connection_success else 'FAILED'}\n")
        if connection_success:
            log.write(f"Database: {mongo_db_name}\n")
            log.write(f"Collection: {mongo_collection_name}\n")
        else:
            log.write(f"Connection Error: {connection_error}\n")
        log.write("-" * 80 + "\n")

        print(f"\n📂 Scanning download directory: {download_dir}")
        
        # Get list of year directories (2021, 2022, 2023, 2024, 2025)
        year_dirs = []
        if os.path.exists(download_dir):
            year_dirs = [d for d in os.listdir(download_dir) 
                        if os.path.isdir(os.path.join(download_dir, d)) 
                        and d.isdigit() and len(d) == 4]
        
        if not year_dirs:
            # Fallback to old structure if no year directories found
            print("   ⚠️  No year directories found, checking for legacy structure...")
            year_dirs = [""]  # Empty string will make os.path.join(download_dir, "") == download_dir
            
        print(f"📅 Found {len(year_dirs)} year directories: {', '.join(year_dirs) if year_dirs != [''] else 'Legacy structure - no year directories'}")
        log.write(f"Processing years: {', '.join(year_dirs) if year_dirs != [''] else 'Legacy structure - no year directories'}\n")
        
        year_stats = {}
        
        for year_dir in year_dirs:
            year = year_dir if year_dir else "unknown"  # Use "unknown" if using legacy structure
            base_dir = os.path.join(download_dir, year_dir) if year_dir else download_dir
            
            print(f"\n📊 {'='*20} Processing Year: {year} {'='*20}")
            log.write(f"\n=== Processing files for year: {year} ===\n")
            
            year_successful = 0
            year_failed = 0
            year_skipped = 0

            if not os.path.exists(base_dir):
                print(f"   ⚠️  Directory not found: {base_dir}")
                log.write(f"SKIP: Directory not found: {base_dir}\n")
                continue

            company_dirs = [d for d in os.listdir(base_dir) 
                           if os.path.isdir(os.path.join(base_dir, d)) and not d.isdigit()]
            
            print(f"   🏢 Found {len(company_dirs)} company directories for year {year}")
            print(f"   📋 Companies: {', '.join(company_dirs)}")
            log.write(f"Found {len(company_dirs)} company directories for year {year}: {', '.join(company_dirs)}\n")
            
            if not company_dirs:
                print(f"   ⚠️  No company directories found for year {year}")
                continue
            
            for idx, company_code in enumerate(company_dirs, 1):
                company_dir = os.path.join(base_dir, company_code)
                
                print(f"   [{idx:2d}/{len(company_dirs):2d}] Processing company: {company_code}")
                print(f"        📂 Directory: {company_dir}")
                
                # Find all zip files that contain "instance.zip"
                zip_files = []
                all_files = []
                try:
                    all_files = os.listdir(company_dir)
                    print(f"        📁 Found {len(all_files)} files: {', '.join(all_files[:10])}{' ...' if len(all_files) > 10 else ''}")

                    # Updated file matching to handle new pattern: COMPANY_instance_YEAR.zip
                    for file in all_files:
                        if (file.endswith("_instance.zip") or 
                            file == "instance.zip" or 
                            "_instance_" in file and file.endswith(".zip")):
                            zip_files.append(file)
                            print(f"        ✅ Match found: {file}")
                    
                    if not zip_files:
                        # Look for any zip files for debugging
                        any_zip_files = [f for f in all_files if f.endswith('.zip')]
                        print(f"        🔍 ZIP files matching pattern: None")
                        print(f"        📦 Any ZIP files found: {any_zip_files if any_zip_files else 'None'}")
                    else:
                        print(f"        🎯 ZIP files matching pattern: {zip_files}")
                    
                except Exception as e:
                    print(f"        ❌ Error listing files in {company_dir}: {e}")
                    log.write(f"ERROR: Cannot list files in {company_dir}: {e}\n")
                    year_failed += 1
                    continue

                if not zip_files:
                    print(f"        ⏭️  No instance.zip files found")
                    print(f"        📋 All files in directory: {all_files}")
                    log.write(f"SKIP: No _instance.zip found in {company_dir}\n")
                    log.write(f"Available files: {all_files}\n")
                    year_skipped += 1
                    skipped_files += 1
                    continue

                for zip_filename in zip_files:
                    zip_filepath = os.path.join(company_dir, zip_filename)
                    
                    print(f"        📦 Processing: {zip_filename}")
                    log.write(f"Processing: {zip_filepath}\n")

                    try:
                        # Check if file exists and is readable
                        if not os.path.exists(zip_filepath):
                            raise FileNotFoundError(f"Zip file not found: {zip_filepath}")
                        
                        file_size = os.path.getsize(zip_filepath)
                        print(f"            📏 File size: {file_size:,} bytes")
                        
                        if file_size < 100:  # Very small file, likely corrupted
                            raise ValueError(f"Zip file too small ({file_size} bytes), likely corrupted")
                        
                        with zipfile.ZipFile(zip_filepath, "r") as zf:
                            # Check if instance.xbrl exists within the zip
                            zip_contents = zf.namelist()
                            print(f"            📄 ZIP contents: {zip_contents}")
                            
                            if "instance.xbrl" not in zip_contents:
                                raise ValueError(f"instance.xbrl not found in zip file. Contents: {zip_contents}")

                            with zf.open("instance.xbrl") as xf:
                                tree = ET.parse(xf)
                                root_element = tree.getroot()

                                key_value_data = {}
                                element_count = 0
                                
                                for elem in root_element.iter():
                                    element_count += 1
                                    local_tag_name = elem.tag.split("}")[-1]

                                    # Skip common XBRL structural elements if they are not facts themselves
                                    if local_tag_name in ["schemaRef", "linkbaseRef", "roleRef", "arcroleRef", "context", "unit"] and \
                                       ("http://www.xbrl.org/2003/instance" in elem.tag or "http://www.xbrl.org/2003/linkbase" in elem.tag):
                                        continue

                                    element_xml_string = None
                                    text_content = None

                                    if elem.attrib.get('contextRef'):
                                        element_xml_string = ET.tostring(elem, encoding='unicode').strip()
                                    elif elem.text and elem.text.strip():
                                        text_content = elem.text.strip()
                                    else:
                                        pass # Continue to next element if neither condition is met

                                    if element_xml_string:  # Tag has contextRef
                                        if local_tag_name not in key_value_data:
                                            key_value_data[local_tag_name] = [element_xml_string]
                                        else:
                                            current_val = key_value_data[local_tag_name]
                                            if not isinstance(current_val, list):
                                                key_value_data[local_tag_name] = [current_val, element_xml_string]
                                            else:
                                                current_val.append(element_xml_string)
                                    elif text_content:  # Tag has no contextRef but has text
                                        if local_tag_name not in key_value_data:
                                            key_value_data[local_tag_name] = text_content
                                        else:
                                            current_val = key_value_data[local_tag_name]
                                            if not isinstance(current_val, list):
                                                # If it's the same text, do nothing to avoid ["text", "text"] for identical simple tags
                                                if current_val != text_content:
                                                    key_value_data[local_tag_name] = [current_val, text_content]
                                            else:  # Existing was a list (e.g., from contextRef elements or previous text_content)
                                                # Avoid adding duplicate plain text to a list that might already contain it
                                                if text_content not in current_val: # Simple check, might need refinement if order matters or objects are complex
                                                    current_val.append(text_content)
                                
                                print(f"            📄 Processed {element_count} XML elements, extracted {len(key_value_data)} data fields")

                                # Determine emiten_name
                                emiten_name_from_data = key_value_data.get("EntityName")
                                resolved_emiten_name = company_code # Default fallback

                                if emiten_name_from_data:
                                    name_to_parse = None
                                    if isinstance(emiten_name_from_data, list):
                                        if emiten_name_from_data: # if list is not empty
                                            name_to_parse = emiten_name_from_data[0] # Take first
                                    elif isinstance(emiten_name_from_data, str):
                                        name_to_parse = emiten_name_from_data
                                    
                                    if name_to_parse:
                                        if isinstance(name_to_parse, str): # Ensure it's a string to parse
                                            if '<' in name_to_parse and '>' in name_to_parse: # Looks like XML
                                                try:
                                                    elem = ET.fromstring(name_to_parse)
                                                    if elem.text and elem.text.strip():
                                                        resolved_emiten_name = elem.text.strip()
                                                except ET.XMLSyntaxError:
                                                    # If parsing XML string fails, stick to default
                                                    pass
                                            else: # Plain text
                                                resolved_emiten_name = name_to_parse.strip()
                                
                                print(f"            🏢 Resolved company name: {resolved_emiten_name}")
                                
                                # Add metadata
                                processing_timestamp = datetime.now().isoformat()
                                key_value_data["company_code"] = company_code
                                key_value_data["report_year"] = year
                                key_value_data["processed_at"] = processing_timestamp
                                key_value_data["source_file"] = zip_filename
                                
                                # Structure the output document
                                output_document = {
                                    "emiten": resolved_emiten_name,
                                    "company_code": company_code,
                                    "report_year": year,
                                    "processed_at": processing_timestamp,
                                    "source_file": zip_filename,
                                    "data_fields_count": len(key_value_data),
                                    "laporan_keuangan_data": key_value_data
                                }

                                # Save to a local JSON file (optional, for debugging/record)
                                output_json_filename = f"{company_code}_{year}_instance.json"
                                output_json_filepath = os.path.join(company_dir, output_json_filename)
                                
                                try:
                                    with open(output_json_filepath, "w", encoding="utf-8") as f:
                                        json.dump(output_document, f, indent=2, ensure_ascii=False)
                                    print(f"            💾 Local JSON saved: {output_json_filename}")
                                except Exception as json_error:
                                    print(f"            ⚠️  Failed to save local JSON: {json_error}")

                                # Insert into MongoDB
                                try:
                                    result = collection.insert_one(output_document)
                                    print(f"            🎯 MongoDB insert successful: {result.inserted_id}")
                                    total_documents_inserted += 1
                                except Exception as mongo_error:
                                    print(f"            ❌ MongoDB insert failed: {mongo_error}")
                                    raise mongo_error

                                print(f"        ✅ Success: {company_code} ({year}) processed and loaded")
                                log.write(f"Status: SUCCESS - Year {year} - Company {company_code} - Fields: {len(key_value_data)}\n")
                                successful_processed += 1
                                year_successful += 1

                    except Exception as e:
                        print(f"        ❌ Failed: {str(e)}")
                        log.write(f"Status: ERROR - Year {year} - Company {company_code} - {str(e)}\n")
                        failed_processed += 1
                        year_failed += 1

                    log.write("-" * 50 + "\n")

            # Year summary
            year_stats[year] = {
                'successful': year_successful,
                'failed': year_failed,
                'skipped': year_skipped,
                'total_companies': len(company_dirs)
            }
            
            print(f"\n📈 Year {year} Summary:")
            print(f"   ✅ Successful: {year_successful}")
            print(f"   ❌ Failed: {year_failed}")  
            print(f"   ⏭️  Skipped: {year_skipped}")
            year_total = year_successful + year_failed
            year_success_rate = (year_successful/year_total*100) if year_total > 0 else 0
            print(f"   📊 Success rate: {year_success_rate:.1f}%")

        # Final summary
        print(f"\n🎉 {'='*20} FINAL SUMMARY {'='*20}")
        
        # MongoDB collection final count
        try:
            final_count = collection.count_documents({})
            print(f"📊 Final documents in MongoDB collection: {final_count:,}")
            print(f"📈 New documents inserted this session: {total_documents_inserted:,}")
        except Exception as e:
            print(f"⚠️  Could not get final document count: {e}")

        # Calculate success rate safely
        total_processed = successful_processed + failed_processed
        success_rate = (successful_processed/total_processed*100) if total_processed > 0 else 0
        
        summary = f"""
Conversion and Load Summary:
---------------------------
Total files processed: {total_processed}
Successfully processed and loaded: {successful_processed}
Failed processing: {failed_processed}
Skipped files: {skipped_files}
Years processed: {', '.join(year_dirs) if year_dirs != [''] else 'Legacy structure'}
New documents inserted to MongoDB: {total_documents_inserted}
Success rate: {success_rate:.1f}%
Completion time: {datetime.now()}

Year-by-Year Breakdown:
"""
        
        for year, stats in year_stats.items():
            year_total = stats['successful'] + stats['failed']
            year_rate = (stats['successful']/year_total*100) if year_total > 0 else 0
            
            summary += f"""
Year {year}:
  - Companies found: {stats['total_companies']}
  - Successful: {stats['successful']}
  - Failed: {stats['failed']}
  - Skipped: {stats['skipped']}
  - Success rate: {year_rate:.1f}%
"""

        print(summary)
        log.write(summary)
        
    print(f"📄 XBRL conversion and MongoDB load process completed. See {log_path} for details.")
    
    # Close MongoDB connection
    try:
        client.close()
        print("🔒 MongoDB connection closed successfully.")
    except Exception as e:
        print(f"⚠️  Error closing MongoDB connection: {e}")

if __name__ == "__main__":
    print("🚀 IDX XBRL to JSON Converter & MongoDB Loader")
    print("=" * 60)
    
    # Get configuration from environment variables
    DOWNLOAD_DIR = os.getenv("IDX_DOWNLOAD_DIR", "/app/output")
    
    # For MongoDB connection, use the same pattern as yfinance-load.py
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    if MONGO_URI.startswith("mongodb"):
        # Using URI format
        MONGO_HOST = MONGO_URI
        MONGO_PORT = None
        print(f"🔗 MongoDB URI: {MONGO_URI}")
    else:
        # Using separate host and port
        MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
        MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
        print(f"🔗 MongoDB Host: {MONGO_HOST}:{MONGO_PORT}")
        
    # Use the same DB as yfinance, but with consolidate collection names for raw and transformed data
    MONGO_DB = os.getenv("MONGO_DB", "bigdata_saham")
    
    # Use idx_raw as the collection name for all years (consolidated)
    MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "idx_raw")

    print(f"📂 Download Directory: {DOWNLOAD_DIR}")
    print(f"🗃️  Database: {MONGO_DB}")
    print(f"📁 Collection: {MONGO_COLLECTION}")
    print("🎯 This will process data from all years (2021-2025) into a single collection")
    print("=" * 60)
    
    try:
        if MONGO_URI.startswith("mongodb"):
            # Using MongoDB URI format
            convert_xbrl_to_json_and_load_to_mongo(DOWNLOAD_DIR, MONGO_URI, None, None, MONGO_DB, MONGO_COLLECTION)
        else:
            # Using separate host and port
            convert_xbrl_to_json_and_load_to_mongo(DOWNLOAD_DIR, None, MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION)
            
        print("\n🎉 Process completed successfully!")
        
    except Exception as e:
        print(f"\n💥 Process failed with error: {str(e)}")
        raise