import re
import time
import gc
import torch
# ==== PERUBAHAN 1: Import JSON dan os ====
import json
import os
from transformers import pipeline, AutoTokenizer, AutoModelForSeq2SeqLM
import threading
from queue import Queue

# Konfigurasi
BATCH_SIZE = 5
MODEL_NAME = "facebook/bart-large-cnn"  # Model berkualitas tinggi
CHUNK_SIZE = 900  # Ukuran chunk yang optimal
MIN_SUMMARY_LENGTH = 80  # Panjang minimum ringkasan
MAX_SUMMARY_LENGTH = 200  # Panjang maksimum ringkasan
MAX_CHUNKS_PER_ARTICLE = 5  # Maksimal chunk per artikel

# UPDATE: Gunakan environment variable yang konsisten
INPUT_OUTPUT_DIR = os.getenv('IQPLUS_DATA_DIR', '/app/output')

class EnhancedSummarizer:
    def __init__(self):
        print("🔄 Memuat model BART (berkualitas tinggi)...")
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
            self.model = AutoModelForSeq2SeqLM.from_pretrained(MODEL_NAME)
            
            # Setup pipeline dengan optimasi
            device = 0 if torch.cuda.is_available() else -1
            print(f"📱 Menggunakan device: {'GPU' if device == 0 else 'CPU'}")
            
            self.summarizer = pipeline(
                "summarization", 
                model=self.model, 
                tokenizer=self.tokenizer,
                device=device,
                framework="pt"
            )
            print("✅ Model BART berhasil dimuat!")
        except Exception as e:
            print(f"❌ Error loading model: {e}")
            raise
    
    def clean_text(self, text):
        """Membersihkan teks dengan preprocessing yang lebih baik"""
        if not text or not isinstance(text, str):
            return ""
        
        try:
            # Hapus header IQPlus dengan tanggal (format: IQPlus, (DD/MM)-)
            text = re.sub(r"IQPlus,\s*\(\d{1,2}/\d{1,2}\)-?\s*", "", text)
            
            # Hapus footer sumber artikel (format: (end/SUMBER))
            text = re.sub(r"\s*\(end/[^)]+\)\s*$", "", text)
            
            # Hapus tanda kutip dan noise characters
            text = re.sub(r'[""''"]', '"', text)  # Standardize quotes
            
            # Hapus excess whitespace dan normalize
            text = re.sub(r"\s+", " ", text)
            
            # Hapus titik ganda atau lebih
            text = re.sub(r"\.{2,}", ".", text)
            
            return text.strip()
        except Exception as e:
            print(f"⚠️ Error cleaning text: {e}")
            return str(text).strip()
    
    def chunk_text_intelligently(self, text, max_chars=CHUNK_SIZE):
        """Membagi teks menjadi chunk dengan mempertahankan konteks - optimized version"""
        if len(text) <= max_chars:
            return [text]
        
        # Simplified chunking - prioritas speed
        chunks = []
        current_chunk = ""
        
        # Split berdasarkan kalimat langsung (lebih cepat)
        sentences = text.split('. ')
        
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue
                
            # Cek apakah menambah kalimat ini akan melebihi batas
            if len(current_chunk) + len(sentence) + 2 < max_chars:
                current_chunk += sentence + ". "
            else:
                if current_chunk.strip():
                    chunks.append(current_chunk.strip())
                current_chunk = sentence + ". "
                
            # Batasi jumlah chunk untuk speed
            if len(chunks) >= MAX_CHUNKS_PER_ARTICLE - 1:
                break
        
        # Tambahkan chunk terakhir
        if current_chunk.strip():
            chunks.append(current_chunk.strip())
        
        # Jika masih ada sisa teks yang belum masuk chunk dan chunk belum penuh
        if len(chunks) < MAX_CHUNKS_PER_ARTICLE and len(sentences) > len(chunks) * 5:
            remaining_sentences = sentences[len(chunks) * 5:]
            if remaining_sentences:
                remaining_text = '. '.join(remaining_sentences[:5])  # Ambil max 5 kalimat
                if remaining_text.strip():
                    chunks.append(remaining_text.strip())
        
        return chunks[:MAX_CHUNKS_PER_ARTICLE]  # Pastikan tidak melebihi batas
    
    def summarize_chunk(self, chunk, attempt=1):
        """Meringkas satu chunk dengan retry mechanism"""
        try:
            # Sesuaikan parameter berdasarkan panjang chunk
            chunk_len = len(chunk)
            if chunk_len < 100:
                return chunk  # Terlalu pendek untuk diringkas
            
            # Dinamis max_length berdasarkan panjang input
            max_len = min(MAX_SUMMARY_LENGTH, max(MIN_SUMMARY_LENGTH, chunk_len // 3))
            min_len = min(min(MIN_SUMMARY_LENGTH, max_len - 20), chunk_len // 5)
            
            result = self.summarizer(
                chunk,
                max_length=max_len,
                min_length=min_len,
                do_sample=False,
                early_stopping=True,
                no_repeat_ngram_size=3,
                length_penalty=1.0
            )
            
            summary = result[0]['summary_text'].strip()
            
            # Validasi hasil
            if len(summary) < 20 and attempt < 3:
                print(f"   🔄 Retry summarization (attempt {attempt + 1})")
                return self.summarize_chunk(chunk, attempt + 1)
            
            return summary
            
        except Exception as e:
            print(f"   ⚠️ Error summarizing chunk (attempt {attempt}): {e}")
            if attempt < 3:
                time.sleep(1)  # Tunggu sebentar sebelum retry
                return self.summarize_chunk(chunk, attempt + 1)
            else:
                # Fallback: ambil kalimat pertama yang meaningful
                sentences = chunk.split('. ')
                meaningful_sentences = [s for s in sentences if len(s) > 30]
                if meaningful_sentences:
                    return meaningful_sentences[0] + "."
                return chunk[:200] + "..." if len(chunk) > 200 else chunk
    
    def summarize_article(self, text):
        """Meringkas artikel lengkap dengan pendekatan multi-chunk - optimized"""
        if not text or len(str(text).strip()) < 30:
            return str(text).strip() if text else ""
        
        try:
            # Bersihkan teks
            cleaned_text = self.clean_text(str(text))
            if len(cleaned_text) < 30:
                return cleaned_text
            
            print(f"   📏 Panjang teks asli: {len(cleaned_text)} karakter")
            
            # Smart pre-filtering untuk speed
            if len(cleaned_text) < 300:
                print("   ⚡ Artikel pendek - langsung dikembalikan")
                return cleaned_text
            elif len(cleaned_text) <= 800:
                print("   ⚡ Artikel sedang - ringkas langsung")
                summary = self.summarize_chunk(cleaned_text)
                print(f"   ✅ Ringkasan langsung: {len(summary)} karakter")
                return summary
            
            # Multi-chunk untuk artikel panjang
            chunks = self.chunk_text_intelligently(cleaned_text)
            print(f"   📦 Dibagi menjadi {len(chunks)} chunk (max: {MAX_CHUNKS_PER_ARTICLE})")
            
            chunk_summaries = []
            for i, chunk in enumerate(chunks):
                print(f"   🔄 Memproses chunk {i+1}/{len(chunks)} ({len(chunk)} char)")
                summary = self.summarize_chunk(chunk)
                if summary and len(summary.strip()) > 10:
                    chunk_summaries.append(summary)
                    print(f"   ✅ Chunk {i+1} selesai ({len(summary)} char)")
            
            if not chunk_summaries:
                print("   ⚠️ Tidak ada chunk yang berhasil diringkas")
                return cleaned_text[:400] + "..." if len(cleaned_text) > 400 else cleaned_text
            
            # Gabungkan hasil ringkasan chunk
            combined_summary = " ".join(chunk_summaries)
            print(f"   📝 Ringkasan gabungan: {len(combined_summary)} karakter")
            
            return combined_summary
            
        except Exception as e:
            print(f"   ❌ Error in summarize_article: {e}")
            try:
                cleaned = self.clean_text(str(text))
                return cleaned[:300] + "..." if len(cleaned) > 300 else cleaned
            except:
                return ""

# Fungsi untuk memuat dan menyimpan JSON
def load_json_file(filename):
    """Memuat data dari file JSON"""
    full_path = os.path.join(INPUT_OUTPUT_DIR, filename)
    try:
        if not os.path.exists(full_path):
            print(f"⚠️ File {full_path} tidak ditemukan")
            return []
        
        with open(full_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            print(f"✅ Berhasil memuat {len(data)} artikel dari {full_path}")
            return data
    except Exception as e:
        print(f"❌ Error loading {full_path}: {e}")
        return []

def save_json_file(filename, data):
    """Menyimpan data ke file JSON dengan backup"""
    full_path = os.path.join(INPUT_OUTPUT_DIR, filename)
    try:
        # Buat backup file asli jika ada
        if os.path.exists(full_path):
            backup_name = f"{full_path}.backup_{int(time.time())}"
            os.rename(full_path, backup_name)
            print(f"📋 Backup dibuat: {backup_name}")
        
        # Simpan data baru
        with open(full_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"💾 Berhasil menyimpan {len(data)} artikel ke {full_path}")
        return True
    except Exception as e:
        print(f"❌ Error saving {full_path}: {e}")
        return False

def process_news_articles():
    """Fungsi utama untuk memproses artikel berita dari file JSON"""
    
    # Definisi file JSON yang akan diproses
    json_files = [
        {"filename": "market_news.json", "type": "market"},
        {"filename": "stock_news.json", "type": "stock"}
    ]
    
    # Inisialisasi summarizer
    try:
        summarizer = EnhancedSummarizer()
    except Exception as e:
        print(f"❌ Gagal membuat summarizer: {e}")
        return
    
    # Proses setiap file JSON
    for file_info in json_files:
        filename = file_info["filename"]
        file_type = file_info["type"]
        
        print(f"\n🗃️ Memproses file: {filename} ({file_type})")
        
        # Muat data dari file JSON
        articles_data = load_json_file(filename)
        if not articles_data:
            print(f"⚠️ Tidak ada data untuk diproses di {filename}")
            continue
        
        try:
            # Hitung artikel yang perlu diproses
            articles_to_process = []
            for idx, article in enumerate(articles_data):
                # Cek apakah artikel perlu diproses (belum ada ringkasan atau ringkasan terlalu pendek)
                ringkasan = article.get("ringkasan", "")
                if (not ringkasan or 
                    ringkasan == "" or 
                    ringkasan is None):
                    articles_to_process.append((idx, article))
            
            total_count = min(5, len(articles_to_process))  # Batasi untuk testing
            print(f"📊 Total artikel yang perlu diproses: {total_count}")
            
            if total_count == 0:
                print("✅ Semua artikel sudah memiliki ringkasan yang baik")
                continue
            
            processed = 0
            errors = 0
            start_time = time.time()
            
            # Proses dalam batch
            for batch_start in range(0, total_count, BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, total_count)
                batch_articles = articles_to_process[batch_start:batch_end]
                
                print(f"\n🔄 Batch {batch_start//BATCH_SIZE + 1}: Memproses {len(batch_articles)} artikel...")
                
                batch_start_time = time.time()
                
                for batch_idx, (original_idx, article) in enumerate(batch_articles):
                    konten = article.get("konten", "") or article.get("content", "")
                    judul = article.get("judul", "") or article.get("title", "") or "Tanpa Judul"
                    
                    # Display info
                    judul_display = (str(judul)[:60] + "...") if len(str(judul)) > 60 else str(judul)
                    print(f"\n📄 [{batch_idx+1}/{len(batch_articles)}] Index: {original_idx}")
                    print(f"📰 Judul: {judul_display}")
                    
                    try:
                        if konten and len(str(konten).strip()) > 30:
                            article_start = time.time()
                            summary = summarizer.summarize_article(konten)
                            article_time = time.time() - article_start
                            
                            if summary and len(summary.strip()) > 20:
                                # Update artikel di dalam list
                                articles_data[original_idx]["ringkasan"] = summary
                                print(f"   ✅ Ringkasan berhasil ({len(summary)} char, {article_time:.1f}s)")
                            else:
                                print("   ⚠️ Ringkasan kosong/terlalu pendek")
                                articles_data[original_idx]["ringkasan"] = ""
                                errors += 1
                        else:
                            articles_data[original_idx]["ringkasan"] = ""
                            print("   ⚠️ Konten kosong, ringkasan disimpan kosong")
                            
                    except Exception as e:
                        print(f"   ❌ Error processing article: {e}")
                        articles_data[original_idx]["ringkasan"] = ""
                        errors += 1
                        continue
                
                processed += len(batch_articles)
                batch_time = time.time() - batch_start_time
                elapsed = time.time() - start_time
                
                # Statistik progress
                rate = processed / elapsed if elapsed > 0 else 0
                eta = (total_count - processed) / rate if rate > 0 else 0
                
                print(f"\n📈 Progress: {processed}/{total_count} ({processed/total_count*100:.1f}%)")
                print(f"⏱️  Waktu batch: {batch_time:.1f}s | Rate: {rate:.2f} artikel/detik")
                print(f"🕐 ETA: {eta/60:.1f} menit | Errors: {errors}")
                
                # Simpan progress setiap batch
                if processed % (BATCH_SIZE * 2) == 0:  # Simpan setiap 2 batch
                    print("💾 Menyimpan progress...")
                    if save_json_file(filename, articles_data):
                        print("✅ Progress tersimpan")
                    
                    # Memory cleanup
                    gc.collect()
                    if torch.cuda.is_available():
                        torch.cuda.empty_cache()
                    time.sleep(1)
            
            # Simpan hasil akhir
            print(f"\n💾 Menyimpan hasil akhir ke {filename}...")
            if save_json_file(filename, articles_data):
                total_time = time.time() - start_time
                success_rate = ((processed - errors) / processed * 100) if processed > 0 else 0
                
                print(f"\n✅ Selesai memproses {filename}")
                print(f"📊 Total waktu: {total_time/60:.1f} menit")
                print(f"📈 Success rate: {success_rate:.1f}% ({processed - errors}/{processed})")
            else:
                print(f"❌ Gagal menyimpan hasil akhir untuk {filename}")
            
        except Exception as e:
            print(f"❌ Error processing file {filename}: {e}")
            continue
    
    print("\n🎉 Semua file JSON berhasil diproses!")
    print("💡 Tips: Anda dapat menjalankan script ini lagi untuk memproses artikel baru")

if __name__ == "__main__":
    try:
        print("🚀 Enhanced News Summarizer v2.0 (JSON Version)")
        print("=" * 50)
        process_news_articles()
    except KeyboardInterrupt:
        print("\n⚠️ Proses dihentikan oleh user")
        print("💾 Progress yang sudah berjalan telah tersimpan")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        print("🔍 Traceback:")
        traceback.print_exc()