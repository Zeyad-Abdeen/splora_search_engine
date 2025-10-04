from bs4 import BeautifulSoup
import requests
import time
import random
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from urllib.parse import urlparse
import csv
import sys
import os
import pandas as pd

# Add the root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from indexing.advanced_indexing import advanced_index_page
from serving.pagerank import compute_pagerank

def can_crawl(url):
    """Always return True to disable robots.txt checking"""
    return True

def load_seed_urls_from_csv(csv_file_path):
    """Load seed URLs from the second column of the specified CSV file"""
    seed_urls = []
    try:
        df = pd.read_csv(csv_file_path)
        
        if len(df.columns) >= 2:
            second_column = df.iloc[:, 1]
            
            for url in second_column.dropna():
                url = str(url).strip()
                if url.startswith(('http://', 'https://')):
                    seed_urls.append(url)
                    print(f"Added seed URL: {url}")
                else:
                    print(f"Skipped invalid URL: {url}")
        else:
            print(f"Error: CSV file must have at least 2 columns")
            return []
            
    except FileNotFoundError:
        print(f"Error: Could not find CSV file: {csv_file_path}")
        return []
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return []
    
    print(f"Loaded {len(seed_urls)} seed URLs from CSV file")
    return seed_urls

def parse_links(hyperlinks, current_url):
    """Parse links from HTML content"""
    urls = []
    hyperlink_connections = set()
    
    for hyperlink in hyperlinks:
        url = hyperlink.get("href", "")
        
        if url.startswith("#"):
            continue
        if url.startswith("//"):
            url = "https:" + url
        elif url.startswith("/"):
            base_url = "{0.scheme}://{0.netloc}".format(requests.utils.urlparse(current_url))
            url = base_url + url
        elif not url.startswith("http"):
            continue
        
        url = url.split("#")[0]
        hyperlink_connections.add(url)
        urls.append(url)
    
    return urls, hyperlink_connections

def crawl_worker(worker_id, shared_data, stop_event):
    """Worker function for crawling URLs"""
    queue = shared_data['queue']
    visited_urls = shared_data['visited_urls']
    crawl_count = shared_data['crawl_count']
    CRAWL_LIMIT = shared_data['CRAWL_LIMIT']
    lock = shared_data['lock']
    index = shared_data['index']
    webpage_info = shared_data['webpage_info']
    webpage_id_counter = shared_data['webpage_id_counter']
    pagerank_graph = shared_data['pagerank_graph']
    seed_urls_set = shared_data['seed_urls_set']
    failed_urls = shared_data['failed_urls']
    remaining_seeds = shared_data['remaining_seeds']
    
    print(f"Worker {worker_id} started")
    
    while not stop_event.is_set():
        current_url = None
        task_done_called = False
        
        try:
            # Try to get URL from queue with timeout
            current_url = queue.get(timeout=30)  # Increased timeout
        except Empty:
            # Check if there are still remaining seed URLs
            with lock:
                if len(remaining_seeds) > 0:
                    # Add remaining seeds back to queue
                    for url in list(remaining_seeds):
                        if url not in visited_urls:
                            queue.put(url)
                            print(f"Worker {worker_id}: Re-queuing missed seed URL: {url}")
                    continue
                else:
                    print(f"Worker {worker_id}: No more URLs to process, exiting")
                    break
        
        try:
            # Check crawl limit and visited status
            with lock:
                if CRAWL_LIMIT != float('inf') and crawl_count[0] >= CRAWL_LIMIT:
                    print(f"Worker {worker_id}: Crawl limit reached, exiting")
                    queue.task_done()
                    task_done_called = True
                    break
                
                if current_url in visited_urls:
                    print(f"Worker {worker_id}: URL already visited: {current_url}")
                    queue.task_done()
                    task_done_called = True
                    continue
                
                visited_urls.add(current_url)
                # Remove from remaining seeds if it's a seed URL
                remaining_seeds.discard(current_url)
                current_count = crawl_count[0]
                crawl_count[0] += 1
            
            print(f"Worker {worker_id} ({current_count + 1}/{CRAWL_LIMIT}): Crawling {current_url}")
            
            if not can_crawl(current_url):
                print(f"Worker {worker_id}: Robots.txt disallows crawling: {current_url}")
                if current_url in seed_urls_set:
                    with lock:
                        failed_urls.append((current_url, "Robots.txt disallows"))
                queue.task_done()
                task_done_called = True
                continue
            
            # Add delay to be respectful - longer delay for NCBI
            if 'ncbi.nlm.nih.gov' in current_url:
                time.sleep(random.uniform(1, 3))  # Reduced delay for faster processing
            else:
                time.sleep(random.uniform(1, 2))
            
            # Make HTTP request with retries
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            # Retry logic for important seed URLs
            max_retries = 3 if current_url in seed_urls_set else 1
            success = False
            last_error = None
            
            for attempt in range(max_retries):
                try:
                    response = requests.get(current_url, timeout=30, headers=headers)  # Increased timeout
                    response.raise_for_status()
                    content = response.content
                    success = True
                    break
                except requests.RequestException as e:
                    last_error = e
                    if attempt < max_retries - 1:
                        print(f"Worker {worker_id}: Attempt {attempt + 1} failed for {current_url}, retrying...")
                        time.sleep(random.uniform(5, 10))  # Wait before retry
                    continue
            
            if not success:
                print(f"Worker {worker_id}: Failed to fetch {current_url} after {max_retries} attempts: {last_error}")
                if current_url in seed_urls_set:
                    with lock:
                        failed_urls.append((current_url, str(last_error)))
                queue.task_done()
                task_done_called = True
                continue
            
            # Check for noindex directive
            try:
                content_text = content.decode('utf-8', errors='ignore').lower()
                if 'noindex' in content_text:
                    print(f"Worker {worker_id}: Noindex found, skipping: {current_url}")
                    if current_url in seed_urls_set:
                        with lock:
                            failed_urls.append((current_url, "Noindex directive found"))
                    queue.task_done()
                    task_done_called = True
                    continue
            except Exception as e:
                print(f"Worker {worker_id}: Error checking noindex for {current_url}: {e}")
            
            # Parse the content
            webpage = BeautifulSoup(content, "html.parser")
            
            # Index the webpage
            try:
                indexed_page = advanced_index_page(webpage, current_url)
            except Exception as e:
                print(f"Worker {worker_id}: Error indexing {current_url}: {e}")
                if current_url in seed_urls_set:
                    with lock:
                        failed_urls.append((current_url, f"Indexing error: {str(e)}"))
                queue.task_done()
                task_done_called = True
                continue
            
            # Update shared data structures
            with lock:
                current_doc_id = webpage_id_counter[0]
                webpage_id_counter[0] += 1
                
                # Update inverted index
                for word in indexed_page["words"]:
                    if word not in index:
                        index[word] = set()
                    index[word].add(current_doc_id)
                
                # Store webpage info
                webpage_info[current_doc_id] = indexed_page
            
            # Parse links for PageRank
            hyperlinks = webpage.select("a[href]")
            new_urls, hyperlink_connections = parse_links(hyperlinks, current_url)
            
            # Update PageRank graph
            with lock:
                pagerank_graph[current_url] = hyperlink_connections
            
            print(f"Worker {worker_id}: Successfully processed {current_url}")
            
        except Exception as e:
            print(f"Worker {worker_id}: Unexpected error processing {current_url}: {e}")
            if current_url and current_url in seed_urls_set:
                with lock:
                    failed_urls.append((current_url, f"Unexpected error: {str(e)}"))
        finally:
            # Only call task_done if it hasn't been called yet
            if current_url is not None and not task_done_called:
                try:
                    queue.task_done()
                except ValueError as e:
                    # This happens when task_done() is called too many times
                    print(f"Worker {worker_id}: Warning - task_done error for {current_url}: {e}")
    
    print(f"Worker {worker_id} finished")

def splora_bot():
    """Main crawling function"""
    # Load seed URLs from CSV file
    csv_file_path = "SB_publication_PMC.csv"
    starting_urls = load_seed_urls_from_csv(csv_file_path)
    
    # Fallback URLs if CSV loading fails
    if not starting_urls:
        print("No URLs loaded from CSV file. Using fallback URLs.")
        starting_urls = [
            "https://www.wikipedia.org/wiki/Google",
            "https://www.bbc.com/news/world",
            "https://news.ycombinator.com/",
        ]
    
    # Remove duplicates while preserving order
    seen = set()
    unique_starting_urls = []
    for url in starting_urls:
        if url not in seen:
            seen.add(url)
            unique_starting_urls.append(url)
    
    starting_urls = unique_starting_urls
    print(f"After removing duplicates: {len(starting_urls)} unique seed URLs")
    
    # Keep track of seed URLs that should be crawled
    seed_urls_set = set(starting_urls)
    remaining_seeds = set(starting_urls)  # Track which seeds haven't been processed
    failed_urls = []
    
    # Initialize data structures
    urls_to_crawl = Queue()
    for seed_url in starting_urls:
        urls_to_crawl.put(seed_url)
    
    visited_urls = set()
    
    # Crawl only seed URLs (no following links to other pages)
    CRAWL_LIMIT = len(starting_urls)  # Will crawl exactly your seed articles
    
    crawl_count = [0]
    
    print(f"Crawl limit set to {CRAWL_LIMIT} pages")
    lock = threading.Lock()
    index = {}
    webpage_info = {}
    pagerank_graph = {}
    webpage_id_counter = [0]
    stop_event = threading.Event()
    
    # Shared data for workers
    shared_data = {
        'queue': urls_to_crawl,
        'visited_urls': visited_urls,
        'crawl_count': crawl_count,
        'CRAWL_LIMIT': CRAWL_LIMIT,
        'lock': lock,
        'index': index,
        'webpage_info': webpage_info,
        'webpage_id_counter': webpage_id_counter,
        'pagerank_graph': pagerank_graph,
        'seed_urls_set': seed_urls_set,
        'failed_urls': failed_urls,
        'remaining_seeds': remaining_seeds
    }
    
    # Start crawling with thread pool
    NUM_WORKERS = 20  # Reduced number of workers to avoid overwhelming the server
    print(f"Starting {NUM_WORKERS} crawler workers...")
    
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        # Submit worker tasks
        futures = []
        for worker_id in range(NUM_WORKERS):
            future = executor.submit(crawl_worker, worker_id, shared_data, stop_event)
            futures.append(future)
        
        # Monitor progress
        while True:
            with lock:
                processed = len(visited_urls & seed_urls_set)  # Intersection of visited and seed URLs
                remaining = len(remaining_seeds)
                
            print(f"Progress: {processed}/{len(starting_urls)} seed URLs processed, {remaining} remaining")
            
            # Check if all seed URLs have been processed or attempted
            if remaining == 0 or processed >= len(starting_urls):
                print("All seed URLs have been processed or attempted")
                stop_event.set()
                break
                
            time.sleep(30)  # Check every 30 seconds
        
        # Wait for all workers to complete
        for future in as_completed(futures, timeout=300):  # 5 minute timeout
            try:
                future.result()
            except Exception as e:
                print(f"Worker encountered error: {e}")
    
    print("All crawling workers have finished")
    
    # Final statistics
    attempted_seeds = set(url for url in visited_urls if url in seed_urls_set)
    failed_seed_urls = set(url for url, reason in failed_urls)
    missing_seeds = seed_urls_set - attempted_seeds - failed_seed_urls
    
    print(f"Total pages crawled: {len(webpage_info)}")
    print(f"Total unique words indexed: {len(index)}")
    print(f"Seed URLs loaded: {len(starting_urls)}")
    print(f"Successfully crawled seed URLs: {len(attempted_seeds)}")
    
    # Report failed URLs
    if failed_urls:
        print(f"\n⚠️  Failed to crawl {len(failed_urls)} URLs:")
        with open('failed_urls.txt', 'w', encoding='utf-8') as f:
            for url, reason in failed_urls:
                print(f"   - {url}: {reason}")
                f.write(f"{url}\t{reason}\n")
        print("Failed URLs saved to 'failed_urls.txt'")
    
    # Report still missing URLs
    if missing_seeds:
        print(f"\n⚠️  {len(missing_seeds)} seed URLs still missing:")
        with open('missing_urls.txt', 'w', encoding='utf-8') as f:
            for url in missing_seeds:
                print(f"   - {url}")
                f.write(f"{url}\n")
        print("Missing URLs saved to 'missing_urls.txt'")
    
    success_rate = (len(attempted_seeds) / len(starting_urls)) * 100
    print(f"\nFinal success rate: {success_rate:.1f}% ({len(attempted_seeds)}/{len(starting_urls)} seed URLs)")
    
    # Retry mechanism for missing URLs
    if missing_seeds and len(missing_seeds) < 50:  # Only retry if reasonable number
        print(f"\n🔄 Retrying {len(missing_seeds)} missing seed URLs...")
        
        # Reset for retry
        stop_event.clear()
        
        # Add missing URLs back to queue
        for url in missing_seeds:
            urls_to_crawl.put(url)
            remaining_seeds.add(url)
        
        # Run another round with fewer workers for the missing URLs
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for worker_id in range(5):
                future = executor.submit(crawl_worker, f"retry-{worker_id}", shared_data, stop_event)
                futures.append(future)
            
            # Monitor retry progress
            retry_start_time = time.time()
            while time.time() - retry_start_time < 300:  # 5 minute timeout for retry
                with lock:
                    remaining = len(remaining_seeds)
                
                if remaining == 0:
                    print("All retry URLs processed")
                    stop_event.set()
                    break
                    
                time.sleep(10)  # Check every 10 seconds
            
            # Set stop event if timeout reached
            if not stop_event.is_set():
                stop_event.set()
            
            # Wait for retry workers to complete
            for future in as_completed(futures, timeout=60):
                try:
                    future.result()
                except Exception as e:
                    print(f"Retry worker encountered error: {e}")
        
        print("Retry phase completed")
    
    # Final final statistics
    attempted_seeds = set(url for url in visited_urls if url in seed_urls_set)
    success_rate = (len(attempted_seeds) / len(starting_urls)) * 100
    print(f"\nFINAL SUCCESS RATE: {success_rate:.1f}% ({len(attempted_seeds)}/{len(starting_urls)} seed URLs)")
    
    # Compute PageRank
    if pagerank_graph:
        print("Computing PageRank scores...")
        pagerank_scores = compute_pagerank(pagerank_graph)
        print(f"PageRank computed for {len(pagerank_scores)} pages")
    else:
        print("Warning: No pages were successfully crawled. Skipping PageRank computation.")
        pagerank_scores = {}
    
    # Save data to CSV files
    print("Saving inverted index to CSV...")
    with open('advanced_pagerank_inverted_index.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['word', 'doc_ids']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for word, doc_ids in index.items():
            writer.writerow({'word': word, 'doc_ids': list(doc_ids)})
    
    print("Saving webpage info and PageRank scores to CSV...")
    with open('advanced_pagerank.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['doc_id', 'url', 'title', 'description', 'pagerank']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for doc_id, info in webpage_info.items():
            writer.writerow({
                'doc_id': doc_id,
                'url': info['url'],
                'title': info['title'],
                'description': info['description'],
                'pagerank': pagerank_scores.get(info['url'], 0)
            })
    
    print("Crawling complete!")

def main():
    splora_bot()

if __name__ == "__main__":
    main()
