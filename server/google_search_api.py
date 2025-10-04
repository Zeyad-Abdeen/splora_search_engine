import os
import json
import csv
import ssl
import pickle
import subprocess
import traceback

# Core web and data libraries
from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup

# NLTK for text processing
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize

# Machine Learning and Semantic Search libraries
import torch
from sentence_transformers import SentenceTransformer, util
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Cohere for summarization
import cohere

# --- Summarization Logic (Integrated) ---

# --- !!! مهم جدًا: ضع مفتاح API الخاص بك هنا !!! ---
COHERE_API_KEY = "ZyZs1m9t83atxUHn6t6baz5htSEdi4xsg7vQsOuQ"

def scrape_article_content(url):
    """Scrapes and extracts article content from a given URL."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')
    
    for element in soup(['script', 'style', 'header', 'footer', 'nav', 'aside', 'a']):
        element.decompose()
        
    paragraphs = soup.find_all('p')
    article_text = ' '.join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])
    
    if not article_text or len(article_text.split()) < 50:
        raise Exception("Could not extract enough meaningful text from the article.")
        
    words = article_text.split()
    if len(words) > 4000:
        article_text = ' '.join(words[:4000])
        
    return article_text

def summarize_with_cohere(text):
    """Summarizes text using Cohere's chat model."""
    try:
        co = cohere.Client(COHERE_API_KEY)
        prompt = f"""Please provide a concise and comprehensive summary of the following article. Focus on:
1. The main topic and objective
2. Key findings and important details
3. Main conclusions or takeaways

Keep the summary clear, informative, and around 3-4 sentences.

Article Text:
{text}

Summary:"""
        # نموذج Cohere الأحدث والأقوى
        response = co.chat(
            model='command-a-03-2025',  # النموذج المصحح والحي (Live)
            message=prompt,
            temperature=0.3
        )
        summary = response.text.strip()
        if not summary:
            raise Exception("Cohere API returned an empty summary.")
        return summary
    except Exception as e:
        # إرجاع رسالة خطأ مفصلة من Cohere
        raise Exception(f"Cohere API error: {str(e)}")

def summarize_article(url, title=""):
    """Main function to summarize an article from URL using Cohere API."""
    if not COHERE_API_KEY or "YOUR_COHERE_API_KEY" in COHERE_API_KEY:
         return {"success": False, "error": "Cohere API key is not set. Please update COHERE_API_KEY in the server script.", "title": title, "url": url}
    try:
        article_text = scrape_article_content(url)
        summary = summarize_with_cohere(article_text)
        return {"success": True, "summary": summary, "title": title, "url": url}
    except requests.exceptions.Timeout:
        return { "success": False, "error": "Request timed out while fetching the article.", "title": title, "url": url }
    except requests.exceptions.RequestException as e:
        return { "success": False, "error": f"Failed to fetch the article: {str(e)}", "title": title, "url": url }
    except Exception as e:
        # رسالة الخطأ هنا ستكون مفصلة بسبب الـ raise في summarize_with_cohere
        return { "success": False, "error": f"An error occurred during summarization: {str(e)}", "title": title, "url": url }

# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app)

# --- NLTK Setup ---
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

def download_nltk_resources():
    """Downloads all necessary NLTK resources if they are not found."""
    # يجب أن تكون كل الموارد المطلوبة موجودة هنا
    resources = ["punkt", "stopwords", "punkt_tab"] 
    for resource in resources:
        try:
            if resource == "stopwords":
                nltk.data.find(f"corpora/{resource}")
            else:
                nltk.data.find(f"tokenizers/{resource}")
            # print(f"NLTK resource '{resource}' already present.")
        except LookupError:
            print(f"NLTK resource '{resource}' not found. Downloading...")
            nltk.download(resource)

download_nltk_resources()
stop_words = set(stopwords.words('english'))
ps = PorterStemmer()

# --- Data and Model Loading on Startup ---

# Define paths dynamically
current_dir = os.path.dirname(__file__)
document_info_path = os.path.abspath(os.path.join(current_dir, '..', 'search', 'complete_examples', 'advanced_pagerank.csv'))
pmc_articles_path = os.path.abspath(os.path.join(current_dir, 'pmc_articles_from_csv.json'))
embeddings_path = os.path.abspath(os.path.join(current_dir, 'document_embeddings.pkl'))

def load_document_info(file_path):
    document_info = {}
    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            doc_id = int(row['doc_id'])
            document_info[doc_id] = {
                'url': row['url'],
                'title': row['title'],
                'description': row['description'],
                'pagerank': float(row['pagerank'])
            }
    return document_info

def load_pmc_articles(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            articles = json.load(f)
            return {article.get('link', ''): {'authors': article.get('authors', []), 'publication_date': article.get('publication_date', '')} for article in articles}
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"Warning: Could not load PMC articles from {file_path}")
        return {}

# Load core data
try:
    document_info = load_document_info(document_info_path)
    pmc_articles = load_pmc_articles(pmc_articles_path)
    print("Successfully loaded document info and PMC articles.")
except FileNotFoundError as e:
    print(f"FATAL ERROR: Core data files not found. {e}")
    # لا نغلق التطبيق هنا لأنه قد يكون هناك سيناريو بدون ملفات
    # exit() 

# Load semantic search model and embeddings
try:
    print("Loading semantic search model...")
    semantic_model = SentenceTransformer('all-MiniLM-L6-v2')
    print("Model loaded.")

    print("Loading document embeddings...")
    with open(embeddings_path, 'rb') as f:
        embedding_data = pickle.load(f)
        doc_ids_with_embeddings = embedding_data['doc_ids']
        document_embeddings = torch.tensor(embedding_data['embeddings'])
    print(f"{len(doc_ids_with_embeddings)} document embeddings loaded.")
except FileNotFoundError:
    print(f"FATAL ERROR: document_embeddings.pkl not found at {embeddings_path}")
    # exit()

# --- Core Search and Summarization Logic ---

def parse_query(query):
    # لا حاجة لإضافة try/except هنا بعد تصحيح NLTK
    tokens = word_tokenize(query.lower())
    return [ps.stem(word) for word in tokens if word.isalpha() and word not in stop_words]

def search(query, num_results=10, page=1):
    if not query:
        return []

    # --- Stage 1: Semantic Search ---
    query_embedding = semantic_model.encode(query, convert_to_tensor=True)
    cosine_scores = util.cos_sim(query_embedding, document_embeddings)[0]
    # Get top 100 candidates to refine
    top_semantic_results = torch.topk(cosine_scores, k=min(100, len(cosine_scores)))
    
    semantic_scores = {doc_ids_with_embeddings[idx.item()]: score.item() for score, idx in zip(top_semantic_results[0], top_semantic_results[1])}
    matched_doc_ids = set(semantic_scores.keys())

    # --- Stage 2: TF-IDF Scoring for candidates ---
    processed_query_for_tfidf = ' '.join(parse_query(query))
    corpus = [document_info[doc_id]['description'] for doc_id in matched_doc_ids]
    doc_id_map = list(matched_doc_ids)
    
    try:
        vectorizer = TfidfVectorizer(stop_words='english').fit(corpus)
        tfidf_matrix = vectorizer.transform(corpus)
        query_tfidf = vectorizer.transform([processed_query_for_tfidf])
        cosine_similarities = cosine_similarity(query_tfidf, tfidf_matrix).flatten()
        tfidf_scores = {doc_id: score for doc_id, score in zip(doc_id_map, cosine_similarities)}
    except ValueError:
        tfidf_scores = {doc_id: 0 for doc_id in matched_doc_ids}

    # --- Stage 3: Combine Scores for Final Ranking ---
    results = []
    for doc_id in matched_doc_ids:
        info = document_info[doc_id]
        
        semantic_score = semantic_scores.get(doc_id, 0)
        tfidf_score = tfidf_scores.get(doc_id, 0)
        pagerank_score = info['pagerank']

        # Weighted final score: 70% Semantic, 20% TFIDF, 10% PageRank
        final_score = (0.7 * semantic_score) + (0.2 * tfidf_score) + (0.1 * pagerank_score)

        article_meta = pmc_articles.get(info['url'], {})
        
        results.append({
            'doc_id': doc_id,
            'url': info['url'],
            'title': info['title'],
            'abstract': info['description'],
            'publication_date': article_meta.get('publication_date', ''),
            'authors': article_meta.get('authors', []),
            'pagerank': pagerank_score,
            'final_score': final_score
        })
            
    sorted_results = sorted(results, key=lambda x: x['final_score'], reverse=True)
    
    start = (page - 1) * num_results
    end = start + num_results
    return sorted_results[start:end]

def get_summary(url, title):
    """Calls the integrated summarize_article function."""
    return summarize_article(url, title)

# --- API Endpoints ---

@app.route('/search')
def search_api():
    query = request.args.get('q', '')
    num_results = int(request.args.get('num_results', 10))
    page = int(request.args.get('page', 1))
    if not query:
        return jsonify({'error': 'No query provided'}), 400
    results = search(query, num_results=num_results, page=page)
    return jsonify({
        'query': query,
        'page': page,
        'num_results': num_results,
        'results': results
    })

@app.route('/summarise', methods=['POST'])
def summarise_api():
    data = request.get_json()
    if not data or 'url' not in data:
        return jsonify({'success': False, 'error': 'No URL provided'}), 400
    
    url = data.get('url')
    title = data.get('title', '')
    
    try:
        summary_result = get_summary(url, title)
        status_code = 200 if summary_result.get('success') else 500
        return jsonify(summary_result), status_code
        
    except Exception as e:
        # تصيد أي خطأ غير متوقع وإرجاعه للمستخدم وللطرفية
        print("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!!!!!!!!!!!!!! A CRITICAL ERROR OCCURRED !!!!!!!!!!!!!!!")
        print(f"The error is: {e}")
        traceback.print_exc()
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
        return jsonify({'success': False, 'error': f'A critical server error occurred: {str(e)}'}), 500
