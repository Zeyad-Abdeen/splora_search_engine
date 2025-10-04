import pandas as pd
from sentence_transformers import SentenceTransformer
import numpy as np
import pickle

print("Loading pre-trained model...")
# نستخدم نموذج خفيف وسريع ومناسب جدًا للبحث
model = SentenceTransformer('all-MiniLM-L6-v2') 

print("Loading document data from CSV...")
# تأكد من أن المسار صحيح
df = pd.read_csv('search/complete_examples/advanced_pagerank.csv')

# تأكد من أن العمود 'description' موجود وليس به قيم فارغة
df['description'] = df['description'].fillna('')

# نستخدم الوصف والعنوان معًا للحصول على معنى أدق
corpus = (df['title'] + ". " + df['description']).tolist()
doc_ids = df['doc_id'].tolist()

print(f"Generating embeddings for {len(corpus)} documents... This may take a while.")

# هذه هي الخطوة التي تحول النصوص إلى أرقام (متجهات)
# show_progress_bar=True ستظهر لك شريط تقدم
document_embeddings = model.encode(corpus, show_progress_bar=True)

print("Embeddings generated successfully.")

# حفظ البيانات لاستخدامها في السيرفر
# سنحفظ المتجهات وقائمة الـ doc_ids المطابقة لها
data_to_save = {
    'doc_ids': doc_ids,
    'embeddings': document_embeddings
}

with open('server/document_embeddings.pkl', 'wb') as f:
    pickle.dump(data_to_save, f)

print("Embeddings and doc_ids saved to server/document_embeddings.pkl")
