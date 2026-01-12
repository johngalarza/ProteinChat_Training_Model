import numpy as np
import sqlite3
import json
from tqdm import tqdm

data = np.load('protein_index.npz', allow_pickle=True)

X_scaled = data['X_scaled']
protein_names = data['protein_names']
sequences = data['sequences']
organisms = data['organisms']
descriptions = data['descriptions']

conn = sqlite3.connect('protein_index.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS proteins (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    protein_name TEXT NOT NULL,
    sequence TEXT NOT NULL,
    organism TEXT,
    description TEXT,
    features TEXT NOT NULL,
    seq_length INTEGER
)
''')

cursor.execute('CREATE INDEX IF NOT EXISTS idx_protein_name ON proteins(protein_name)')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_seq_length ON proteins(seq_length)')

batch_size = 1000
total = len(protein_names)

for i in tqdm(range(0, total, batch_size)):
    batch_end = min(i + batch_size, total)
    batch_data = []
    
    for j in range(i, batch_end):
        features_json = json.dumps(X_scaled[j].tolist())
        
        batch_data.append((
            str(protein_names[j]),
            str(sequences[j]),
            str(organisms[j]),
            str(descriptions[j]),
            features_json,
            len(sequences[j])
        ))
    
    cursor.executemany('''
        INSERT INTO proteins (protein_name, sequence, organism, description, features, seq_length)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', batch_data)
    
    conn.commit()


cursor.execute('SELECT COUNT(*) FROM proteins')
count = cursor.fetchone()[0]

cursor.execute('SELECT AVG(seq_length), MIN(seq_length), MAX(seq_length) FROM proteins')
avg_len, min_len, max_len = cursor.fetchone()

conn.close()