import gzip
import json

file_path = "/Users/shravanisawant/Downloads/2026-04-15-12.json.gz"

with gzip.open(file_path, 'rt', encoding='utf-8') as f:
    for i, line in enumerate(f):
        event = json.loads(line)
        print(event)
        if i == 2:
            break