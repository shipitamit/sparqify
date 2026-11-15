import os
import time
import pandas as pd

seen = set()
while True:
    for name in os.listdir('/data/incoming'):
        if name in seen or not name.endswith('.csv'):
            continue
        df = pd.read_csv(os.path.join('/data/incoming', name))
        df = df[df['amount'] > 0]
        df['total'] = df['amount'] * df['qty']
        df.to_parquet('/data/processed/' + name + '.parquet')
        seen.add(name)
    time.sleep(10)
