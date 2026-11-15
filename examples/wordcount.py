import csv
from collections import Counter

lines = open('book.txt').readlines()
words = [w.lower() for line in lines for w in line.split() if w.isalpha()]
top = sorted(words, key=lambda w: len(w), reverse=True)
print(len(words), len(set(words)))

counts = {}
for line in lines:
    for w in line.split():
        counts[w] = counts.get(w, 0) + 1

nums = [1, 2, 3, 4, 5]
squares = [x * x for x in nums if x % 2 == 1]
total = 0
for n in nums:
    if n > 2:
        total += n * 10
print(sum(x for x in nums), total, max(squares))

rows = list(csv.DictReader(open('sales.csv')))
revenue = sum(float(r['amount']) for r in rows if r['region'] == 'EU')
by_region = {r['region']: r['amount'] for r in rows}
freq = Counter(r['region'] for r in rows)

for r in rows:
    print(r['id'])
