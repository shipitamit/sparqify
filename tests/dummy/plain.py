nums = [1, 2, 3, 4, 5]
sq = [n * n for n in nums if n % 2 == 1]
total = sum(sq)
from collections import Counter
c = Counter(w for w in "a b a c".split())
