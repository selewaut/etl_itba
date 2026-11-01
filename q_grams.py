from collections import Counter

def talend_qgram(s1: str, s2: str, q: int = 3) -> float:
    def pad(s):
        return "#" * (q - 1) + s + "#" * (q - 1)

    def trigrams(s):
        p = pad(s)
        return [p[i:i+q] for i in range(len(p) - q + 1)]

    t1 = Counter(trigrams(s1))
    print(len(set(t1)))
    print(t1)
    t2 = Counter(trigrams(s2))
    print(len(set(t2)))
    print(t2)

    only_in_1 = set(t1) - set(t2)
    only_in_2 = set(t2) - set(t1)

    no_overlap = only_in_1 | only_in_2
    print("no overlap", no_overlap)
    print(len(no_overlap))
    inter = sum(min(t1[k], t2[k]) for k in set(t1) | set(t2))
    total = sum(t1.values()) + sum(t2.values())
    return (2 * inter / total) if total else 0.0

# Example from previous question
print(talend_qgram("Algunos errores", "Algunos horrores"))  # 0.8