from collections import defaultdict
from datetime import datetime

WINDOW_SECONDS = 600


def find_correlated_pairs(events):
    by_ip = defaultdict(list)
    for event in events:
        by_ip[event["ip"]].append(event)

    pairs = []
    for ip, group in by_ip.items():
        n = len(group)
        for i in range(n):
            for j in range(i + 1, n):
                a, b = group[i], group[j]
                if a["email"] == b["email"]:
                    continue
                ts_a = datetime.fromisoformat(a["ts"])
                ts_b = datetime.fromisoformat(b["ts"])
                diff = abs((ts_a - ts_b).total_seconds())
                if diff <= WINDOW_SECONDS:
                    pairs.append((a["email"], b["email"], ip, diff))

    return pairs
