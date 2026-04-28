from collections import defaultdict


class UnionFind:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
            return x

        root = x
        while self.parent[root] != root:
            root = self.parent[root]

        node = x
        while self.parent[node] != root:
            self.parent[node], node = root, self.parent[node]

        return root

    def union(self, x, y):
        root_x = self.find(x)
        root_y = self.find(y)
        if root_x != root_y:
            self.parent[root_x] = root_y

    def get_groups(self):
        groups = defaultdict(list)
        for node in self.parent:
            groups[self.find(node)].append(node)
        return [members for members in groups.values() if len(members) > 1]


def build_groups(pairs):
    uf = UnionFind()
    for email_a, email_b, _ip, _diff in pairs:
        uf.union(email_a, email_b)
    return uf.get_groups()
