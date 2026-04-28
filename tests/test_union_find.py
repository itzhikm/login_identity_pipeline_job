from src.union_find import UnionFind, build_groups


def test_find_creates_singleton_on_first_lookup():
    uf = UnionFind()
    assert uf.find("a") == "a"
    assert uf.parent["a"] == "a"


def test_find_returns_root_after_union():
    uf = UnionFind()
    uf.union("a", "b")
    assert uf.find("a") == uf.find("b")


def test_find_applies_path_compression():
    uf = UnionFind()
    uf.parent = {"a": "b", "b": "c", "c": "c"}
    uf.find("a")
    assert uf.parent["a"] == "c"
    assert uf.parent["b"] == "c"


def test_union_merges_two_singletons():
    uf = UnionFind()
    uf.union("a", "b")
    assert uf.find("a") == uf.find("b")


def test_union_is_idempotent_for_same_root():
    uf = UnionFind()
    uf.union("a", "b")
    parent_before = dict(uf.parent)
    uf.union("a", "b")
    assert uf.find("a") == uf.find("b")
    assert dict(uf.parent) == parent_before or uf.find("a") == uf.find("b")


def test_union_chains_transitively():
    uf = UnionFind()
    uf.union("a", "b")
    uf.union("b", "c")
    assert uf.find("a") == uf.find("c")


def test_get_groups_filters_singletons():
    uf = UnionFind()
    uf.find("loner")
    uf.union("a", "b")
    groups = uf.get_groups()
    assert len(groups) == 1
    assert sorted(groups[0]) == ["a", "b"]


def test_get_groups_returns_each_connected_component():
    uf = UnionFind()
    uf.union("a", "b")
    uf.union("c", "d")
    uf.union("d", "e")
    groups = [sorted(g) for g in uf.get_groups()]
    groups.sort()
    assert groups == [["a", "b"], ["c", "d", "e"]]


def test_get_groups_empty_when_nothing_unioned():
    assert UnionFind().get_groups() == []


def test_build_groups_from_pairs():
    pairs = [
        ("a", "b", "1.1.1.1", 10.0),
        ("b", "c", "1.1.1.1", 20.0),
        ("x", "y", "2.2.2.2", 5.0),
    ]
    groups = [sorted(g) for g in build_groups(pairs)]
    groups.sort()
    assert groups == [["a", "b", "c"], ["x", "y"]]


def test_build_groups_empty_input():
    assert build_groups([]) == []


def test_build_groups_ignores_extra_tuple_fields():
    pairs = [("a", "b", "ip", 1.0)]
    assert [sorted(g) for g in build_groups(pairs)] == [["a", "b"]]
