import numpy as np

from flowlens.bounds import Part, for_key, top_k


def part(rows, u=0, frac=1.0, refine=None):
    return Part(np.array([r[0] for r in rows], dtype=np.uint64),
                np.array([r[2] for r in rows], dtype=float), np.array([r[3] for r in rows], dtype=float),
                [r[1] for r in rows], u, frac, refine)


def test_pruning_example_from_design_doc():
    rows = [(1, "/api/order/create", 9200, 9600), (2, "/api/user/{id}", 7100, 7800), (3, "/api/cart", 5200, 6100),
            (4, "/search", 4300, 5600), (5, "/api/pay", 3000, 4700), (6, "/static/{id}", 1800, 3900),
            (7, "/healthz", 900, 2600)]
    ans = top_k([part(rows)], 3)
    assert [r.key for r in ans.items] == ["/api/order/create", "/api/user/{id}", "/api/cart"]


def test_absent_parts_use_u():
    a = part([(1, "x", 50, 60)], u=10)
    b = part([], u=7)
    assert for_key([a, b], 1) == (50, 67)
    ans = top_k([a, b], 1)
    assert ans.items[0].upper == 67 and ans.unseen_upper == 17


def test_partial_edge_second():
    r = top_k([part([(1, "x", 10, 10)], u=5, frac=0.4)], 1).items[0]
    assert (r.lower, r.upper, r.count) == (0, 10, 4)


def test_refine_tightens_upper():
    live = part([], u=100, refine=lambda h: np.full(len(h), 3))
    other = part([(9, "k", 5, 5)])
    assert top_k([live, other], 1).items[0].upper == 8
