from src.matching import compose_batch, count_by_departement, pack_departements


def test_empty():
    assert pack_departements({}, 200) == []


def test_everything_fits_in_one_batch():
    assert pack_departements({"75": 50, "69": 30, "13": 20}, 200) == [["13", "69", "75"]]


def test_fills_the_remaining_room_with_the_biggest_that_fits():
    # 180 leaves 20: takes 20, not 15
    assert pack_departements({"75": 180, "69": 20, "13": 15}, 200)[0] == ["69", "75"]


def test_no_batch_exceeds_max_and_all_departements_are_used_once():
    counts = {str(i): (i * 37) % 190 + 1 for i in range(1, 96)}
    batches = pack_departements(counts, 200)

    seen = [dpt for batch in batches for dpt in batch]
    assert sorted(seen) == sorted(counts)
    assert len(seen) == len(set(seen))
    for batch in batches:
        assert sum(counts[dpt] for dpt in batch) <= 200


def test_oversized_departement_gets_its_own_batch():
    # 75 is truncated to 200 by the caller, so it leaves no room for 69
    batches = pack_departements({"75": 500, "69": 10}, 200)
    assert batches == [["75"], ["69"]]


def test_compose_batch_skips_blocked():
    counts = {"69": 120, "59": 90, "33": 60}
    assert compose_batch(counts, blocked={"69"}, max_size=200) == ["33", "59"]


def test_compose_batch_first_batch_only():
    counts = {"69": 150, "59": 150, "33": 150}
    assert compose_batch(counts, blocked=set(), max_size=200) == ["33"]


def test_compose_batch_all_blocked():
    assert compose_batch({"69": 10}, blocked={"69"}, max_size=200) == []


def test_count_by_departement():
    changes = [{"departement_number": d} for d in ("69", "59", "69")]
    assert count_by_departement(changes) == {"69": 2, "59": 1}
