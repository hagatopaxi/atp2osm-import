from src.matching import apply_on_node


def match(tags, **atp):
    base = {
        "osm_id": 1,
        "version": 1,
        "node_type": "node",
        "tags": tags,
        "lon": 1,
        "lat": 2,
        "brand": "Babylone",
        "id": "atp-1",
        "source_uri": "https://babylone.fr",
        "source_type": "spider",
        "postcode": "75001",
        "departement_number": "75",
        "atp_opening_hours": None,
        "atp_website": None,
        "atp_phone": None,
        "atp_email": None,
    }
    base.update(atp)
    return apply_on_node(base)


def test_apply_on_node_default():
    res = match({"addr:city": "Babylone"}, atp_email="contact@babylone.fr")
    assert res["id"] == 1
    assert res["tag"] == {
        "addr:city": "Babylone",
        "email": "contact@babylone.fr",
    }


def test_apply_on_node_keep_contact_phone():
    res = match({"contact:phone": "0622334455"}, atp_phone="+33622334455",
                atp_email="contact@babylone.fr")
    assert res["tag"] == {
        "contact:phone": "0622334455",
        "email": "contact@babylone.fr",
    }


def test_apply_on_node_keep_contact_email():
    res = match({"contact:email": "contact@babylone.fr"},
                atp_email="contact@babylone.fr")
    assert res is None


def test_apply_on_node_relation_id_is_negated():
    # osm2pgsql stores relations with a negative area_id
    res = match({}, osm_id=-42, node_type="relation",
                atp_opening_hours="Mo-Fr 09:00-18:00")
    assert res["id"] == 42
    assert res["node_type"] == "relation"
