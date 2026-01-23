from src.compute_diff import apply_on_node


def test_apply_on_node_default():
    res = apply_on_node(
        {
            "osm_id": 1,
            "version": 1,
            "tags": {"addr:city": "Babylone"},
            "node_type": "node",
            "lon": 1,
            "lat": 2,
            "atp_city": "Zion",
            "atp_opening_hours": None,
            "atp_country": "FR",
            "atp_postcode": None,
            "atp_website": None,
            "atp_phone": None,
            "atp_email": "contact@babylone.fr",
        }
    )

    target = {
        "id": 1,
        "version": 1,
        "node_type": "node",
        "lon": 1,
        "lat": 2,
        "tag": {
            "addr:city": "Babylone",
            "email": "contact@babylone.fr",
            "addr:country": "FR",
        },
    }

    assert res == target


def test_apply_on_node_keep_contact_phone():
    res = apply_on_node(
        {
            "osm_id": 1,
            "version": 1,
            "tags": {"contact:phone": "0622334455"},
            "node_type": "node",
            "lon": 1,
            "lat": 2,
            "atp_city": "Zion",
            "atp_opening_hours": None,
            "atp_country": None,
            "atp_postcode": None,
            "atp_website": None,
            "atp_phone": "+33622334455",
            "atp_email": "contact@babylone.fr",
        }
    )

    target = {
        "id": 1,
        "version": 1,
        "node_type": "node",
        "lon": 1,
        "lat": 2,
        "tag": {
            "addr:city": "Zion",
            "email": "contact@babylone.fr",
            "contact:phone": "0622334455",
        },
    }

    assert res == target


def test_apply_on_node_keep_contact_email():
    res = apply_on_node(
        {
            "osm_id": 1,
            "version": 1,
            "tags": {"contact:email": "contact@babylone.fr"},
            "node_type": "node",
            "lon": 1,
            "lat": 2,
            "atp_city": "Zion",
            "atp_opening_hours": None,
            "atp_country": None,
            "atp_postcode": None,
            "atp_website": None,
            "atp_phone": None,
            "atp_email": "contact@babylone.fr",
        }
    )

    target = {
        "id": 1,
        "version": 1,
        "node_type": "node",
        "lon": 1,
        "lat": 2,
        "tag": {
            "addr:city": "Zion",
            "contact:email": "contact@babylone.fr",
        },
    }

    assert res == target
