class OsmPoi:
    def __init__(self, _osm_poi):
        self.osm_id = _osm_poi[0]
        self.node_type = _osm_poi[1]
        self.name = _osm_poi[2]
        self.brand_wikidata = _osm_poi[3]
        self.brand = _osm_poi[4]
        self.city = _osm_poi[5]
        self.postcode = _osm_poi[6]
        self.opening_hours = _osm_poi[7]
        self.website = _osm_poi[8]
        self.phone = _osm_poi[9]
        self.email = _osm_poi[10]
        self.geom = _osm_poi[11]

    def __str__(self):
        return f"OsmPoi(osm_id={self.osm_id}, node_type={self.node_type}, name={self.name}, brand_wikidata={self.brand_wikidata}, brand={self.brand}, city={self.city}, postcode={self.postcode}, opening_hours={self.opening_hours}, website={self.website}, phone={self.phone}, email={self.email})"