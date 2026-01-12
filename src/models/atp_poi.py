class AtpPoi:
    def __init__(self, _apt_poi):
        self.country = _apt_poi[0]
        self.city = _apt_poi[1]
        self.postcode = _apt_poi[2]
        self.brand_wikidata = _apt_poi[3]
        self.brand = _apt_poi[4]
        self.name = _apt_poi[5]
        self.opening_hours = _apt_poi[6]
        self.website = _apt_poi[7]
        self.phone = _apt_poi[8]
        self.email = _apt_poi[9]
        self.geom = _apt_poi[10]

    def __str__(self):
        return f"AtpPoi(country={self.country}, city={self.city}, postcode={self.postcode}, brand_wikidata={self.brand_wikidata}, brand={self.brand}, name={self.name}, opening_hours={self.opening_hours}, website={self.website}, phone={self.phone}, email={self.email})"