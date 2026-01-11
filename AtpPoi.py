class AtpPoi:
    def __init__(self, tuple):
        self.country = tuple[0]
        self.city = tuple[1]
        self.postcode = tuple[2]
        self.brand_wikidata = tuple[3]
        self.brand = tuple[4]
        self.name = tuple[5]
        self.opening_hours = tuple[6]
        self.website = tuple[7]
        self.phone = tuple[8]
        self.email = tuple[9]
        self.geom = tuple[10]