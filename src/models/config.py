class Config:
    args = None

    @staticmethod
    def setup(_args):
        Config.args = _args

    @staticmethod
    def debug():
        return Config.args.debug

    @staticmethod
    def brand():
        return Config.args.brand_wikidata

    @staticmethod
    def postcode():
        return Config.args.postcode

    @staticmethod
    def force_atp_setup():
        return Config.args.force_atp_setup

    @staticmethod
    def force_osm_setup():
        return Config.args.force_osm_setup

    @staticmethod
    def force_atp_dl():
        return Config.args.force_atp_dl

    @staticmethod
    def departement_number():
        return Config.args.departement_number
