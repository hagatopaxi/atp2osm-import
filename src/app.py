import logging

from flask import Flask, render_template


logger = logging.getLogger(__name__)

app = Flask(
    __name__, template_folder="/home/gwenael/dev/osm/atp2osm-import/website/templates"
)
# app.config.from_object("config")


@app.route("/")
def home():
    return render_template("home.html")


@app.errorhandler(500)
def internal_error(error):
    # db_session.rollback()
    return render_template("errors/500.html"), 500


@app.errorhandler(404)
def not_found_error(error):
    return render_template("errors/404.html"), 404


# Default port:
if __name__ == "__main__":
    app.run()
