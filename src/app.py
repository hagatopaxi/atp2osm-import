import logging
import os
import locale
import json
import psycopg

from flask import Flask, render_template
from werkzeug.middleware.proxy_fix import ProxyFix

from src.config import TEMPLATE_DIR, STATIC_DIR, CACHE_DIR, APP_VERSION, api_url, env
from src.db import teardown_osmdb
from src.extensions import cache
from src.migrate import run_migrations
from src.routes.auth import auth_bp
from src.routes.brands import brands_bp
from src.routes.history import history_bp
from src.routes.misc import misc_bp
from src.routes.todo import todo_bp

logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
app.secret_key = os.getenv("SECRET_KEY", os.urandom(24))

try:
    locale.setlocale(locale.LC_TIME, "fr_FR.utf8")
except locale.Error:
    logging.warning("French locale (fr_FR.UTF-8) not available — date formatting will use system default")

app.config["CACHE_TYPE"] = "FileSystemCache"
app.config["CACHE_DIR"] = CACHE_DIR
app.config["CACHE_THRESHOLD"] = 1000
app.config["CACHE_DEFAULT_TIMEOUT"] = 0  # Infinite cache duration

cache.init_app(app)

app.register_blueprint(auth_bp)
app.register_blueprint(brands_bp)
app.register_blueprint(history_bp)
app.register_blueprint(misc_bp)
app.register_blueprint(todo_bp)

app.teardown_appcontext(teardown_osmdb)


@app.template_filter("parse_comment")
def parse_comment(value):
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


def run_startup_tasks():
    """Run migrations at server startup."""
    try:
        with psycopg.connect(
            dbname=os.getenv("OSM_DB_NAME"),
            user=os.getenv("OSM_DB_USER"),
            password=os.getenv("OSM_DB_PASSWORD"),
            host=os.getenv("OSM_DB_HOST"),
            port=os.getenv("OSM_DB_PORT"),
        ) as conn:
            run_migrations(conn)
    except Exception:
        logger.exception("Startup tasks failed.")
        raise


run_startup_tasks()


@app.context_processor
def inject_globals():
    return {"api_url": api_url, "app_version": APP_VERSION, "is_dev": env == "DEVELOPMENT"}


@app.errorhandler(500)
def internal_error(error):
    return render_template("errors/500.html"), 500


@app.errorhandler(403)
def not_authorized_error(error):
    return render_template("errors/403.html"), 403


@app.errorhandler(404)
def not_found_error(error):
    return render_template("errors/404.html"), 404


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(port=port)
