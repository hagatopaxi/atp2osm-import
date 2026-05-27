import os
import functools
from urllib.parse import urlparse

from flask import Blueprint, session, request, redirect, url_for, abort, Response
from requests_oauthlib import OAuth2Session

from src.config import api_url

auth_bp = Blueprint("auth", __name__)

client_id = os.getenv("OSM_OAUTH_CLIENT_ID")
client_secret = os.getenv("OSM_OAUTH_CLIENT_SECRET")
authorization_base_url = f"{api_url}/oauth2/authorize"
token_url = f"{api_url}/oauth2/token"
scope = ["write_api", "read_prefs"]


def auth_required(f):
    @functools.wraps(f)
    def decorator(*args, **kwargs):
        if "user" not in session:
            return abort(403)
        if "token" not in session:
            session.clear()
            return redirect("/?session_expired=1")
        return f(*args, **kwargs)

    return decorator


def get_oauth_redirect_uri():
    base_url = os.getenv("APP_BASE_URL", "").rstrip("/")
    if base_url:
        return f"{base_url}/oauth-callback"
    return url_for("auth.oauth_callback", _external=True)


@auth_bp.route("/login", methods=["POST"])
def login():
    redirect_uri = get_oauth_redirect_uri()

    osm = OAuth2Session(client_id, redirect_uri=redirect_uri, scope=scope)
    authorization_url, state = osm.authorization_url(authorization_base_url)
    session["oauth_state"] = state

    data = request.get_json(silent=True) or {}
    next_url = data.get("next", "/")
    # Protected against open-redirect attack, see https://owasp.org/www-community/attacks/open_redirect
    parsed = urlparse(next_url)
    if parsed.netloc or parsed.scheme or not next_url.startswith("/"):
        next_url = "/"
    session["oauth_next"] = next_url

    return authorization_url


@auth_bp.route("/oauth-callback")
def oauth_callback():
    if "error" in request.args:
        return "Authentication failed: " + request.args["error"], 401

    # Validate state
    if request.args.get("state") != session.get("oauth_state"):
        return "Invalid state parameter", 401

    redirect_uri = get_oauth_redirect_uri()

    osm = OAuth2Session(
        client_id, redirect_uri=redirect_uri, state=session["oauth_state"]
    )

    authorization_response = request.url
    if redirect_uri.startswith("https://") and authorization_response.startswith(
        "http://"
    ):
        authorization_response = "https://" + authorization_response[7:]
    token = osm.fetch_token(
        token_url,
        client_secret=client_secret,
        authorization_response=authorization_response,
        headers={"User-Agent": f"atp2osm/{os.getenv('APP_VERSION')}"},
    )
    user_detail_url = f"{api_url}/api/0.6/user/details.json"
    response = osm.get(user_detail_url)
    res_json = response.json()
    user = {"osm_id": res_json["user"]["id"], "name": res_json["user"]["display_name"]}
    del session["oauth_state"]
    session["user"] = user
    session["token"] = dict(token)

    next_url = session.pop("oauth_next", "/")
    return redirect(next_url)


@auth_bp.route("/logout", methods=["POST"])
@auth_required
def logout():
    # clean the session
    session.clear()

    return Response(status=204)
