import functools

from flask import Blueprint, flash, g, redirect, render_template, request, session, url_for, current_app
from werkzeug.security import check_password_hash

bp = Blueprint("auth", __name__, url_prefix="/api/auth")


@bp.route("/", methods=["POST"])
def auth_api():
    data = request.json
    password = data["pw"]
    error = None

    secret = current_app.config["PW_HASH"]

    if not check_password_hash(
        secret,
        password,
    ):
        error = "Incorrect password."

    if error is None:
        session.clear()
        session["logged_in"] = True
        return {"message": "success!"}, 200

    return {"message": str(error)}, 403


@bp.before_app_request
def load_logged_in_user():
    logged_in = session.get("logged_in")
    if logged_in:
        g.logged_in = True
    else:
        g.logged_in = False


@bp.route("/logout")
def logout():
    session.clear()
    return {"message": "success!"}, 200


def login_required_api(view):
    @functools.wraps(view)
    def wrapped_view(**kwargs):
        if not g.logged_in:
            response_body = {"message": f"Not authenticated, authenticate with endpoint: {url_for('auth.auth_api')}"}
            return response_body, 403
        return view(**kwargs)

    return wrapped_view
