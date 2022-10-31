import functools

from flask import Blueprint, flash, g, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash

bp = Blueprint("auth", __name__, url_prefix="/auth")


@bp.route("/login", methods=("GET", "POST"))
def login():
    if request.method == "POST":
        password = request.form["password"]
        error = None

        if not check_password_hash(
            "pbkdf2:sha256:260000$SNxnLrxzE8sjRDSh$f11173ccfb1842fba25ca906eebb3bbab1444e91e56a7a0bba1284d578d8b6cd",
            password,
        ):
            error = "Incorrect password."

        if error is None:
            session.clear()
            session["logged_in"] = True
            return redirect(url_for("portfolio.index"))

        flash(error)

    return render_template("auth/login.html")


@bp.route("/api", methods=["POST"])
def auth_api():
    data = request.json
    password = data["pw"]
    error = None

    if not check_password_hash(
        "pbkdf2:sha256:260000$SNxnLrxzE8sjRDSh$f11173ccfb1842fba25ca906eebb3bbab1444e91e56a7a0bba1284d578d8b6cd",
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
    return redirect(url_for("portfolio.index"))


def login_required_api(view):
    @functools.wraps(view)
    def wrapped_view(**kwargs):
        if not g.logged_in:
            response_body = {"message": f"Not authenticated, authenticate with endpoint: {url_for('auth.auth_api')}"}
            return response_body, 403
        return view(**kwargs)

    return wrapped_view


def login_required(view):
    @functools.wraps(view)
    def wrapped_view(**kwargs):
        if g.user is None:
            return redirect(url_for("auth.login"))

        return view(**kwargs)

    return wrapped_view
