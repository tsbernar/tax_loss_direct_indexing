import time
import pandas as pd
from functools import lru_cache

from flask import Blueprint, Flask, g, render_template, request, current_app

import backend.auth as auth
from tax_loss.portfolio import Portfolio


def get_ttl_hash(seconds=300):
    return round(time.time() / seconds)


@lru_cache()
def get_navs(ttl_hash):
    try:
        filepath = current_app.config["CSV_DIR"] + "/nav.csv"
        pf_navs = pd.read_csv(filepath, names=["date", "nav"])
        pf_navs = pf_navs.to_dict("records")
        filepath = current_app.config["PRICE_FILE"]
        index_navs = pd.read_parquet(filepath, columns=["IVV"])
        index_navs = index_navs[index_navs.index > pf_navs[0]["date"]]
        # convert to returns and then start from pf starting value
        index_navs["IVV"] = index_navs["IVV"] / index_navs["IVV"].iloc[0]
        index_navs["IVV"] * pf_navs[0]["nav"]
        index_navs = index_navs.reset_index()
        index_navs = index_navs.rename({"index": "date", "IVV": "nav"}, axis=1)
        index_navs["date"] = index_navs["date"].astype(str)
        index_navs = index_navs.to_dict("records")
    except Exception:
        pf_navs = [
            {"date": str(pd.to_datetime("now")), "nav": 0},
        ]

        index_navs = [
            {"date": str(pd.to_datetime("now")), "nav": 0},
        ]

    navs = {"pf_navs": pf_navs, "index_navs": index_navs}
    return navs


def create_app(config: str):
    app = Flask(__name__)
    app.config.from_pyfile(config)

    @app.route("/api/returns")
    @auth.login_required_api
    def retruns():
        navs = get_navs(ttl_hash=get_ttl_hash())
        returns = {"index_returns": [], "pf_returns": []}
        for ret_key, nav_key in zip(["index_returns", "pf_returns"], ["index_navs", "pf_navs"]):
            returns[ret_key] = [
                {"date": n["date"], "return": n["nav"] / navs[nav_key][0]["nav"] - 1} for n in navs[nav_key]
            ]

        return returns

    @app.route("/api/holdings")
    @auth.login_required_api
    def holdings():
        args = request.args

        portfolio = Portfolio(filename="data/portfolio.json")

        # data = request.get_json()
        response_body = {
            "nav": portfolio.nav,
            "positions": portfolio._generate_positions_table(None, False),
            "args": args,
        }
        return response_body

    @app.get("/api/parameters")
    @auth.login_required_api
    def get_parameters():
        response_body = {
            "max_stocks": 100,
            "tax_coefficient": 0.6,
            "tracking_error_func": "tracking_error_func",
            "max_total_deviation": 0.6,
            "cash_constraint": 0.95,
        }
        return response_body

    @app.post("/api/parameters")
    @auth.login_required_api
    def update_parameters():
        if not g.logged_in:
            response_body = {"message": "Not authenticated"}
            return response_body, 403

    app.register_blueprint(auth.bp)
    bp = Blueprint("portfolio", __name__)

    @bp.route("/index/")
    def index():
        if not g.logged_in:
            return {"message": "Not authenticated"}, 403
        portfolio = Portfolio(filename="data/portfolio.json")
        sorting = request.args.get("sorting", "")
        if sorting == "loss_sorted":
            pf = portfolio.head(None).replace(" ", "&nbsp").split("\n")
        else:
            pf = portfolio.head(None, False).replace(" ", "&nbsp").split("\n")
        return render_template("portfolio/index.html", portfolio=pf, sorting=sorting)

    app.register_blueprint(bp)
    app.add_url_rule("/", endpoint="portfolio.index")

    return app
