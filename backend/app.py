import time

from flask import Blueprint, Flask, g, redirect, render_template, request, url_for

import backend.auth as auth
from tax_loss.portfolio import Portfolio


def get_navs():  # dummy data for now
    pf_navs = [
        {"date": "2022-09-13 17:11:44", "nav": 50371.89698535159},
        {"date": "2022-09-14 11:56:25", "nav": 50569.6639951172},
        {"date": "2022-09-15 10:01:38", "nav": 50296.86797070312},
        {"date": "2022-09-16 10:00:50", "nav": 49495.482234375006},
        {"date": "2022-09-16 15:29:50", "nav": 49679.46805859375},
        {"date": "2022-09-16 15:53:22", "nav": 49677.4120390625},
        {"date": "2022-09-19 10:35:11", "nav": 49644.10403906249},
        {"date": "2022-09-19 10:54:19", "nav": 49343.73900671388},
        {"date": "2022-09-20 10:11:45", "nav": 49189.410004882826},
        {"date": "2022-09-21 09:33:08", "nav": 49483.269029296855},
        {"date": "2022-09-22 10:01:54", "nav": 48018.29603417971},
        {"date": "2022-09-23 09:33:14", "nav": 47195.655921875},
        {"date": "2022-09-29 09:05:07", "nav": 46532.41207812501},
        {"date": "2022-10-03 09:05:06", "nav": 46453.731912109375},
        {"date": "2022-10-07 09:05:06", "nav": 46873.61405859373},
        {"date": "2022-10-10 09:04:06", "nav": 46317.19998046875},
        {"date": "2022-10-12 09:55:11", "nav": 45954.55605859372},
        {"date": "2022-10-13 09:04:05", "nav": 45141.67800976561},
        {"date": "2022-10-14 09:04:06", "nav": 46622.872068359386},
        {"date": "2022-10-17 09:04:09", "nav": 46917.01806835937},
        {"date": "2022-10-18 09:04:06", "nav": 47834.24206835937},
        {"date": "2022-10-19 09:04:06", "nav": 47409.876068359365},
        {"date": "2022-10-20 09:04:05", "nav": 47532.61006835937},
        {"date": "2022-10-21 09:04:06", "nav": 47305.33491210936},
        {"date": "2022-10-24 09:05:15", "nav": 48220.877912109376},
        {"date": "2022-10-24 12:36:04", "nav": 48378.17496582032},
        {"date": "2022-10-25 09:05:09", "nav": 48785.48899023438},
        {"date": "2022-10-26 09:05:08", "nav": 49057.84404882813},
        {"date": "2022-10-27 09:05:07", "nav": 48820.90599267577},
    ]

    index_navs = [
        {"date": "2022-09-14", "nav": 50371.89698535159},
        {"date": "2022-09-15", "nav": 49790.26877063981},
        {"date": "2022-09-16", "nav": 49413.096856363685},
        {"date": "2022-09-19", "nav": 49797.888405271646},
        {"date": "2022-09-20", "nav": 49212.446477725214},
        {"date": "2022-09-21", "nav": 48373.02062463615},
        {"date": "2022-09-22", "nav": 47964.104128246094},
        {"date": "2022-09-23", "nav": 47167.85620473746},
        {"date": "2022-09-26", "nav": 46702.703999990925},
        {"date": "2022-09-27", "nav": 46582.714232133076},
        {"date": "2022-09-28", "nav": 47487.74062060056},
        {"date": "2022-09-29", "nav": 46511.231463475466},
        {"date": "2022-09-30", "nav": 45781.08270174244},
        {"date": "2022-10-03", "nav": 46992.46826504964},
        {"date": "2022-10-04", "nav": 48431.06385368238},
        {"date": "2022-10-05", "nav": 48317.45494550089},
        {"date": "2022-10-06", "nav": 47840.051334355005},
        {"date": "2022-10-07", "nav": 46506.12443842315},
        {"date": "2022-10-10", "nav": 46160.19847986379},
        {"date": "2022-10-11", "nav": 45843.63304595689},
        {"date": "2022-10-12", "nav": 45695.560483589354},
        {"date": "2022-10-13", "nav": 46912.049176430155},
        {"date": "2022-10-14", "nav": 45837.2482907618},
        {"date": "2022-10-17", "nav": 47016.71786913104},
        {"date": "2022-10-18", "nav": 47555.394094348616},
        {"date": "2022-10-19", "nav": 47242.65795535127},
        {"date": "2022-10-20", "nav": 46840.566407772545},
        {"date": "2022-10-21", "nav": 47971.528986941536},
        {"date": "2022-10-24", "nav": 48561.26377612615},
        {"date": "2022-10-25", "nav": 49341.197267202195},
        {"date": "2022-10-26", "nav": 48968.46234875719},
    ]

    navs = {"pf_navs": pf_navs, "index_navs": index_navs}
    return navs


def create_app(config: str):
    app = Flask(__name__)
    app.config.from_pyfile(config)

    @app.route("/api/returns")
    def retruns():
        # args = request.args
        # start_ts = None if "start_ts" not in args else args['start_ts']
        # end_ts = None if "end_ts" not in args else args['end_ts']
        time.sleep(0.5)  # TODO remove this, for testing loading screen
        navs = get_navs()
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
            return redirect(url_for("auth.login"))
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
