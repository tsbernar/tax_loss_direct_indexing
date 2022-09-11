import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional

import pandas as pd

from tax_loss.portfolio import Portfolio
from tax_loss.trade import Trade
from tax_loss.util import read_config

logger = logging.getLogger(__name__)
EMAIL_SERVER = "smtp.gmail.com"
EMAIL_PORT = 587  # TODO constants in constant file


class Emailer:
    def __init__(self, secrets_filepath: str):
        config = read_config(secrets_filepath)
        self.user = config.email_user
        self.pwd = config.email_app_pwd
        self.email_to = config.email_to if "email_to" in config else self.user

    def send_summary_msg(self, current_portfolio: Portfolio, executed_trades: List[Trade], is_dry_run=False) -> None:
        if is_dry_run:
            subject = "[DRY_RUN] Direct Indexing Notification"
        else:
            subject = "Direct Indexing Notification"

        trades_str = str(chr(10) + " ").join(
            [str((t.symbol, t.qty, t.price, t.side, t.exchange_ts)) for t in executed_trades]
        )
        msg = f" --- TRADES --- {trades_str}"

        msg_html = "<h1> TRADES </h1>"
        msg_html += (
            pd.DataFrame(executed_trades)[["symbol", "qty", "price", "side", "exchange_ts"]]
            .sort_values(["side", "qty"])
            .to_html()
        )

        msg += f"\n\n --- PORTFOLIO ---\n{current_portfolio}"
        msg_html += f"<h1> PORTFOLIO </h1>\n{current_portfolio.to_html()}"

        self.send_msg(msg=msg, html_msg=msg_html, subject=subject)

    def send_msg(
        self,
        msg: str,
        html_msg: Optional[str] = None,
        subject: str = "Direct Indexing Notification",
        to: Optional[str] = None,
    ) -> None:
        if to is None:
            to = self.user
        if html_msg is None:
            send_email(self.user, self.pwd, to, subject, msg)
            return
        send_email_html(self.user, self.pwd, to, subject, html_msg, msg)


def send_email_html(user: str, pwd: str, to: str, subject: str, body_html: str, body_text: str) -> None:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to

    html = """\
<html>
  <head>
      <style type="text/css">
      table {
        background: white;
        border-radius:3px;
        border-collapse: collapse;
        height: auto;
        max-width: 900px;
        padding:5px;
        width: 100%;
        animation: float 5s infinite;
      }
      th {
        color:#D5DDE5;;
        background:#1b1e24;
        border-bottom: 4px solid #9ea7af;
        font-size:14px;
        font-weight: 300;
        padding:10px;
        text-align:center;
        vertical-align:middle;
      }
      tr {
        border-top: 1px solid #C1C3D1;
        border-bottom: 1px solid #C1C3D1;
        border-left: 1px solid #C1C3D1;
        color:#666B85;
        font-size:16px;
        font-weight:normal;
      }
      tr:hover td {
        background:#4E5066;
        color:#FFFFFF;
        border-top: 1px solid #22262e;
      }
      td {
        background:#FFFFFF;
        padding:10px;
        text-align:left;
        vertical-align:middle;
        font-weight:300;
        font-size:13px;
        border-right: 1px solid #C1C3D1;
      }
    </style>
</head>
"""
    html += f"""\
  <body>
    {body_html}
  </body>
</html>
"""

    msg.attach(MIMEText(body_text, "plain"))
    msg.attach(MIMEText(html, "html"))

    try:
        server = smtplib.SMTP(EMAIL_SERVER, EMAIL_PORT)
        server.ehlo()
        server.starttls()
        server.login(user, pwd)
        server.sendmail(user, to, msg.as_string())
        server.close()

    except Exception as e:
        logger.warning(f"Failed to send email {e}")


def send_email(user: str, pwd: str, to: str, subject: str, body: str) -> None:
    message = f"From: {user}\nTo: {to}\nSubject: {subject}\n\n{body}"
    try:
        server = smtplib.SMTP(EMAIL_SERVER, EMAIL_PORT)
        server.ehlo()
        server.starttls()
        server.login(user, pwd)
        server.sendmail(user, to, message)
        server.close()
    except Exception as e:
        logger.warning(f"Failed to send email {e}")
