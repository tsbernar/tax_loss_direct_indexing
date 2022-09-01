import sqlite3

conn = sqlite3.connect("database.db")
cur = conn.cursor()


cur.execute(
    """CREATE TABLE tickers
               (date text, ticker text,  text, qty real, price real)"""
)
