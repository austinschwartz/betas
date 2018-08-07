from flask import Flask
from flask import redirect
from flask import request
from flask import render_template
from flask import Response

from .src.quandl_api import QuandlAPI
from .src.beta_calculator import BetaCalculator
from .src.plots import chart

app = Flask(__name__)

QUANDL_API = QuandlAPI('gJu9gtGNuDsq-ezhhsP2')
BETAS = BetaCalculator(QUANDL_API)


def ticker_mapping():
    tickers = {}
    i = 0
    m = QUANDL_API.ticker_mapping()
    # Take first 500 stocks we have available.
    # There are 3000+ in the dataset, but it takes ~2 hrs to download
    for ticker in m:
        if i > 500:
            break
        i += 1
        tickers[ticker] = m[ticker]
    return tickers


TICKER_MAPPING = ticker_mapping()
BETAS.build_df(TICKER_MAPPING.keys())


def handle_error(error):
    return "Error: {}".format(error) + "<br/><br/>" + "<a href='/betas'>Betas</a>"


@app.route('/')
def hello():
    return render_template("readme.html")


@app.route('/avail')
def avail():
    resp = '\n'.join([key for key in TICKER_MAPPING])
    return Response(resp, mimetype='text/plain')


@app.route('/betas/<stocks>/<int:window>')
def betas(stocks, window):
    if BETAS.df_changes is None:
        BETAS.build_df(TICKER_MAPPING.keys())

    # Handle out of bounds window sizes
    if window == 1:
        return handle_error("Window needs to be > 1")
    elif window >= len(BETAS.df_changes):
        return handle_error("Window is too large")

    # Default to 1 year window
    if window is None:
        window = 365

    stocks = stocks.split(',')
    try:
        data = BETAS.betas(stocks, window)
    except Exception as e:
        return handle_error(e)

    if data is None:
        return handle_error("Couldn't retrieve betas")

    script, div = chart(data)

    return render_template("chart.html",
                           window=str(window),
                           betas=data.iloc[-1].to_dict(),
                           stocks="[{}]".format(', '.join(stocks)),
                           the_div=div,
                           the_script=script)


@app.route('/betas', methods=['POST'])
def betas_post():
    stocks = request.form.get('stock').replace(" ", "")
    window = request.form.get('window')
    if window:
        return redirect('/betas/{}/{}'.format(stocks, window), code=302)
    return redirect('/betas/{}'.format(stocks), code=302)


@app.route('/betas', methods=['GET'])
def betas_default():
    # Default to showing these tickers + 3 year window
    return redirect('/betas/AAPL,AMZN,MSFT/1095')


@app.route('/betas/')
def betas_redirect():
    return betas_default()


@app.route('/betas/<stocks>')
def betas_no_window(stocks):
    return betas(stocks, 365)
