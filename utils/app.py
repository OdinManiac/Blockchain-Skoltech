from dash import Dash, dcc, html, Output
import dash
import plotly.express as px
from utils.data import get_default_data, get_data

from config import CONFIG

app = Dash(__name__)

app.layout = html.Pre(
    [
        html.H2(
            "Enter any wallet-token addresses combination you want!",
            style={"text-align": "center"},
        ),
        html.Div(
            [
                "Wallet address:",
                dcc.Input(
                    id="wallet_address",
                    value=CONFIG.default.wallet_address,
                    type="text",
                    children=["This is integer "],
                ),
                "          Token address:",
                dcc.Input(
                    id="token_address",
                    value=CONFIG.default.token_address,
                    type="text",
                ),
            ],
            style={"text-align": "center"},
        ),
        html.Div(
            html.Button("Submit", id="submit-conf-button"),
            style={"text-align": "center"},
        ),
        html.Div(id="symbol"),
        dcc.Graph(id="trades"),
        dcc.Graph(id="PNL"),
        html.Img(
            src=CONFIG.default.degen_ape,
            id="image",
            style={"width": "50%", "height": "50%"},
        ),
    ],
    style={"textAlign": "center"},
)


@app.callback(
    [Output("trades", "figure"), Output("PNL", "figure"), Output("symbol", "children")],
    [dash.dependencies.Input("submit-conf-button", "n_clicks")],
    [
        dash.dependencies.State("wallet_address", "value"),
        dash.dependencies.Input("token_address", "value"),
    ],
)
def update_line_chart(n_clicks, wallet_address, token_address):
    if not n_clicks is None:
        quotes_df, pnl_df, in_transfers, out_transfers, token_symbol = get_data(
            token_address=token_address,
            wallet_address=wallet_address,
        )
    else:
        (
            quotes_df,
            pnl_df,
            in_transfers,
            out_transfers,
            token_symbol,
        ) = get_default_data()

    fig_trades = px.line(quotes_df, x="block_number", y="native_quote")
    fig_trades.update_layout(
        title="Trade history",
        xaxis_title="Block number",
        yaxis_title="Quote value",
        font=dict(family="Courier New, monospace", size=18, color="#7f7f7f"),
    )

    fig_pnl = px.line(pnl_df, x="block_number", y="pnl")
    fig_pnl.update_layout(
        title="PNL history",
        xaxis_title="Block number",
        yaxis_title="PNL value",
        font=dict(family="Courier New, monospace", size=18, color="#7f7f7f"),
    )

    for i in in_transfers:
        fig_trades.add_vline(x=i, line_width=1, line_color="green")
        fig_pnl.add_vline(x=i, line_width=1, line_color="green")

    for o in out_transfers:
        fig_trades.add_vline(x=o, line_width=1, line_color="red")
        fig_pnl.add_vline(x=o, line_width=1, line_color="red")

    return fig_trades, fig_pnl, token_symbol


app.run_server(debug=False, host=CONFIG.host, port=CONFIG.port)
