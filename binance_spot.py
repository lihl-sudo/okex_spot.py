from qqmail import get_data
import ccxt

apikey = "m6QsGSReeYDTzJZYiujVSz5P5MkrD8H0EGmPrSzMOIXCC7OZX9v6c1GMZNzygu6q"
secretkey = "H3ZHYddBP5f1f3FX0F8Cv9u5VPYGP941tW6B1HumwFzYvAs6iuGR6xuFpnPro2O3"

binance=ccxt.binance({
    'apiKey': apikey,
    'secret': secretkey,
})

#由于现在直接访问okex会被墙，需要通过代理的方式访问，若是国外服务器则不需要下面的这几行代码
# okex.proxies={
#     'http': 'http://127.0.0.1:10808',
#     'https': 'http://127.0.0.1:10808',
# }
# balance=binance.fetch_balance()
# print(balance)

def stop_order(side,amount):
    if side == "buy":
        order_side = "SELL"
        stop_price = str(int(binance.fetch_ticker("BTCUSDT")["close"]) - 500)
    else:
        order_side = "BUY"
        stop_price = str(int(binance.fetch_ticker("BTCUSDT")["close"]) + 500)
    order_symbol="BTCUSD_PERP"
    order_type='STOP_MARKET'
    order_amount=amount
    # order_price='50000'
    binance.dapiPrivatePostOrder({
        "symbol": order_symbol,
        "side": order_side,
        "type": order_type,
        "quantity": order_amount,
        # "price": order_price,
        "timeInForce": "GTC",
        'positionSide': 'long',
        "stopPrice": stop_price
    })

def take_profit_order(side,amount):
    if side == "buy":
        order_side = "SELL"
        stop_price=str(int(binance.fetch_ticker("BTCUSDT")["close"])+1500)

    else:
        order_side = "BUY"
        stop_price=str(int(binance.fetch_ticker("BTCUSDT")["close"])-1500)
    order_symbol="BTCUSD_PERP"
    order_type='TAKE_PROFIT_MARKET'
    order_amount=amount
    # order_price='50000'
    binance.dapiPrivatePostOrder({
        "symbol": order_symbol,
        "side": order_side,
        "type": order_type,
        "quantity": order_amount,
        # "price": order_price,
        "timeInForce": "GTC",
        'positionSide': 'long',
        "stopPrice": stop_price
    })

if __name__ == '__main__':
    # while True:
    #     data=get_data()
    #     if data:
    #         side=data["side"]
    #         amount=data["amount"]
    #         order=order(side, amount)
    #         print(order)
    take_profit_order("sell", "1")