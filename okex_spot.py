from pprint import pprint

from qqmail import get_data
import ccxt

apikey = ""
secretkey = ""
password = "lhl19911015"

okex=ccxt.okex({
    'apiKey': apikey,
    'secret': secretkey,
    'password': password
})

#由于现在直接访问okex会被墙，需要通过代理的方式访问，若是国外服务器则不需要下面的这几行代码
# okex.proxies={
#     'http': 'http://127.0.0.1:10808',
#     'https': 'http://127.0.0.1:10808',
# }
# balance=okex.fetch_balance()
# pprint(balance)
def limit_order(side,amount):
    order_symbol='BTC-USDT-SWAP'  #-SWAP'
    order_side=side
    order_amount=amount
    order_price="28000"
    take_order=okex.create_limit_order(order_symbol,order_side,order_amount,order_price,params={"tdMode":"cross"})
    return take_order

def market_order(side,amount):
    order_symbol='BTC-USDT-SWAP'  #-SWAP'
    order_side=side
    order_amount=amount
    take_order=okex.create_market_order(order_symbol,order_side,order_amount,params={"tdMode":"cross"})
    return take_order

def stop_order(side,amount):
    if side == "buy":
        order_side = "sell"
        stop_price = str(int(okex.fetch_ticker("BTC-USDT-SWAP")["close"]) - 500)
    else:
        order_side = "buy"
        stop_price = str(int(okex.fetch_ticker("BTC-USDT-SWAP")["close"]) + 500)
    order_symbol="BTC-USDT-SWAP"
    order_amount=amount
    # order_price='50000'
    okex.privatePostTradeOrderAlgo({"instId": order_symbol,
                                     "tdMode": "cross",
                                     "side": order_side,
                                     "ordType": "conditional",
                                     "sz": order_amount,
                                     "slTriggerPx": stop_price,
                                     "slOrdPx":'-1'
                                     })

def take_profit_order(side,amount):
    if side == "buy":
        order_side = "sell"
        stop_price=str(int(okex.fetch_ticker("BTC-USDT-SWAP")["close"])+1000)

    else:
        order_side = "buy"
        stop_price=str(int(okex.fetch_ticker("BTC-USDT-SWAP")["close"])-1000)
    order_symbol="BTC-USDT-SWAP"
    order_amount=amount
    # order_price='50000'
    okex.privatePostTradeOrderAlgo({"instId": order_symbol,
                                    "tdMode": "cross",
                                    "side": order_side,
                                    "ordType": "conditional",
                                    "sz": order_amount,
                                    "tpTriggerPx": stop_price,
                                    "tpOrdPx": "-1",
                                    })

def run():
    while True:
        try:
            data=get_data()
            if data:
                side=data["side"]
                amount=data["amount"]
                algo_list = okex.privateGetTradeOrdersAlgoPending({"ordType": "conditional"})
                body_list = []
                for i in algo_list["data"]:
                    body_list.append({"algoId": i["algoId"], "instId": "BTC-USDT-SWAP"})
                if body_list:
                    okex.privatePostTradeCancelAlgos(body_list)
                    okex.privatePostTradeClosePosition({"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "autoCxl": "true"})
                # resp = market_order(side, amount)
                resp = limit_order(side, amount)
                print(resp)
                take_profit_order(side, amount)
                stop_order(side, amount)
        except Exception as e:
            print(e)
            continue



if __name__ == '__main__':
    run()
    # algo_list = okex.private
    # okex.privatePostTradeClosePosition({"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "autoCxl": "true"})
    # order("sell", "1")
    # take_profit_order("sell", "1")
    # stop_order("sell", "1")
    # okex.privatePostGetTradeClosePosition({"instId":"BTC-USDT-SWAP","mgnMode":"cross","autoCxl":"true"})
    # pprint(dir(ccxt.okex()))
