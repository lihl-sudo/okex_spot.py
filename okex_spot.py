from qqmail import get_data
import ccxt

apikey = "bc203ced-30fe-4f5f-a8aa-2f87018d6354"
secretkey = "A61C6F83E09AFA391F6E8DBEB8D3E701"
password = "lhl19911015"

okex=ccxt.okex({
    'apiKey': apikey,
    'secret': secretkey,
    'password': password
})

#由于现在直接访问okex会被墙，需要通过代理的方式访问，若是国外服务器则不需要下面的这几行代码
okex.proxies={
    'http': 'http://127.0.0.1:10808',
    'https': 'http://127.0.0.1:10808',
}
# balance=okex.fetch_balance()
# pprint(balance)
def buy(side,amount):
    order_symbol='BTC-USDT-SWAP'  #-SWAP'
    order_type='market'
    order_side=side
    order_amount=amount
    # order_price="28888"
    # take_order=okex.create_order(order_symbol,order_type,order_side,order_amount, params={"tdMode":"cross"})
    take_order=okex.privatePostTradeOrderAlgo({ "instId": order_symbol,
                                                "tdMode": "cross",
                                                "side":order_side,
                                                "ordType":"conditional",
                                                "sz": order_amount,
                                                "tpTriggerPx":"40000",
                                                "tpOrdPx":"-1"

    })
    return take_order

def run():
    while True:
        data=get_data()
        if data:
            side=data["side"]
            amount=data["amount"]
            order=buy(side, amount)
            print(order)


if __name__ == '__main__':
    # run()
    buy("sell", "1")