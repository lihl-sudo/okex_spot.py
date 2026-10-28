import asyncio
import websockets
import json
import requests
import hmac
import base64
import zlib
import datetime
import time


def get_timestamp():
    now = datetime.datetime.now()
    t = now.isoformat("T", "milliseconds")
    return t + "Z"


def get_server_time():
    url = "https://www.okex.com/api/v5/public/time"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['data'][0]['ts']
    else:
        return ""


def get_local_timestamp():
    return int(time.time())


def login_params(timestamp, api_key, passphrase, secret_key):
    message = timestamp + 'GET' + '/users/self/verify'

    mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
    d = mac.digest()
    sign = base64.b64encode(d)

    login_param = {"op": "login", "args": [{"apiKey": api_key,
                                            "passphrase": passphrase,
                                            "timestamp": timestamp,
                                            "sign": sign.decode("utf-8")}]}
    login_str = json.dumps(login_param)
    return login_str


def partial(res):
    data_obj = res['data'][0]
    bids = data_obj['bids']
    asks = data_obj['asks']
    instrument_id = res['arg']['instId']
    # print('全量数据bids为：' + str(bids))
    # print('档数为：' + str(len(bids)))
    # print('全量数据asks为：' + str(asks))
    # print('档数为：' + str(len(asks)))
    return bids, asks, instrument_id


def update_bids(res, bids_p):
    # 获取增量bids数据
    bids_u = res['data'][0]['bids']
    # print('增量数据bids为：' + str(bids_u))
    # print('档数为：' + str(len(bids_u)))
    # bids合并
    for i in bids_u:
        bid_price = i[0]
        for j in bids_p:
            if bid_price == j[0]:
                if i[1] == '0':
                    bids_p.remove(j)
                    break
                else:
                    del j[1]
                    j.insert(1, i[1])
                    break
        else:
            if i[1] != "0":
                bids_p.append(i)
    else:
        bids_p.sort(key=lambda price: sort_num(price[0]), reverse=True)
        # print('合并后的bids为：' + str(bids_p) + '，档数为：' + str(len(bids_p)))
    return bids_p


def update_asks(res, asks_p):
    # 获取增量asks数据
    asks_u = res['data'][0]['asks']
    # print('增量数据asks为：' + str(asks_u))
    # print('档数为：' + str(len(asks_u)))
    # asks合并
    for i in asks_u:
        ask_price = i[0]
        for j in asks_p:
            if ask_price == j[0]:
                if i[1] == '0':
                    asks_p.remove(j)
                    break
                else:
                    del j[1]
                    j.insert(1, i[1])
                    break
        else:
            if i[1] != "0":
                asks_p.append(i)
    else:
        asks_p.sort(key=lambda price: sort_num(price[0]))
        # print('合并后的asks为：' + str(asks_p) + '，档数为：' + str(len(asks_p)))
    return asks_p


def sort_num(n):
    if n.isdigit():
        return int(n)
    else:
        return float(n)


def check(bids, asks):
    # 获取bid档str
    bids_l = []
    bid_l = []
    count_bid = 1
    while count_bid <= 25:
        if count_bid > len(bids):
            break
        bids_l.append(bids[count_bid-1])
        count_bid += 1
    for j in bids_l:
        str_bid = ':'.join(j[0 : 2])
        bid_l.append(str_bid)
    # 获取ask档str
    asks_l = []
    ask_l = []
    count_ask = 1
    while count_ask <= 25:
        if count_ask > len(asks):
            break
        asks_l.append(asks[count_ask-1])
        count_ask += 1
    for k in asks_l:
        str_ask = ':'.join(k[0 : 2])
        ask_l.append(str_ask)
    # 拼接str
    num = ''
    if len(bid_l) == len(ask_l):
        for m in range(len(bid_l)):
            num += bid_l[m] + ':' + ask_l[m] + ':'
    elif len(bid_l) > len(ask_l):
        # bid档比ask档多
        for n in range(len(ask_l)):
            num += bid_l[n] + ':' + ask_l[n] + ':'
        for l in range(len(ask_l), len(bid_l)):
            num += bid_l[l] + ':'
    elif len(bid_l) < len(ask_l):
        # ask档比bid档多
        for n in range(len(bid_l)):
            num += bid_l[n] + ':' + ask_l[n] + ':'
        for l in range(len(bid_l), len(ask_l)):
            num += ask_l[l] + ':'

    new_num = num[:-1]
    int_checksum = zlib.crc32(new_num.encode())
    fina = change(int_checksum)
    return fina


def change(num_old):
    num = pow(2, 31) - 1
    if num_old > num:
        out = num_old - num * 2 - 2
    else:
        out = num_old
    return out


# subscribe channels un_need login
async def subscribe_without_login(url, channels):
    l = []
    while True:
        try:
            async with websockets.connect(url) as ws:
                sub_param = {"op": "subscribe", "args": channels}
                sub_str = json.dumps(sub_param)
                await ws.send(sub_str)
                print(f"send: {sub_str}")

                while True:
                    try:
                        res = await asyncio.wait_for(ws.recv(), timeout=25)
                    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed) as e:
                        try:
                            await ws.send('ping')
                            res = await ws.recv()
                            print(res)
                            continue
                        except Exception as e:
                            print("连接关闭，正在重连……")
                            break

                    print(get_timestamp() + res)
                    res = eval(res)
                    if 'event' in res:
                        continue
                    for i in res['arg']:
                        if 'books' in res['arg'][i] and 'books5' not in res['arg'][i]:
                            # 订阅频道是深度频道
                            if res['action'] == 'snapshot':
                                for m in l:
                                    if res['arg']['instId'] == m['instrument_id']:
                                        l.remove(m)
                                # 获取首次全量深度数据
                                bids_p, asks_p, instrument_id = partial(res)
                                d = {}
                                d['instrument_id'] = instrument_id
                                d['bids_p'] = bids_p
                                d['asks_p'] = asks_p
                                l.append(d)

                                # 校验checksum
                                checksum = res['data'][0]['checksum']
                                # print('推送数据的checksum为：' + str(checksum))
                                check_num = check(bids_p, asks_p)
                                # print('校验后的checksum为：' + str(check_num))
                                if check_num == checksum:
                                    print("校验结果为：True")
                                else:
                                    print("校验结果为：False，正在重新订阅……")

                                    # 取消订阅
                                    await unsubscribe_without_login(url, channels)
                                    # 发送订阅
                                    async with websockets.connect(url) as ws:
                                        sub_param = {"op": "subscribe", "args": channels}
                                        sub_str = json.dumps(sub_param)
                                        await ws.send(sub_str)
                                        print(f"send: {sub_str}")

                            elif res['action'] == 'update':
                                for j in l:
                                    if res['arg']['instId'] == j['instrument_id']:
                                        # 获取全量数据
                                        bids_p = j['bids_p']
                                        asks_p = j['asks_p']
                                        # 获取合并后数据
                                        bids_p = update_bids(res, bids_p)
                                        asks_p = update_asks(res, asks_p)

                                        # 校验checksum
                                        checksum = res['data'][0]['checksum']
                                        # print('推送数据的checksum为：' + str(checksum))
                                        check_num = check(bids_p, asks_p)
                                        # print('校验后的checksum为：' + str(check_num))
                                        if check_num == checksum:
                                            print("校验结果为：True")
                                        else:
                                            print("校验结果为：False，正在重新订阅……")

                                            # 取消订阅
                                            await unsubscribe_without_login(url, channels)
                                            # 发送订阅
                                            async with websockets.connect(url) as ws:
                                                sub_param = {"op": "subscribe", "args": channels}
                                                sub_str = json.dumps(sub_param)
                                                await ws.send(sub_str)
                                                print(f"send: {sub_str}")
        except Exception as e:
            print("连接断开，正在重连……")
            continue


# subscribe channels need login
async def subscribe(url, api_key, passphrase, secret_key, channels):
    while True:
        try:
            async with websockets.connect(url) as ws:
                # login
                timestamp = str(get_local_timestamp())
                login_str = login_params(timestamp, api_key, passphrase, secret_key)
                await ws.send(login_str)
                # print(f"send: {login_str}")
                res = await ws.recv()
                print(res)

                # subscribe
                sub_param = {"op": "subscribe", "args": channels}
                sub_str = json.dumps(sub_param)
                await ws.send(sub_str)
                print(f"send: {sub_str}")

                while True:
                    try:
                        res = await asyncio.wait_for(ws.recv(), timeout=25)
                    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed) as e:
                        try:
                            await ws.send('ping')
                            res = await ws.recv()
                            print(res)
                            continue
                        except Exception as e:
                            print("连接关闭，正在重连……")
                            break

                    print(get_timestamp() + res)

        except Exception as e:
            print("连接断开，正在重连……")
            continue


# trade
async def trade(url, api_key, passphrase, secret_key, trade_param):
    while True:
        try:
            async with websockets.connect(url) as ws:
                # login
                timestamp = str(get_local_timestamp())
                login_str = login_params(timestamp, api_key, passphrase, secret_key)
                await ws.send(login_str)
                # print(f"send: {login_str}")
                res = await ws.recv()
                print(res)

                # trade
                sub_str = json.dumps(trade_param)
                await ws.send(sub_str)
                print(f"send: {sub_str}")

                while True:
                    try:
                        res = await asyncio.wait_for(ws.recv(), timeout=25)
                    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed) as e:
                        try:
                            await ws.send('ping')
                            res = await ws.recv()
                            print(res)
                            continue
                        except Exception as e:
                            print("连接关闭，正在重连……")
                            break

                    print(get_timestamp() + res)

        except Exception as e:
            print("连接断开，正在重连……")
            continue


# unsubscribe channels
async def unsubscribe(url, api_key, passphrase, secret_key, channels):
    async with websockets.connect(url) as ws:
        # login
        timestamp = str(get_local_timestamp())
        login_str = login_params(timestamp, api_key, passphrase, secret_key)
        await ws.send(login_str)
        # print(f"send: {login_str}")

        res = await ws.recv()
        print(f"recv: {res}")

        # unsubscribe
        sub_param = {"op": "unsubscribe", "args": channels}
        sub_str = json.dumps(sub_param)
        await ws.send(sub_str)
        print(f"send: {sub_str}")

        res = await ws.recv()
        print(f"recv: {res}")


# unsubscribe channels
async def unsubscribe_without_login(url, channels):
    async with websockets.connect(url) as ws:
        # unsubscribe
        sub_param = {"op": "unsubscribe", "args": channels}
        sub_str = json.dumps(sub_param)
        await ws.send(sub_str)
        print(f"send: {sub_str}")

        res = await ws.recv()
        print(f"recv: {res}")


api_key = "data:image/webp;base64,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"
secret_key = ""
passphrase = ""


# WebSocket公共频道 public channels
# 实盘 real trading
# url = "wss://ws.okex.com:8443/ws/v5/public"
# 模拟盘 demo trading
# url = "wss://ws.okex.com:8443/ws/v5/public?brokerId=9999"

# WebSocket私有频道 private channels
# 实盘 real trading
# url = "wss://ws.okex.com:8443/ws/v5/private"
# 模拟盘 demo trading
# url = "wss://ws.okex.com:8443/ws/v5/private?brokerId=9999"

'''
公共频道 public channel
:param channel: 频道名
:param instType: 产品类型
:param instId: 产品ID
:param uly: 合约标的指数

'''

# 产品频道
# channels = [{"channel": "instruments", "instType": "FUTURES"}]
# 行情频道 tickers channel
# channels = [{"channel": "tickers", "instId": "BTC-USD-210326"}]
# 持仓总量频道 
# channels = [{"channel": "open-interest", "instId": "BTC-USD-210326"}]
# K线频道
# channels = [{"channel": "candle1m", "instId": "BTC-USD-210326"}]
# 交易频道
# channels = [{"channel": "trades", "instId": "BTC-USD-201225"}]
# 预估交割/行权价格频道
# channels = [{"channel": "estimated-price", "instType": "FUTURES", "uly": "BTC-USD"}]
# 标记价格频道
# channels = [{"channel": "mark-price", "instId": "BTC-USDT-210326"}]
# 标记价格K线频道
# channels = [{"channel": "mark-price-candle1D", "instId": "BTC-USD-201225"}]
# 限价频道
# channels = [{"channel": "price-limit", "instId": "BTC-USD-201225"}]
# 深度频道
# channels = [{"channel": "books", "instId": "BTC-USD-SWAP"}]
# 期权定价频道
# channels = [{"channel": "opt-summary", "uly": "BTC-USD"}]
# 资金费率频道
# channels = [{"channel": "funding-rate", "instId": "BTC-USD-SWAP"}]
# 指数K线频道
# channels = [{"channel": "index-candle1m", "instId": "BTC-USDT"}]
# 指数行情频道
# channels = [{"channel": "index-tickers", "instId": "BTC-USDT"}]
# status频道
# channels = [{"channel": "status"}]

'''
私有频道 private channel
:param channel: 频道名
:param ccy: 币种
:param instType: 产品类型
:param uly: 合约标的指数
:param instId: 产品ID

'''

# 账户频道
# channels = [{"channel": "account", "ccy": "BTC"}]
# 持仓频道
# channels = [{"channel": "positions", "instType": "FUTURES", "uly": "BTC-USDT", "instId": "BTC-USDT-210326"}]
# 订单频道
# channels = [{"channel": "orders", "instType": "FUTURES", "uly": "BTC-USD", "instId": "BTC-USD-201225"}]
# 策略委托订单频道
# channels = [{"channel": "orders-algo", "instType": "FUTURES", "uly": "BTC-USD", "instId": "BTC-USD-201225"}]

'''
交易 trade
'''

# 下单
# trade_param = {"id": "1512", "op": "order", "args": [{"side": "buy", "instId": "BTC-USDT", "tdMode": "isolated", "ordType": "limit", "px": "19777", "sz": "1"}]}
# 批量下单
# trade_param = {"id": "1512", "op": "batch-orders", "args": [
#         {"side": "buy", "instId": "BTC-USDT", "tdMode": "isolated", "ordType": "limit", "px": "19666", "sz": "1"},
#         {"side": "buy", "instId": "BTC-USDT", "tdMode": "isolated", "ordType": "limit", "px": "19633", "sz": "1"}
#     ]}
# 撤单
# trade_param = {"id": "1512", "op": "cancel-order", "args": [{"instId": "BTC-USDT", "ordId": "259424589042823169"}]}
# 批量撤单
# trade_param = {"id": "1512", "op": "batch-cancel-orders", "args": [
#         {"instId": "BTC-USDT", "ordId": "259432098826694656"},
#         {"instId": "BTC-USDT", "ordId": "259432098826694658"}
#     ]}
# 改单
# trade_param = {"id": "1512", "op": "amend-order", "args": [{"instId": "BTC-USDT", "ordId": "259432767558135808", "newSz": "2"}]}
# 批量改单
# trade_param = {"id": "1512", "op": "batch-amend-orders", "args": [
#         {"instId": "BTC-USDT", "ordId": "259435442492289024", "newSz": "2"},
#         {"instId": "BTC-USDT", "ordId": "259435442496483328", "newSz": "3"}
#     ]}


loop = asyncio.get_event_loop()

# 公共频道 不需要登录（行情，持仓总量，K线，标记价格，深度，资金费率等）
# loop.run_until_complete(subscribe_without_login(url, channels))

# 私有频道 需要登录（账户，持仓，订单等）
# loop.run_until_complete(subscribe(url, api_key, passphrase, secret_key, channels))

# 交易（下单，撤单，改单等）
# loop.run_until_complete(trade(url, api_key, passphrase, secret_key, trade_param))

loop.close()
