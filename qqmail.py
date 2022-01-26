import json
from imbox import Imbox


def get_data():
    with Imbox('imap.qq.com','783654214@qq.com',"wolizveywvfabgai",ssl=True) as imbox:
        # imap服务器地址，邮箱，密码，是否支持ssl
        mails = imbox.messages(unread=True)
        # 读取收件箱所有信息
        for uid, messages in mails:
            print(messages.sent_from)
            imbox.mark_seen(uid)
            if messages.sent_from==[{'name': 'TradingView', 'email': 'noreply@tradingview.com'}]:
                data=json.loads(messages.body['plain'][0])
                print(data)
                return data
            else:
                return None

if __name__ == '__main__':
    get_data()