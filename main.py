import uvicorn

from fastapi import FastAPI
from pydantic import BaseModel  # FastAPI 定义请求体，需要 Pydantic 模型。需要从pydantic中导入BaseModel。
from okex_spot import buy


# 创建数据类型然后，声明数据模型为一个类，且该类继承 BaseModel。
class Item(BaseModel):
    side: str
    amount: str


app = FastAPI()



@app.post("/buy")
def buy_(item: Item):
    result = buy(side=item.side, amount=item.amount)
    return result


if __name__ == '__main__':
    uvicorn.run(app='main:app', host="0.0.0.0", port=80, reload=True, debug=True)