import base64
import csv
import hashlib
import hmac
from collections import defaultdict
from copy import copy
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from threading import Lock
from time import sleep, time
from typing import Any, Dict, List, Union
from urllib.parse import quote, urlencode

from vnpy.api.rest import Request, RestClient
from vnpy.api.websocket import WebsocketClient
from vnpy.event import Event
from vnpy.event.engine import EventEngine
from vnpy.trader.constant import Direction, Exchange, Interval, Offset, Status
from vnpy.trader.database import database_manager
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    AccountData,
    BarData,
    CancelRequest,
    ContractData,
    HistoryRequest,
    OrderData,
    OrderRequest,
    OrderType,
    PositionData,
    Product,
    SubscribeRequest,
    TickData,
    TradeData,
)
from vnpy.trader.setting import bingx_account  # 导入账户字典
from vnpy.trader.utility import (
    ACTIVE_STATUSES,
    TZ_INFO,
    GetFilePath,
    delete_dr_data,
    extract_vt_symbol,
    get_folder_path,
    get_local_datetime,
    get_uuid,
    load_json,
    remain_alpha,
    remain_digit,
    save_json,
    is_target_contract
)

# REST API地址
REST_HOST: str = "https://open-api.bingx.com"
# u本位永续Websocket API地址
USDT_WEBSOCKET_HOST: str = "wss://open-api-swap.bingx.com/swap-market"

# 委托类型映射
ORDERTYPE_VT2BINGX = {OrderType.LIMIT: "LIMIT", OrderType.MARKET: "MARKET"}

ORDERTYPE_BINGX2VT = {v: k for k, v in ORDERTYPE_VT2BINGX.items()}

DIRECTION_OFFSET_VT2BINGX = {
    (Direction.LONG, Offset.OPEN): ("LONG", "BUY"),
    (Direction.SHORT, Offset.CLOSE): ("LONG", "SELL"),
    (Direction.SHORT, Offset.OPEN): ("SHORT", "SELL"),
    (Direction.LONG, Offset.CLOSE): ("SHORT", "BUY"),
}
DIRECTION_OFFSET_BINGX2VT = {v: k for k, v in DIRECTION_OFFSET_VT2BINGX.items()}

DIRECTION_BINGX2VT = {"LONG": Direction.LONG, "SHORT": Direction.SHORT}

STATUS_BINGX2VT = {
    "NEW": Status.NOTTRADED,
    "PENDING": Status.NOTTRADED,
    "FILLED": Status.ALLTRADED,
    "PARTIALLY_FILLED": Status.PARTTRADED,
    "EXPIRED": Status.REJECTED,
    "CANCELLED": Status.CANCELLED,
    "CANCELED": Status.CANCELLED,
}
# 多空反向映射
OPPOSITE_DIRECTION = {
    Direction.LONG: Direction.SHORT,
    Direction.SHORT: Direction.LONG,
}


# 鉴权类型
class Security(Enum):
    NONE: int = 0
    SIGNED: int = 1


# ----------------------------------------------------------------------------------------------------
class BingxsGateway(BaseGateway):
    """
    vn.py用于对接bingx usdt永续的交易接口
    """

    default_setting: Dict[str, Any] = {
        "key": "",
        "secret": "",
        "host": "",
        "port": 0,
    }

    exchanges: Exchange = [Exchange.BINGXS]
    # ----------------------------------------------------------------------------------------------------
    def __init__(self, event_engine: EventEngine, gateway_name: str = "BINGXS") -> None:
        """
        构造函数
        """
        super().__init__(event_engine, gateway_name)

        self.usdt_ws_api: "BingxsWebsocketApi" = BingxsWebsocketApi(self)
        self.rest_api: "BingxsRestApi" = BingxsRestApi(self)
        self.orders: Dict[str, OrderData] = {}
        # 所有合约列表
        self.recording_list = GetFilePath.recording_list
        self.recording_list = [vt_symbol for vt_symbol in self.recording_list if is_target_contract(vt_symbol, self.gateway_name)]
        # 查询历史数据合约列表
        self.history_contracts = copy(self.recording_list)
        # 查询行情合约列表
        self.query_contracts = copy(self.recording_list)
        self.query_functions = [self.query_account, self.query_order, self.query_position]
        self.count = 0
        # 下载历史数据状态
        self.history_status: bool = True
    # ----------------------------------------------------------------------------------------------------
    def connect(self, log_account: dict = {}) -> None:
        """
        连接交易接口
        """
        if not log_account:
            log_account = bingx_account
        key: str = log_account["key"]
        secret: str = log_account["secret"]
        proxy_host: str = log_account["host"]
        proxy_port: int = log_account["port"]
        self.account_file_name = log_account["account_file_name"]
        self.rest_api.connect(key, secret, proxy_host, proxy_port)
        self.init_query()
        # 获取ws连接令牌
        listen_key = self.rest_api.listen_key
        while not listen_key:
            self.rest_api.get_listen_key()
            listen_key = self.rest_api.listen_key
            sleep(1)

        usdt_ws_host = f"{USDT_WEBSOCKET_HOST}?listenKey={listen_key}"
        self.usdt_ws_api.connect(usdt_ws_host,key, secret, proxy_host, proxy_port)
    # ----------------------------------------------------------------------------------------------------
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅行情
        """
        self.usdt_ws_api.subscribe(req)
    # ----------------------------------------------------------------------------------------------------
    def send_order(self, req: OrderRequest) -> str:
        """
        委托下单
        """
        return self.rest_api.send_order(req)
    # ----------------------------------------------------------------------------------------------------
    def cancel_order(self, req: CancelRequest) -> None:
        """
        委托撤单
        """
        self.rest_api.cancel_order(req)
    # ----------------------------------------------------------------------------------------------------
    def query_account(self) -> None:
        """
        查询资金
        """
        self.rest_api.query_account()
    # ----------------------------------------------------------------------------------------------------
    def query_position(self) -> None:
        """
        查询持仓
        """
        self.rest_api.query_position()
    # ----------------------------------------------------------------------------------------------------
    def query_order(self) -> None:
        """
        查询活动委托单
        """
        self.rest_api.query_order()
    # ----------------------------------------------------------------------------------------------------
    def on_order(self, order: OrderData) -> None:
        """
        推送委托数据
        """
        self.orders[order.orderid] = copy(order)
        super().on_order(order)
    # ----------------------------------------------------------------------------------------------------
    def get_order(self, orderid: str) -> OrderData:
        """
        查询委托数据
        """
        return self.orders.get(orderid, None)
    # ----------------------------------------------------------------------------------------------------
    def query_history(self, event: Event):
        """
        查询合约历史数据
        """
        if len(self.history_contracts) > 0:
            symbol, exchange, gateway_name = extract_vt_symbol(self.history_contracts.pop(0))
            req = HistoryRequest(
                symbol=symbol,
                exchange=exchange,
                interval=Interval.MINUTE,
                start=datetime.now(TZ_INFO) - timedelta(minutes=1440),
                end=datetime.now(TZ_INFO),
                gateway_name=self.gateway_name,
            )
            self.rest_api.query_history(req)
            self.rest_api.set_leverage(symbol)
    # ----------------------------------------------------------------------------------------------------
    def process_timer_event(self, event) -> None:
        """
        处理定时事件
        """
        # 一次查询30个tick行情
        query_contracts = self.query_contracts[:30]
        remain_contracts = self.query_contracts[30:]
        for vt_symbol in query_contracts:
            symbol, *_ = extract_vt_symbol(vt_symbol)
            self.rest_api.query_tick(symbol)
        remain_contracts.extend(query_contracts)
        self.query_contracts = remain_contracts

        function = self.query_functions.pop(0)
        function()
        self.query_functions.append(function)
        # 从委托单映射字典中删除非活动委托单用户id和交易所id
        orderids = list(self.rest_api.orderid_systemid_map)
        if len(orderids) > 50:
            orderid = orderids[0]
            order = self.get_order(orderid)
            if not order or order.status not in ACTIVE_STATUSES:
                systemid = self.rest_api.orderid_systemid_map.pop(orderid)
                if systemid in self.rest_api.systemid_orderid_map:
                    self.rest_api.systemid_orderid_map.pop(systemid)

        # 每隔30分钟发送一次延长listenkey请求
        self.count += 1
        if self.count < 1800:
            return
        self.count = 0
        self.rest_api.keep_listen_key()
    # ----------------------------------------------------------------------------------------------------
    def init_query(self):
        """
        定时任务
        """
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        if self.history_status:
            self.event_engine.register(EVENT_TIMER, self.query_history)
    # ----------------------------------------------------------------------------------------------------
    def close(self) -> None:
        """
        关闭连接
        """
        self.rest_api.stop()
        self.usdt_ws_api.stop()
# ----------------------------------------------------------------------------------------------------
class BingxsRestApi(RestClient):
    """
    BINGX交易所REST API
    """
    # ----------------------------------------------------------------------------------------------------
    def __init__(self, gateway: BingxsGateway) -> None:
        """
        构造函数
        """
        super().__init__()

        self.gateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.usdt_ws_api: BingxsWebsocketApi = self.gateway.usdt_ws_api

        # 保存用户登陆信息
        self.key: str = ""
        self.secret: str = ""
        # 确保生成的orderid不发生冲突
        self.order_count: int = 0
        self.order_count_lock: Lock = Lock()
        self.connect_time: int = 0
        self.account_date = None  # 账户日期
        self.accounts_info: Dict[str, dict] = {}
        # websocket令牌
        self.listen_key = ""
        # 用户自定义orderid与交易所orderid映射
        self.orderid_systemid_map: Dict[str, str] = defaultdict(str)
        self.systemid_orderid_map: Dict[str, str] = defaultdict(str)
        # 成交委托单
        self.trade_id = 0
    # ----------------------------------------------------------------------------------------------------
    def sign(self, request: Request) -> Request:
        """
        生成BINGX签名
        """
        # 获取鉴权类型并将其从data中删除
        security = request.data.pop("security", None)
        if security == Security.NONE:
            request.data = None
            return request

        sorted_data = request.params or request.data or {}
        sorted_data.update({
            "timestamp": int(time() * 1000),
            "recvWindow": 5000
        })

        params_str = "&".join(f"{k}={sorted_data[k]}" for k in sorted(sorted_data))
        signature = get_sign(self.secret, params_str)
        request.path = f"{request.path}?{params_str}&signature={signature}"
        
        request.data = {}
        request.params = {}
        request.headers = request.headers or {}
        request.headers["X-BX-APIKEY"] = self.key

        return request
    # ----------------------------------------------------------------------------------------------------
    def connect(
        self,
        key: str,
        secret: str,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """
        连接REST服务器
        """
        self.key = key
        self.secret = secret.encode()
        self.connect_time = int(datetime.now().strftime("%Y%m%d%H%M%S"))
        self.init(REST_HOST, proxy_host, proxy_port, gateway_name=self.gateway_name)
        self.start()
        self.gateway.write_log(f"交易接口：{self.gateway_name}，REST API启动成功")
        self.query_contract()
        self.set_positionside()
    # ----------------------------------------------------------------------------------------------------
    def set_leverage(self, symbol: str):
        """
        设置杠杆
        """
        sides = ["LONG", "SHORT"]
        path: str = "/openApi/swap/v2/trade/leverage"
        for side in sides:
            data: dict = {
                "security": Security.SIGNED,
                "symbol": symbol,
                "side": side,
                "leverage": 20,
            }
            self.add_request(
                method="POST",
                path=path,
                callback=self.on_leverage,
                data=data,
            )
    # ----------------------------------------------------------------------------------------------------
    def on_leverage(self, data: dict, request: dict):
        pass
    # ----------------------------------------------------------------------------------------------------
    def set_positionside(self):
        """
        设置持仓模式(双向持仓)
        """
        data: dict = {
            "security": Security.SIGNED,
            "dualSidePosition": "true",     # true双向持仓，false单向持仓
        }
        self.add_request(
            method="POST",
            path="/openApi/swap/v1/positionSide/dual",
            callback=self.on_positionside,
            data=data,
        )
    # ----------------------------------------------------------------------------------------------------
    def on_positionside(self,data: dict, request: dict):
        pass
    # ----------------------------------------------------------------------------------------------------
    def get_listen_key(self):
        """
        获取websocket私有令牌
        """
        data: dict = {"security": Security.SIGNED}
        path: str = "/openApi/user/auth/userDataStream"
        self.add_request(
            method="POST",
            path=path,
            callback=self.on_listen_key,
            data=data,
        )
    # ----------------------------------------------------------------------------------------------------
    def on_listen_key(self, data: dict, request: Request):
        """
        收到listen_key回报
        """
        self.listen_key: str = data["listenKey"]
    # ----------------------------------------------------------------------------------------------------
    def keep_listen_key(self):
        """
        延长websocket私有令牌
        """
        if not self.listen_key:
            return
        data: dict = {"security": Security.SIGNED, "listenKey": self.listen_key}
        path: str = "/openApi/user/auth/userDataStream"
        self.add_request(
            method="POST",
            path=path,
            callback=self.on_keep_listen_key,
            data=data,
        )
    # ----------------------------------------------------------------------------------------------------
    def on_keep_listen_key(self, data: dict, request: Request):
        """
        收到listen_key回报
        """
        pass
    # ----------------------------------------------------------------------------------------------------
    def query_tick(self, symbol: str):
        """
        查询24小时tick变动
        """
        data: dict = {
            "security": Security.NONE,
        }
        params = {"symbol": symbol}
        path: str = "/openApi/swap/v2/quote/ticker"
        self.add_request(method="GET", path=path, callback=self.on_tick, data=data, params=params)
    # ----------------------------------------------------------------------------------------------------
    def on_tick(self, data: dict, request: Request) -> None:
        data = data["data"]
        symbol = data["symbol"]

        tick = self.usdt_ws_api.ticks.get(symbol, None)
            
        if not tick:
            tick = TickData(
                symbol=symbol,
                exchange=Exchange.BINGXS,
                gateway_name=self.gateway_name,
            )
        tick.open_price = float(data["openPrice"])
        tick.high_price = float(data["highPrice"])
        tick.low_price = float(data["lowPrice"])
        tick.volume = float(data["volume"])
    # ----------------------------------------------------------------------------------------------------
    def query_account(self) -> None:
        """
        查询资金
        """
        data: dict = {"security": Security.SIGNED}
        path: str = "/openApi/swap/v2/user/balance"
        self.add_request(method="GET", path=path, callback=self.on_query_account, data=data)
    # ----------------------------------------------------------------------------------------------------
    def query_position(self) -> None:
        """
        查询持仓
        """
        data: dict = {"security": Security.SIGNED}
        path: str = "/openApi/swap/v2/user/positions"
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_position,
            data=data,
        )
    # ----------------------------------------------------------------------------------------------------
    def query_order(self) -> None:
        """
        查询活动委托单
        """
        data: dict = {"security": Security.SIGNED}
        path: str = "/openApi/swap/v2/trade/openOrders"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_order,
            data=data,
        )
    # ----------------------------------------------------------------------------------------------------
    def query_contract(self) -> None:
        """
        查询合约信息
        """
        self.add_request(method="GET", path="/openApi/swap/v2/quote/contracts", callback=self.on_query_contract, data={"security": Security.NONE})
    # ----------------------------------------------------------------------------------------------------
    def get_traded(self, symbol: str, system_id: str):
        """
        通过系统委托单号查询委托单
        """
        data: dict = {
            "security": Security.SIGNED,
        }
        params = {"symbol": symbol, "orderId": int(system_id)}
        path: str = "/openApi/swap/v2/trade/order"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_traded,
            data=data,
            params=params,
        )
    # ----------------------------------------------------------------------------------------------------
    def on_traded(self, data: dict, request: Request) -> None:
        """
        收到委托单推送
        """
        data = data["data"]["order"]
        system_id = data["orderId"]
        order_id = self.systemid_orderid_map[data["orderId"]]
        direction, offset = DIRECTION_OFFSET_BINGX2VT[(data["positionSide"], data["side"])]
        order: OrderData = OrderData(
            orderid=order_id,
            symbol=data["symbol"],
            exchange=Exchange.BINGXS,
            price=float(data["price"]),
            volume=float(data["origQty"]),
            traded=float(data["executedQty"]),
            direction=direction,
            offset=offset,
            type=ORDERTYPE_BINGX2VT[data["type"]],
            status=STATUS_BINGX2VT[data["status"]],
            datetime=get_local_datetime(data["time"]),
            gateway_name=self.gateway_name,
        )

        self.gateway.on_order(order)
        if order.status not in ACTIVE_STATUSES:
            if order_id in self.orderid_systemid_map:
                self.orderid_systemid_map.pop(order_id)
            if system_id in self.systemid_orderid_map:
                self.systemid_orderid_map.pop(system_id)

        if order.traded:
            self.trade_id += 1
            trade: TradeData = TradeData(
                symbol=order.symbol,
                exchange=Exchange.BINGXS,
                orderid=order.orderid,
                tradeid=self.trade_id,
                direction=order.direction,
                offset=order.offset,
                price=order.price,
                volume=order.traded,
                datetime=get_local_datetime(data["time"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_trade(trade)
    # ----------------------------------------------------------------------------------------------------
    def _new_order_id(self) -> int:
        """
        生成本地委托号
        """
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count
    # ----------------------------------------------------------------------------------------------------
    def send_order(self, req: OrderRequest) -> str:
        """
        委托下单
        """
        # 生成本地委托号
        orderid: str = req.symbol + "-" + str(self.connect_time + self._new_order_id())

        # 推送提交中事件
        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)
        direction, offset = DIRECTION_OFFSET_VT2BINGX[(req.direction, req.offset)]
        data: dict = {
            "security": Security.SIGNED,
            "symbol": req.symbol,
            "side": offset,
            "positionSide": direction,
            "price": float(req.price),
            "quantity": float(req.volume),
            "type": ORDERTYPE_VT2BINGX[req.type],
        }
        self.add_request(
            method="POST",
            path="/openApi/swap/v2/trade/order",
            callback=self.on_send_order,
            data=data,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed,
        )
        return order.vt_orderid
    # ----------------------------------------------------------------------------------------------------
    def cancel_order(self, req: CancelRequest) -> None:
        """
        委托撤单
        必须用api生成的订单编号撤单
        """
        systemid = self.orderid_systemid_map[req.orderid]
        if not systemid:
            systemid = req.orderid
        data: dict = {
            "security": Security.SIGNED,
            "symbol": req.symbol,
            "orderId": systemid,
        }
        path: str = "/openApi/swap/v2/trade/order"
        order: OrderData = self.gateway.get_order(req.orderid)
        self.add_request(method="DELETE", path=path, callback=self.on_cancel_order, data=data, on_failed=self.on_cancel_failed, extra=order)
    # ----------------------------------------------------------------------------------------------------
    def on_query_account(self, data: dict, request: Request) -> None:
        """
        资金查询回报
        """
        asset = data["data"]["balance"]
        account: AccountData = AccountData(
            accountid=asset["asset"] + "_" + self.gateway_name,
            balance=float(asset["balance"]),
            available=float(asset["availableMargin"]),
            position_profit=float(asset["unrealizedProfit"]),
            close_profit=float(asset["realisedProfit"]),
            datetime=datetime.now(TZ_INFO),
            file_name=self.gateway.account_file_name,
            gateway_name=self.gateway_name,
        )
        account.frozen = account.balance - account.available
        if account.balance:
            self.gateway.on_account(account)
            # 保存账户资金信息
            self.accounts_info[account.accountid] = account.__dict__

        if not self.accounts_info:
            return
        accounts_info = list(self.accounts_info.values())
        account_date = accounts_info[-1]["datetime"].date()
        account_path = str(GetFilePath.ctp_account_path).replace("ctp_account_main", self.gateway.account_file_name)
        write_header = not Path(account_path).exists()
        additional_writing = self.account_date and self.account_date != account_date
        self.account_date = account_date
        # 文件不存在则写入文件头，否则只在日期变更后追加写入文件
        if not write_header and not additional_writing:
            return
        write_mode = "w" if write_header else "a"
        for account_data in accounts_info:
            with open(account_path, write_mode, newline="") as f1:
                w1 = csv.DictWriter(f1, list(account_data))
                if write_header:
                    w1.writeheader()
                w1.writerow(account_data)
    # ----------------------------------------------------------------------------------------------------
    def on_query_position(self, data: dict, request: Request) -> None:
        """
        持仓查询回报
        """
        for raw in data["data"]:
            position: PositionData = PositionData(
                symbol=raw["symbol"],
                exchange=Exchange.BINGXS,
                direction=DIRECTION_BINGX2VT[raw["positionSide"]],
                volume=float(raw["positionAmt"]),
                price=float(raw["avgPrice"]),
                pnl=float(raw["unrealizedProfit"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_position(position)
    # ----------------------------------------------------------------------------------------------------
    def on_query_order(self, data: dict, request: Request) -> None:
        """
        活动委托查询回报
        """
        for raw in data["data"]["orders"]:
            volume = float(raw["origQty"])
            traded = float(raw["executedQty"])
            order_id = self.systemid_orderid_map[raw["orderId"]]
            #rest api可能把非活动委托单识别为活动委托单，需要调用ws的委托单数据过滤
            old_order = self.gateway.get_order(order_id)
            if old_order and old_order.status not in ACTIVE_STATUSES:
                continue
            # 获取不到用户id，使用交易所id撤销该委托单
            if not order_id:
                self.cancel_order(
                    req = CancelRequest(
                        gateway_name = self.gateway_name,
                        orderid = raw["orderId"],
                        symbol = raw["symbol"],
                        exchange=Exchange.BINGXS
                    )
                )
                continue
            direction, offset = DIRECTION_OFFSET_BINGX2VT[(raw["positionSide"], raw["side"])]
            order: OrderData = OrderData(
                orderid=order_id,
                symbol=raw["symbol"],
                exchange=Exchange.BINGXS,
                price=float(raw["price"]),
                volume=volume,
                type=ORDERTYPE_BINGX2VT[raw["type"]],
                direction=direction,
                offset=offset,
                traded=traded,
                status=STATUS_BINGX2VT[raw["status"]],
                datetime=get_local_datetime(raw["time"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_order(order)
    # ----------------------------------------------------------------------------------------------------
    def on_query_contract(self, data: dict, request: Request):
        """
        合约信息查询回报
        """
        for raw in data["data"]:
            contract: ContractData = ContractData(
                symbol=raw["symbol"],
                exchange=Exchange.BINGXS,
                name=raw["symbol"],
                price_tick=float("1e-{}".format(raw["pricePrecision"])),
                size=20,
                min_volume=float(raw["tradeMinQuantity"]),      # 最小币的委托量
                open_commission_ratio=raw["feeRate"],
                product=Product.FUTURES,
                gateway_name=self.gateway_name,
            )
            self.gateway.on_contract(contract)
        self.gateway.write_log(f"交易接口：{self.gateway_name}，合约信息查询成功")
    # ----------------------------------------------------------------------------------------------------
    def on_send_order(self, data: dict, request: Request) -> None:
        """
        委托下单回报
        """
        if not data["data"]:
            msg = data["msg"]
            self.gateway.write_log(f"交易接口，发送委托单出错，错误信息：{msg}")
            return
        order = request.extra
        system_id = data["data"]["order"]["orderId"]
        self.orderid_systemid_map[order.orderid] = system_id
        self.systemid_orderid_map[system_id] = order.orderid
    # ----------------------------------------------------------------------------------------------------
    def on_send_order_error(self, exception_type: type, exception_value: Exception, tb, request: Request) -> None:
        """
        委托下单回报函数报错回报
        """
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)
    # ----------------------------------------------------------------------------------------------------
    def on_send_order_failed(self, status_code: str, request: Request) -> None:
        """
        委托下单失败服务器报错回报
        """
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)
        msg: str = "委托失败，状态码：{0}，信息：{1}".format(status_code, request.response.text)
        self.gateway.write_log(msg)
    # ----------------------------------------------------------------------------------------------------
    def on_cancel_order(self, status_code: str, request: Request) -> None:
        """
        委托撤单回报
        """
        data = request.response.json()
        code = data["code"]
        msg = data["msg"]
        order = request.extra
        # 正常撤单
        if not code:
            if order:
                order.status = Status.CANCELLED
                self.gateway.on_order(order)
        else:
            if order:
                order.status = Status.REJECTED
                self.gateway.on_order(order)
                msg = f"撤单失败，状态码：{code}，信息：{msg}"
                self.gateway.write_log(msg)
    # ----------------------------------------------------------------------------------------------------
    def on_cancel_failed(self, status_code: str, request: Request):
        """
        撤单回报函数报错回报
        """
        if request.extra:
            order = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)
        msg = f"撤单失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)
    # ----------------------------------------------------------------------------------------------------
    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """
        查询历史数据
        """
        history = []
        time_consuming_start = time()
        start_time = req.start
        end_time = start_time + timedelta(minutes=500)
        while start_time < req.end:

            # 创建查询参数
            params = {"symbol": req.symbol, "interval": "1m", "startTime": int(datetime.timestamp(start_time) * 1000), "endTime": int(datetime.timestamp(end_time) * 1000)}

            resp = self.request("GET", "/openApi/swap/v3/quote/klines", data={"security": Security.NONE}, params=params)
            # 如果请求失败则终止循环
            if not resp:
                msg = f"标的：{req.vt_symbol}获取历史数据失败"
                self.gateway.write_log(msg)
                break
            elif resp.status_code // 100 != 2:
                msg = f"标的：{req.vt_symbol}获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data = resp.json()
                if not data:
                    delete_dr_data(req.symbol, self.gateway_name)
                    msg = f"标的：{req.vt_symbol}获取历史数据为空，开始时间：{req.start}"
                    self.gateway.write_log(msg)
                    break
                buf = []
                for raw_data in data["data"]:
                    bar = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=get_local_datetime(raw_data["time"]),
                        interval=req.interval,
                        volume=float(raw_data["volume"]),
                        open_price=float(raw_data["open"]),
                        high_price=float(raw_data["high"]),
                        low_price=float(raw_data["low"]),
                        close_price=float(raw_data["close"]),
                        gateway_name=self.gateway_name,
                    )
                    buf.append(bar)
                start_time = end_time
                end_time = start_time +  timedelta(minutes = 500)
                history.extend(buf)

        if history:
            history.sort(key=lambda x: x.datetime)
            try:
                database_manager.save_bar_data(history, False)  # 保存数据到数据库
            except Exception as err:
                self.gateway.write_log(f"保存bar到数据库出错：{err}")
                return

            time_consuming_end = time()
            query_time = round(time_consuming_end - time_consuming_start, 3)
            msg = f"载入{req.vt_symbol}:bar数据，开始时间：{history[0].datetime}，结束时间：{history[-1].datetime}，数据量：{len(history)}，耗时:{query_time}秒"
            self.gateway.write_log(msg)
        else:
            self.gateway.write_log(f"未获取到合约：{req.vt_symbol}历史数据")
# ----------------------------------------------------------------------------------------------------
class BingxsWebsocketApi(WebsocketClient):
    """
    BINGX交易所Websocket接口
    """
    # ----------------------------------------------------------------------------------------------------
    def __init__(self, gateway: BingxsGateway) -> None:
        """
        构造函数
        """
        super().__init__()

        self.gateway: BingxsGateway = gateway
        self.gateway_name: str = gateway.gateway_name
        self.ticks: Dict[str, TickData] = {}
        self.subscribed: Dict[str, SubscribeRequest] = {}
        # 成交委托号
        self.trade_id: int = 0
        self.ws_connected: bool = False
        self.ping_count: int = 0
        self.put_data = {"depth5": self.on_depth, "trade": self.on_tick, "ACCOUNT_UPDATE": self.on_position, "ORDER_TRADE_UPDATE": self.on_order}
    # ----------------------------------------------------------------------------------------------------
    def connect(
        self,
        ws_host: str,
        api_key: str,
        api_secret: str,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """
        连接Websocket交易频道
        """
        self.api_key = api_key
        self.api_secret = api_secret

        self.init(ws_host, proxy_host, proxy_port, gateway_name=self.gateway_name)
        self.start()
    # ----------------------------------------------------------------------------------------------------
    def on_connected(self) -> None:
        """
        连接成功回报
        """
        self.ws_connected = True
        self.gateway.write_log(f"交易接口：{self.gateway_name}，Websocket API连接成功")
        for req in list(self.subscribed.values()):
            self.subscribe(req)
    # ----------------------------------------------------------------------------------------------------
    def on_disconnected(self) -> None:
        """
        连接断开回报
        """
        self.ws_connected = False
        self.gateway.write_log(f"交易接口：{self.gateway_name}，Websocket 连接断开")
    # ----------------------------------------------------------------------------------------------------
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅行情
        """
        # 等待ws连接成功后再订阅行情
        while not self.ws_connected:
            sleep(1)
        self.ticks[req.symbol] = TickData(
            symbol=req.symbol,
            name=req.symbol,
            exchange=req.exchange,
            gateway_name=self.gateway_name,
            datetime=datetime.now(TZ_INFO),
        )

        self.subscribed[req.symbol] = req
        # 公共和私有主题
        public_topics = [f"{req.symbol}@depth5", f"{req.symbol}@trade"]
        private_topics = ["ORDER_TRADE_UPDATE", "ACCOUNT_UPDATE"]

        # 订阅公共主题
        for topic in public_topics:
            self.send_subscription(topic)

        # 订阅私有主题
        for topic in private_topics:
            self.send_subscription(topic)
    # ----------------------------------------------------------------------------------------------------
    def send_subscription(self, topic:str):
        packet = {"id": get_uuid(), "reqType": "sub", "dataType": topic}
        self.send_packet(packet)
    # ----------------------------------------------------------------------------------------------------
    def on_packet(self, packet: Union[str, dict]) -> None:
        """
        推送数据回报
        """
        if packet == "Ping":
            self.send_packet("Pong")
            return
        if "dataType" in packet:
            type_ = packet["dataType"]
        elif "e" in packet:
            type_ = packet["e"]
        else:
            return
        if not type_:
            return
        if "@" in type_:
            type_ = type_.split("@")[1]
        channel = self.put_data.get(type_, None)
        if channel:
            channel(packet)
    # ----------------------------------------------------------------------------------------------------
    def on_tick(self, packet: dict):
        """
        收到tick事件回报
        """
        data = packet["data"][0]
        symbol = data["s"]
        tick = self.ticks[symbol]
        tick.last_price = float(data["p"])
        tick.datetime = get_local_datetime(data["T"])
    # ----------------------------------------------------------------------------------------------------
    def on_depth(self, packet: dict):
        """
        收到orderbook事件回报
        """
        symbol = packet["dataType"].split("@")[0]
        tick = self.ticks[symbol]
        data = packet["data"]
        bids = sorted(data["bids"], key=lambda x: x[0], reverse=True)
        asks = sorted(data["asks"], key=lambda x: x[0], reverse=False)
        for n, buf in enumerate(bids):
            tick.__setattr__(f"bid_price_{(n + 1)}", float(buf[0]))
            tick.__setattr__(f"bid_volume_{(n + 1)}", float(buf[1]))
        for n, buf in enumerate(asks):
            tick.__setattr__(f"ask_price_{(n + 1)}", float(buf[0]))
            tick.__setattr__(f"ask_volume_{(n + 1)}", float(buf[1]))
        if tick.last_price:
            self.gateway.on_tick(copy(tick))
    # ----------------------------------------------------------------------------------------------------
    def on_position(self, packet: dict):
        """
        收到仓位事件回报
        """
        data = packet["a"]["P"]
        for pos_data in data:
            position: PositionData = PositionData(
                symbol=pos_data["s"],
                exchange=Exchange.BINGXS,
                direction=DIRECTION_BINGX2VT[pos_data["ps"]],
                volume=float(pos_data["pa"]),
                price=float(pos_data["ep"]),
                pnl=float(pos_data["up"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_position(position)
    # ----------------------------------------------------------------------------------------------------
    def on_order(self, packet: dict):
        """
        * 收到委托事件回报
        """
        data = packet["o"]
        # 用户委托单ID和系统委托单ID映射
        # order_id = data["c"]        # 交易所暂时没有用户自定义委托单id推送
        system_id = data["i"]
        systemid_orderid_map = self.gateway.rest_api.systemid_orderid_map
        order_id = systemid_orderid_map[system_id]
        # 获取不到用户委托单id用交易所委托单id撤单
        if not order_id:
            self.gateway.rest_api.cancel_order(
                req= CancelRequest(
                    gateway_name = self.gateway_name,
                    orderid = system_id,
                    symbol  = data["s"],
                    exchange = Exchange.BINGXS
                )
            )
            return
        direction,offset = DIRECTION_OFFSET_BINGX2VT[(data["ps"], data["S"])]
        order: OrderData = OrderData(
            orderid=order_id,
            symbol=data["s"],
            exchange=Exchange.BINGXS,
            price=float(data["p"]),
            volume=float(data["q"]),
            traded = float(data["z"]),
            direction=direction,
            offset=offset,
            type = ORDERTYPE_BINGX2VT[data["o"]],
            status=STATUS_BINGX2VT[data["X"]],
            datetime=get_local_datetime(packet["E"]),
            gateway_name=self.gateway_name,
        )
        self.gateway.on_order(order)

        if order.traded:
            self.trade_id += 1
            trade: TradeData = TradeData(
                symbol=order.symbol,
                exchange=Exchange.BINGXS,
                orderid=order.orderid,
                tradeid=self.trade_id,
                direction=direction,
                offset=offset,
                price=order.price,
                volume=order.traded,
                datetime=get_local_datetime(packet["E"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_trade(trade)

def get_sign(api_secret, payload):
    signature = hmac.new(api_secret, payload.encode("utf-8"), digestmod=hashlib.sha256).hexdigest()
    return signature
