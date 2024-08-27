import json
import socket
import ssl
import sys
import traceback
from datetime import datetime
from threading import Lock, Thread
from time import sleep
from typing import Dict, List, Union
import gzip
import websocket

from vnpy.trader.utility import save_connection_status,write_log
# ----------------------------------------------------------------------------------------------------
class WebsocketClient:
    """
    Websocket API

    创建客户端对象后，使用 start() 运行工作线程和 ping 线程。
    工作线程会自动连接 websocket。

    在销毁客户端对象之前，使用 stop 停止线程并断开 websocket 连接（尤其是在退出程序时）。
    对象（尤其是在退出程序时）。

    默认序列化格式为 json。

    重载回调
    * 卸载数据
    * on_connected
    * 断开连接时
    * on_packet
    * on_error

    调用 start() 后，ping 线程将每 20 秒 ping 服务器一次。

    如果要发送 JSON 以外的内容，请覆盖 send_packet。
    """
    # ----------------------------------------------------------------------------------------------------
    def __init__(self):
        self.host :str = ""

        self._ws_lock = Lock()
        self._ws = None

        self._worker_thread :Thread = None
        self._ping_thread :Thread = None
        self._active :bool= False

        self.proxy_host :str = ""
        self.proxy_port :int = 0
        self.ping_interval :int = 20  # 心跳间隔
        self.timeout :int = 60  # 超时时间
        self.header = {}
        self._last_sent_text :str = ""
        self._last_received_text :str = ""
    # ----------------------------------------------------------------------------------------------------
    def init(self,
                host: str,
                proxy_host: str = "",
                proxy_port: int = 0,
                ping_interval: int = 20,
                header: dict = None,
                gateway_name: str = ""
                ):
        """
        * init方法负责设置 WebSocket 客户端的基本配置，包括目标主机、代理服务器信息（如有）、心跳间隔、HTTP 头部以及交易接口名称等参数。
        * 参数：
            host: 目标WebSocket服务器的主机地址（URL），类型为字符串。
            proxy_host: 代理服务器的地址，若使用代理则指定，默认为空字符串。
            proxy_port: 代理服务器的端口号，仅当proxy_host不为空时有效，默认值为0。
            ping_interval: 发送心跳包的时间间隔（单位：秒），用于保持长连接稳定，默认值为20秒。
            header: HTTP 头部字典，可用于自定义HTTP请求头信息。
            gateway_name: 交易接口名称
        """
        self.host = host
        self.ping_interval = ping_interval
        self.gateway_name = gateway_name
        if header:
            self.header = header

        if proxy_host and proxy_port:
            self.proxy_host = proxy_host
            self.proxy_port = proxy_port
        assert self.gateway_name, "请到交易接口WEBSOCKET API connect函数里面的self.init函数中添加gateway_name参数"
    # ----------------------------------------------------------------------------------------------------
    def start(self):
        """
        启动客户端，并在 websocket 函数。
        在调用 on_connected 函数之前，请不要发送数据包。
        """
        self._active = True
        self._worker_thread = Thread(target=self._run)
        self._worker_thread.start()

        self._ping_thread = Thread(target=self._run_ping)
        self._ping_thread.start()
    # ----------------------------------------------------------------------------------------------------
    def stop(self):
        """
        关闭客户端
        """
        self._active = False
        self._disconnect()
    # ----------------------------------------------------------------------------------------------------
    def join(self):
        """
        等待所有线程结束。
        此函数不能从工作线程或回调函数调用。
        """
        self._ping_thread.join()
        self._worker_thread.join()
    # ----------------------------------------------------------------------------------------------------
    def send_packet(self, packet: Union[dict, str]):
        """
        向服务器发送数据包（dict 数据
        如果要发送非 json 数据包，则覆盖此选项
        """
        if packet in ["Pong", "ping"]:
            text = packet
        else:
            text: str = json.dumps(packet)
        self._record_last_sent_text(text)
        return self._send_text(text)
    # ----------------------------------------------------------------------------------------------------
    def _send_text(self, text: str):
        """
        发送字符串到服务器
        """
        ws = self._ws
        if ws:
            ws.send(text, opcode=websocket.ABNF.OPCODE_TEXT)
    # ----------------------------------------------------------------------------------------------------
    def _send_binary(self, data: bytes):
        """
        发送bytes到服务器
        """
        ws = self._ws
        if ws:
            ws._send_binary(data)
    # ----------------------------------------------------------------------------------------------------
    def _create_connection(self, *args, **kwargs):
        """
        创建websocket连接
        """
        return websocket.create_connection(*args, **kwargs)
    # ----------------------------------------------------------------------------------------------------
    def _ensure_connection(self):
        """
        """
        triggered = False
        with self._ws_lock:
            if self._ws is None:
                self._ws = self._create_connection(
                    self.host,
                    sslopt={"cert_reqs": ssl.CERT_NONE},
                    http_proxy_host=self.proxy_host,
                    http_proxy_port=self.proxy_port,
                    header=self.header,
                    timeout = self.timeout
                )
                triggered = True
        if triggered:
            self.on_connected()
    # ----------------------------------------------------------------------------------------------------
    def _disconnect(self):
        """
        """
        triggered = False
        with self._ws_lock:
            if self._ws:
                ws: websocket.WebSocket = self._ws
                self._ws = None

                triggered = True
        if triggered:
            ws.close()
            self.on_disconnected()
    # ----------------------------------------------------------------------------------------------------
    def _run(self):
        """
        保持运行直到stop函数被调用
        """
        try:
            while self._active:
                try:
                    self._ensure_connection()
                    ws = self._ws
                    if ws:
                        text = ws.recv()

                        # 当recv函数阻塞时，ws 对象被关闭
                        if not text:
                            self._disconnect()
                            continue
                        # 解压gzip数据
                        if isinstance(text, bytes):
                            text = gzip.decompress(text).decode("UTF-8")
                        self._record_last_received_text(text)
                        # ping,pong消息不需要解包
                        if text in ["Ping", "pong"]:
                            data = text
                        else:
                            data = self.unpack_data(text)
                        self.on_packet(data)
                # 处理ws.recv函数调用前ws被关闭的错误
                except (
                    websocket.WebSocketConnectionClosedException,
                    websocket.WebSocketBadStatusException,
                    websocket.WebSocketTimeoutException,
                    socket.error
                ):
                    self._disconnect()
                except Exception:
                    self.on_error(*sys.exc_info())
        except Exception:
            self.on_error(*sys.exc_info())
        finally:
            self._disconnect()
    # ----------------------------------------------------------------------------------------------------
    @staticmethod
    def unpack_data(data: str)-> Dict:
        """
        默认序列化格式为 json。
        如果想使用其他序列化格式，请重载此方法。
        """
        return json.loads(data)
    # ----------------------------------------------------------------------------------------------------
    def _run_ping(self):
        """
        发送心跳
        """
        while self._active:
            try:
                self._ping()
            except:
                self.on_error(*sys.exc_info())
                # 等待1秒后重连websocket
                sleep(1)

            for i in range(self.ping_interval):
                if not self._active:
                    break
                sleep(1)
    # ----------------------------------------------------------------------------------------------------
    def _ping(self):
        """"""
        ws = self._ws
        if ws:
            ws.send("ping", websocket.ABNF.OPCODE_PING)
    # ----------------------------------------------------------------------------------------------------
    @staticmethod
    def on_connected():
        """
        连接成功回调
        """
        pass
    # ----------------------------------------------------------------------------------------------------
    @staticmethod
    def on_disconnected():
        """
        连接断开回调
        """
        pass
    # ----------------------------------------------------------------------------------------------------
    @staticmethod
    def on_packet(packet: Union[dict, str]):
        """
        收到数据回调
        """
        pass
    # ----------------------------------------------------------------------------------------------------
    def on_error(self, exception_type: type, exception_value: Exception, tracebacks):
        """
        触发异常回调
        """
        write_log(self.exception_detail(exception_type, exception_value, tracebacks))
        save_connection_status(self.gateway_name, False)
    # ----------------------------------------------------------------------------------------------------
    def exception_detail(self, exception_type: type, exception_value: Exception, tracebacks):
        """
        异常信息格式化
        """
        text = "[{}]: Unhandled WebSocket Error:{}\n".format(datetime.now().isoformat(), exception_type)
        text += "LastSentText:\n{}\n".format(self._last_sent_text)
        text += "LastReceivedText:\n{}\n".format(self._last_received_text)
        text += "Exception trace: \n"
        text += "".join(traceback.format_exception(exception_type, exception_value, tracebacks))
        return text
    # ----------------------------------------------------------------------------------------------------
    def _record_last_sent_text(self, text: str):
        """
        记录最近发出的数据字符串
        """
        self._last_sent_text = text[:1000]
    # ----------------------------------------------------------------------------------------------------
    def _record_last_received_text(self, text: str):
        """
        记录最近收到的数据字符串
        """
        self._last_received_text = text[:1000]
