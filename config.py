import os
from dotenv import load_dotenv

# 加载环境变量（override=True 强制覆盖已存在的环境变量）
load_dotenv(override=True)

# AI API配置（支持DeepSeek、字节方舟等OpenAI兼容API）
# 优先使用 ARK_API_KEY（字节方舟），如果没有则使用 DEEPSEEK_API_KEY
DEEPSEEK_API_KEY = os.getenv("ARK_API_KEY", os.getenv("DEEPSEEK_API_KEY", ""))
DEEPSEEK_BASE_URL = os.getenv("ARK_BASE_URL", os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1"))

# 其他配置
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", "")

# 股票数据源配置
DEFAULT_PERIOD = "1y"  # 默认获取1年数据
DEFAULT_INTERVAL = "1d"  # 默认日线数据

# MiniQMT量化交易配置
MINIQMT_CONFIG = {
    'enabled': os.getenv("MINIQMT_ENABLED", "false").lower() == "true",
    'account_id': os.getenv("MINIQMT_ACCOUNT_ID", ""),
    'host': os.getenv("MINIQMT_HOST", "127.0.0.1"),
    'port': int(os.getenv("MINIQMT_PORT", "58610")),
}

# TDX股票数据API配置项目地址github.com/oficcejo/tdx-api
TDX_CONFIG = {
    'enabled': os.getenv("TDX_ENABLED", "false").lower() == "true",
    'base_url': os.getenv("TDX_BASE_URL", "http://192.168.1.222:8181"),
}