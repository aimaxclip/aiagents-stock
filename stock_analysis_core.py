"""
股票分析核心模块
独立于 Streamlit UI，可被后台线程安全调用
"""

from stock_data import StockDataFetcher
from ai_agents import StockAnalysisAgents
from database import db


def get_stock_data_for_analysis(symbol, period):
    """获取股票数据（不带Streamlit缓存的版本）"""
    fetcher = StockDataFetcher()
    stock_info = fetcher.get_stock_info(symbol)
    stock_data = fetcher.get_stock_data(symbol, period)

    if isinstance(stock_data, dict) and "error" in stock_data:
        return stock_info, None, None

    stock_data_with_indicators = fetcher.calculate_technical_indicators(stock_data)
    indicators = fetcher.get_latest_indicators(stock_data_with_indicators)

    return stock_info, stock_data_with_indicators, indicators


def analyze_single_stock(symbol, period, enabled_analysts_config=None, selected_model='deepseek-chat'):
    """单个股票分析（用于批量分析和后台分析）

    Args:
        symbol: 股票代码
        period: 数据周期
        enabled_analysts_config: 分析师配置字典
        selected_model: 选择的AI模型

    返回分析结果或错误信息
    """
    try:
        # 使用默认配置
        if enabled_analysts_config is None:
            enabled_analysts_config = {
                'technical': True,
                'fundamental': True,
                'fund_flow': True,
                'risk': True,
                'sentiment': False,
                'news': False
            }

        # 1. 获取股票数据
        stock_info, stock_data, indicators = get_stock_data_for_analysis(symbol, period)

        if "error" in stock_info:
            return {"symbol": symbol, "error": stock_info['error'], "success": False}

        if stock_data is None:
            return {"symbol": symbol, "error": "无法获取股票历史数据", "success": False}

        # 2. 获取财务数据
        fetcher = StockDataFetcher()
        financial_data = fetcher.get_financial_data(symbol)

        # 2.5 获取季报数据（仅A股）
        quarterly_data = None
        enable_fundamental = enabled_analysts_config.get('fundamental', True)
        if enable_fundamental and fetcher._is_chinese_stock(symbol):
            try:
                from quarterly_report_data import QuarterlyReportDataFetcher
                quarterly_fetcher = QuarterlyReportDataFetcher()
                quarterly_data = quarterly_fetcher.get_quarterly_reports(symbol)
            except:
                pass

        # 获取分析师选择状态（从参数而不是session_state）
        enable_fund_flow = enabled_analysts_config.get('fund_flow', True)
        enable_sentiment = enabled_analysts_config.get('sentiment', False)
        enable_news = enabled_analysts_config.get('news', False)

        # 3. 获取资金流向数据（akshare数据源，可选）
        fund_flow_data = None
        if enable_fund_flow and fetcher._is_chinese_stock(symbol):
            try:
                from fund_flow_akshare import FundFlowAkshareDataFetcher
                fund_flow_fetcher = FundFlowAkshareDataFetcher()
                fund_flow_data = fund_flow_fetcher.get_fund_flow_data(symbol)
            except:
                pass

        # 4. 获取市场情绪数据（可选）
        sentiment_data = None
        if enable_sentiment and fetcher._is_chinese_stock(symbol):
            try:
                from market_sentiment_data import MarketSentimentDataFetcher
                sentiment_fetcher = MarketSentimentDataFetcher()
                sentiment_data = sentiment_fetcher.get_market_sentiment_data(symbol, stock_data)
            except:
                pass

        # 5. 获取新闻数据（qstock数据源，可选）
        news_data = None
        if enable_news and fetcher._is_chinese_stock(symbol):
            try:
                from qstock_news_data import QStockNewsDataFetcher
                news_fetcher = QStockNewsDataFetcher()
                news_data = news_fetcher.get_stock_news(symbol)
            except:
                pass

        # 5.5 获取风险数据（限售解禁、大股东减持、重要事件，可选）
        risk_data = None
        enable_risk = enabled_analysts_config.get('risk', True)
        if enable_risk and fetcher._is_chinese_stock(symbol):
            try:
                risk_data = fetcher.get_risk_data(symbol)
            except:
                pass

        # 6. 初始化AI分析系统
        agents = StockAnalysisAgents(model=selected_model)

        # 使用传入的分析师配置
        enabled_analysts = enabled_analysts_config

        # 7. 运行多智能体分析
        agents_results = agents.run_multi_agent_analysis(
            stock_info, stock_data, indicators, financial_data,
            fund_flow_data, sentiment_data, news_data, quarterly_data, risk_data,
            enabled_analysts=enabled_analysts_config
        )

        # 8. 团队讨论
        discussion_result = agents.conduct_team_discussion(agents_results, stock_info)

        # 9. 最终决策
        final_decision = agents.make_final_decision(discussion_result, stock_info, indicators)

        # 保存到数据库
        saved_to_db = False
        db_error = None
        try:
            record_id = db.save_analysis(
                symbol=stock_info.get('symbol', ''),
                stock_name=stock_info.get('name', ''),
                period=period,
                stock_info=stock_info,
                agents_results=agents_results,
                discussion_result=discussion_result,
                final_decision=final_decision
            )
            saved_to_db = True
            print(f"✅ {symbol} 成功保存到数据库，记录ID: {record_id}")
        except Exception as e:
            db_error = str(e)
            print(f"❌ {symbol} 保存到数据库失败: {db_error}")

        return {
            "symbol": symbol,
            "success": True,
            "stock_info": stock_info,
            "indicators": indicators,
            "agents_results": agents_results,
            "discussion_result": discussion_result,
            "final_decision": final_decision,
            "saved_to_db": saved_to_db,
            "db_error": db_error
        }

    except Exception as e:
        print(f"❌ {symbol} 分析异常: {e}")
        return {"symbol": symbol, "error": str(e), "success": False}
