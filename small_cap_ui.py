#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
小市值策略UI模块
"""

import streamlit as st
import pandas as pd
import time
from datetime import datetime
from small_cap_selector import small_cap_selector
from notification_service import notification_service
from low_price_bull_monitor import low_price_bull_monitor
from low_price_bull_service import low_price_bull_service
from selector_scheduler import selector_scheduler, run_small_cap_selection
from selector_task_db import selector_task_db


def display_selection_history():
    """显示选股历史记录"""
    st.markdown("## 📚 小市值策略选股历史")
    st.markdown("---")

    # 获取历史记录
    tasks = selector_task_db.get_recent_tasks('small_cap', limit=20)

    if not tasks:
        st.info("暂无选股历史记录")
        if st.button("🔙 返回选股", type="primary"):
            del st.session_state.show_small_cap_history
            st.rerun()
        return

    # 显示历史列表
    for task in tasks:
        status_emoji = {
            'completed': '✅',
            'failed': '❌',
            'cancelled': '⚠️',
            'running': '🔄',
            'pending': '⏳'
        }.get(task['status'], '❓')

        task_time = task.get('created_at', '')[:19] if task.get('created_at') else 'N/A'
        result_count = 0
        if task.get('results') and task['results'].get('stocks'):
            result_count = len(task['results']['stocks'])

        with st.expander(f"{status_emoji} {task_time} - {task['status']} ({result_count}只股票)"):
            col1, col2 = st.columns(2)

            with col1:
                st.caption(f"任务ID: {task['task_id'][:8]}...")
                st.caption(f"状态: {task['status']}")
                if task.get('completed_at'):
                    st.caption(f"完成时间: {task['completed_at'][:19]}")

            with col2:
                if task.get('params'):
                    params = task['params']
                    st.caption(f"选股数量: {params.get('top_n', 'N/A')}")

            # 显示结果
            if task['status'] == 'completed' and task.get('results'):
                results = task['results']
                if results.get('stocks'):
                    st.markdown("**选股结果:**")
                    stocks_df = pd.DataFrame(results['stocks'])
                    display_cols = [col for col in ['股票代码', '股票简称', '最新价', '涨跌幅', '总市值'] if col in stocks_df.columns]
                    if display_cols:
                        st.dataframe(stocks_df[display_cols], use_container_width=True, height=200)
                    else:
                        st.dataframe(stocks_df.head(10), use_container_width=True, height=200)

                    if st.button("📥 加载此结果", key=f"load_{task['task_id']}"):
                        st.session_state.small_cap_stocks = stocks_df
                        st.session_state.small_cap_time = task.get('completed_at', '')[:19]
                        del st.session_state.show_small_cap_history
                        st.rerun()

            elif task['status'] == 'failed':
                st.error(f"失败原因: {task.get('error_message', '未知错误')}")

    st.markdown("---")
    if st.button("🔙 返回选股", type="primary"):
        del st.session_state.show_small_cap_history
        st.rerun()


def check_and_display_background_task() -> bool:
    """检查并显示后台任务状态"""
    running_tasks = selector_scheduler.get_running_tasks('small_cap')

    if not running_tasks:
        if 'small_cap_task_id' in st.session_state:
            task = selector_scheduler.get_task_status(st.session_state.small_cap_task_id)
            if task and task['status'] == 'completed':
                st.success("✅ 后台选股任务已完成!")
                if task.get('results') and task['results'].get('success'):
                    stocks_data = task['results'].get('stocks', [])
                    if stocks_data:
                        st.session_state.small_cap_stocks = pd.DataFrame(stocks_data)
                        st.session_state.small_cap_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                del st.session_state.small_cap_task_id
                st.rerun()
            elif task and task['status'] == 'failed':
                st.error(f"❌ 后台选股失败: {task.get('error_message', '未知错误')}")
                del st.session_state.small_cap_task_id
        return False

    task = running_tasks[0]
    st.info("⏳ 后台选股任务运行中...")

    col1, col2 = st.columns([3, 1])
    with col1:
        progress = task.get('progress_percent', 0) / 100
        st.progress(progress)
        st.caption(f"当前步骤: {task.get('current_step', '处理中...')} ({task.get('progress_percent', 0):.0f}%)")

    with col2:
        if st.button("取消任务", type="secondary"):
            selector_scheduler.cancel_task(task['task_id'])
            st.rerun()

    st.markdown("---")
    st.info("💡 您可以离开此页面，任务将在后台继续运行。")

    time.sleep(2)
    st.rerun()
    return True


def display_small_cap():
    """显示小市值策略界面"""

    # 检查是否显示监控面板
    if st.session_state.get('show_small_cap_monitor'):
        from low_price_bull_monitor_ui import display_monitor_panel
        display_monitor_panel()

        # 返回按钮
        if st.button("🔙 返回选股", type="secondary"):
            del st.session_state.show_small_cap_monitor
            st.rerun()
        return

    # 检查是否显示历史记录
    if st.session_state.get('show_small_cap_history'):
        display_selection_history()
        return

    # 检查后台任务状态
    if check_and_display_background_task():
        return

    # 顶部按钮区
    col_title, col_monitor, col_history = st.columns([3, 1, 1])

    with col_title:
        st.markdown("## 📊 小市值策略 - 小盘高成长股票筛选")

    with col_monitor:
        st.write("")  # 占位
        if st.button("📊 策略监控", type="primary", width='content'):
            st.session_state.show_small_cap_monitor = True
            st.rerun()

    with col_history:
        st.write("")  # 占位
        if st.button("📚 选股历史", width='content'):
            st.session_state.show_small_cap_history = True
            st.rerun()
    
    st.markdown("---")
    
    st.markdown("""
    ### 📋 选股策略说明

    **筛选条件**：
    - ✅ 总市值 ≤ 50亿
    - ✅ 营收增长率 ≥ 10%
    - ✅ 净利润增长率 ≥ 100%（净利润同比增长率）
    - ✅ 按总市值由小至大排名

    **量化交易策略**：
    - 💰 资金量：10万元
    - 📅 持股周期：5天
    - 💼 仓位控制：满仓
    - 📊 个股最大持仓：3成（30%）
    - 🎯 账户最大持股数：4只
    - 🛒 单日最大买入数：2只
    - 📈 买入时机：开盘买入
    - 📉 卖出时机：MA5下穿MA20或持股满5天
    """)
    
    st.markdown("---")

    # 参数设置
    col1, col2 = st.columns([2, 1])

    with col1:
        top_n = st.slider(
            "筛选数量",
            min_value=3,
            max_value=10,
            value=5,
            step=1,
            help="选择展示的股票数量"
        )

    with col2:
        st.info(f"💡 将筛选市值最小的前{top_n}只股票")

    # 高级选项
    with st.expander("⚙️ 高级筛选参数"):
        # 市场选择
        st.markdown("**市场选择**")
        market_options = {
            "上海主板": "上海主板",
            "深圳主板": "深圳主板",
            "创业板": "创业板",
            "北交所": "北交所"
        }
        selected_markets = st.multiselect(
            "选择市场",
            options=list(market_options.keys()),
            default=["上海主板", "深圳主板"],
            help="选择要筛选的市场，默认为沪深主板",
            key="small_cap_markets"
        )

    st.markdown("---")

    # 选股按钮区域
    btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 2])

    with btn_col1:
        start_button = st.button("🚀 开始小市值策略选股", type="primary", width='content')

    with btn_col2:
        background_button = st.button("🔄 后台选股", width='content', help="提交后台任务，可离开页面")

    # 前台选股
    if start_button:
        # 验证市场选择
        if not selected_markets:
            st.error("请至少选择一个市场")
            st.stop()

        with st.spinner("正在获取数据，请稍候..."):
            success, stocks_df, message = small_cap_selector.get_small_cap_stocks(
                top_n=top_n,
                markets=selected_markets
            )

            if not success:
                st.error(f"❌ {message}")
                return

            st.success(f"✅ {message}")

            # 保存到session_state
            st.session_state.small_cap_stocks = stocks_df
            st.session_state.small_cap_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 后台选股
    if background_button:
        if not selected_markets:
            st.error("请至少选择一个市场")
            st.stop()

        result = selector_scheduler.start_background_selection(
            selector_type='small_cap',
            selection_func=run_small_cap_selection,
            params={'top_n': top_n}
        )

        if result.get('success'):
            st.session_state.small_cap_task_id = result['task_id']
            st.success("✅ 后台选股任务已启动")
            st.info("💡 任务已提交到后台，您可以离开页面，稍后返回查看结果")
            time.sleep(1)
            st.rerun()
        else:
            st.error(f"❌ {result.get('message', '启动失败')}")
    
    # 显示选股结果
    if 'small_cap_stocks' in st.session_state and st.session_state.small_cap_stocks is not None:
        st.markdown("---")
        st.markdown("## 📈 选股结果")
        
        stocks_df = st.session_state.small_cap_stocks
        select_time = st.session_state.small_cap_time
        
        st.info(f"🕒 选股时间：{select_time} | 📊 股票数量：{len(stocks_df)} 只")
        
        # 显示股票列表
        display_stock_list(stocks_df)
        
        # 发送钉钉通知
        st.markdown("---")
        if st.button("📲 发送钉钉通知", type="secondary", use_container_width=True):
            send_dingtalk_notification(stocks_df)


def display_stock_list(stocks_df: pd.DataFrame):
    """显示股票列表"""
    
    for idx, row in stocks_df.iterrows():
        stock_code = row.get('股票代码', 'N/A')
        stock_name = row.get('股票简称', 'N/A')
        
        with st.expander(f"📊 {idx+1}. {stock_code} {stock_name}", expanded=True):
            display_stock_detail(row)


def display_stock_detail(row: pd.Series):
    """显示股票详细信息"""
    
    # 获取所有可能的字段
    financial_fields = [
        ('总市值', row.get('总市值', row.get('总市值[20241211]', None))),
        ('营收增长率', row.get('营收增长率', row.get('营业收入增长率', None))),
        ('净利润增长率', row.get('净利润增长率', row.get('净利润同比增长率', None))),
        ('股价', row.get('股价', row.get('最新价', None))),
        ('市盈率', row.get('市盈率', row.get('市盈率TTM', None))),
        ('市净率', row.get('市净率', row.get('市净率PB', None))),
        ('所属行业', row.get('所属行业', row.get('所属同花顺行业', None))),
    ]
    
    # 检查是否有任何有效数据
    has_any_data = any(is_valid_value(value) for _, value in financial_fields)
    
    # 决定布局
    if has_any_data:
        col1, col2 = st.columns(2)
    else:
        col1 = st.container()
        col2 = None
    
    with col1:
        st.markdown("#### 📊 基本信息")
        st.markdown(f"**股票代码**: {row.get('股票代码', 'N/A')}")
        st.markdown(f"**股票名称**: {row.get('股票简称', 'N/A')}")
    
    # 只有当有财务数据时才显示财务指标
    if col2 is not None:
        with col2:
            st.markdown("#### 💼 财务指标")
            
            for field_name, value in financial_fields:
                if is_valid_value(value):
                    formatted_value = format_value(value, get_suffix(field_name))
                    st.markdown(f"**{field_name}**: {formatted_value}")
    
    # 添加监控按钮
    st.markdown("---")
    st.markdown("#### 📊 策略监控")
    
    from low_price_bull_monitor_ui import add_stock_to_monitor_button
    
    stock_code = row.get('股票代码', '')
    stock_name = row.get('股票简称', '')
    price = row.get('股价', row.get('最新价', None))
    
    # 去掉代码后缀
    if isinstance(stock_code, str) and '.' in stock_code:
        stock_code = stock_code.split('.')[0]
    
    # 转换价格
    try:
        price_float = float(price) if price and not pd.isna(price) else None
    except:
        price_float = None
    
    if stock_code and stock_name:
        add_stock_to_monitor_button(stock_code, stock_name, price_float)


def is_valid_value(value):
    """判断值是否有效"""
    if value is None:
        return False
    if pd.isna(value):
        return False
    if str(value).strip() in ['', 'N/A', 'nan', 'None']:
        return False
    return True


def format_value(value, suffix=''):
    """格式化显示值"""
    if isinstance(value, (int, float)):
        if abs(value) >= 100000000:  # 亿
            return f"{value/100000000:.2f}亿{suffix}"
        elif abs(value) >= 10000:  # 万
            return f"{value/10000:.2f}万{suffix}"
        else:
            return f"{value:.2f}{suffix}"
    return f"{value}{suffix}"


def get_suffix(field_name: str) -> str:
    """获取字段后缀"""
    suffix_map = {
        '总市值': '元',
        '股价': '元',
        '营收增长率': '%',
        '净利润增长率': '%',
    }
    return suffix_map.get(field_name, '')


def send_dingtalk_notification(stocks_df: pd.DataFrame):
    """发送钉钉通知"""
    
    try:
        if not notification_service.config['webhook_enabled']:
            st.warning("⚠️ Webhook通知未启用，请在系统配置中启用")
            return
        
        # 构建消息
        keyword = notification_service.config.get('webhook_keyword', 'aiagents通知')
        
        message_text = f"### {keyword} - 小市值策略选股完成\n\n"
        message_text += "**筛选策略**: 总市值≤50亿 + 营收增长率≥10% + 净利润增长率≥100% + 沪深A股\n\n"
        message_text += f"**筛选数量**: {len(stocks_df)} 只\n\n"
        message_text += "**精选股票**:\n\n"
        
        for idx, row in stocks_df.iterrows():
            stock_code = row.get('股票代码', 'N/A')
            stock_name = row.get('股票简称', 'N/A')
            message_text += f"{idx+1}. {stock_code} {stock_name}\n\n"
        
        message_text += f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        message_text += "_此消息由AI股票分析系统自动发送_"
        
        # 直接发送钉钉Webhook
        if notification_service.config['webhook_type'] == 'dingtalk':
            import requests
            
            data = {
                "msgtype": "markdown",
                "markdown": {
                    "title": f"{keyword}",
                    "text": message_text
                }
            }
            
            response = requests.post(
                notification_service.config['webhook_url'],
                json=data,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code == 200:
                st.success("✅ 钉钉通知发送成功")
            else:
                st.error(f"❌ 钉钉通知发送失败: HTTP {response.status_code}")
        else:
            st.warning("⚠️ 当前仅支持钉钉通知")
    
    except Exception as e:
        st.error(f"❌ 发送通知失败: {str(e)}")
