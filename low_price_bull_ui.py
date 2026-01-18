#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
低价擒牛UI模块
"""

import streamlit as st
import pandas as pd
import time
from datetime import datetime
from low_price_bull_selector import LowPriceBullSelector
from low_price_bull_strategy import LowPriceBullStrategy
from notification_service import notification_service
from low_price_bull_monitor import low_price_bull_monitor
from low_price_bull_service import low_price_bull_service
from selector_scheduler import selector_scheduler, run_low_price_bull_selection
from selector_task_db import selector_task_db


def display_selection_history():
    """显示选股历史记录"""
    st.markdown("## 📚 低价擒牛选股历史")
    st.markdown("---")

    # 获取历史记录
    tasks = selector_task_db.get_recent_tasks('low_price_bull', limit=20)

    if not tasks:
        st.info("暂无选股历史记录")
        if st.button("🔙 返回选股", type="primary"):
            del st.session_state.show_low_price_bull_history
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
                    markets = params.get('markets', [])
                    if markets:
                        st.caption(f"市场: {', '.join(markets)}")

            # 显示结果
            if task['status'] == 'completed' and task.get('results'):
                results = task['results']
                if results.get('stocks'):
                    st.markdown("**选股结果:**")
                    stocks_df = pd.DataFrame(results['stocks'])
                    # 选择关键列显示
                    display_cols = [col for col in ['股票代码', '股票简称', '最新价', '涨跌幅', '市值'] if col in stocks_df.columns]
                    if display_cols:
                        st.dataframe(stocks_df[display_cols], use_container_width=True, height=200)
                    else:
                        st.dataframe(stocks_df.head(10), use_container_width=True, height=200)

                    # 加载结果按钮
                    if st.button("📥 加载此结果", key=f"load_{task['task_id']}"):
                        st.session_state.low_price_bull_stocks = stocks_df
                        del st.session_state.show_low_price_bull_history
                        st.rerun()

            elif task['status'] == 'failed':
                st.error(f"失败原因: {task.get('error_message', '未知错误')}")

    st.markdown("---")
    if st.button("🔙 返回选股", type="primary"):
        del st.session_state.show_low_price_bull_history
        st.rerun()


def check_and_display_background_task() -> bool:
    """检查并显示后台任务状态，返回是否有运行中的任务"""
    running_tasks = selector_scheduler.get_running_tasks('low_price_bull')

    if not running_tasks:
        # 检查是否有刚完成的任务
        if 'low_price_bull_task_id' in st.session_state:
            task = selector_scheduler.get_task_status(st.session_state.low_price_bull_task_id)
            if task and task['status'] == 'completed':
                st.success("✅ 后台选股任务已完成!")
                # 加载结果
                if task.get('results') and task['results'].get('success'):
                    stocks_data = task['results'].get('stocks', [])
                    if stocks_data:
                        st.session_state.low_price_bull_stocks = pd.DataFrame(stocks_data)
                del st.session_state.low_price_bull_task_id
                st.rerun()
            elif task and task['status'] == 'failed':
                st.error(f"❌ 后台选股失败: {task.get('error_message', '未知错误')}")
                del st.session_state.low_price_bull_task_id
        return False

    # 显示运行中的任务
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
    st.info("💡 您可以离开此页面，任务将在后台继续运行。稍后回来查看结果。")

    # 自动刷新
    time.sleep(2)
    st.rerun()

    return True


def display_low_price_bull():
    """显示低价擒牛选股界面"""

    # 检查是否显示监控面板
    if st.session_state.get('show_low_price_monitor'):
        from low_price_bull_monitor_ui import display_monitor_panel
        display_monitor_panel()

        # 返回按钮
        if st.button("🔙 返回选股", type="secondary"):
            del st.session_state.show_low_price_monitor
            st.rerun()
        return

    # 检查是否显示历史记录
    if st.session_state.get('show_low_price_bull_history'):
        display_selection_history()
        return

    # 检查后台任务状态
    if check_and_display_background_task():
        return

    # 顶部按钮区
    col_title, col_monitor, col_history = st.columns([3, 1, 1])

    with col_title:
        st.markdown("## 🐂 低价擒牛 - 低价高成长股票筛选")

    with col_monitor:
        st.write("")  # 占位
        if st.button("📊 策略监控", type="primary", width='content'):
            st.session_state.show_low_price_monitor = True
            st.rerun()

    with col_history:
        st.write("")  # 占位
        if st.button("📚 选股历史", width='content'):
            st.session_state.show_low_price_bull_history = True
            st.rerun()
    
    st.markdown("---")
    
    st.markdown("""
    ### 📋 选股策略说明

    **筛选条件**：
    - ✅ 股价 < 10元
    - ✅ 净利润增长率 ≥ 100%（净利润同比增长率）
    - ✅ 按成交额由小至大排名

    **量化交易策略**：
    - 💰 资金量：100万元
    - 📅 持股周期：5天
    - 💼 仓位控制：满仓
    - 📊 个股最大持仓：4成（40%）
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
        st.info(f"💡 将筛选成交额最小的前{top_n}只股票")

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
            key="low_price_bull_markets"
        )

    st.markdown("---")

    # 选股按钮区域
    btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 2])

    with btn_col1:
        start_button = st.button("🚀 开始低价擒牛选股", type="primary", width='content')

    with btn_col2:
        background_button = st.button("🔄 后台选股", width='content', help="提交后台任务，可离开页面")

    # 前台选股
    if start_button:
        # 验证市场选择
        if not selected_markets:
            st.error("请至少选择一个市场")
            st.stop()

        with st.spinner("正在获取数据，请稍候..."):
            # 创建选股器
            selector = LowPriceBullSelector()

            # 获取股票
            success, stocks_df, message = selector.get_low_price_stocks(
                top_n=top_n,
                markets=selected_markets
            )

            if success and stocks_df is not None:
                # 保存结果
                st.session_state.low_price_bull_stocks = stocks_df
                st.session_state.low_price_bull_selector = selector

                st.success(f"✅ {message}")

                # 发送钉钉通知
                send_dingtalk_notification(stocks_df, top_n)

                st.rerun()
            else:
                st.error(f"❌ {message}")

    # 后台选股
    if background_button:
        # 验证市场选择
        if not selected_markets:
            st.error("请至少选择一个市场")
            st.stop()

        result = selector_scheduler.start_background_selection(
            selector_type='low_price_bull',
            selection_func=run_low_price_bull_selection,
            params={'top_n': top_n, 'markets': selected_markets}
        )

        if result.get('success'):
            st.session_state.low_price_bull_task_id = result['task_id']
            st.success("✅ 后台选股任务已启动")
            st.info("💡 任务已提交到后台，您可以离开页面，稍后返回查看结果")
            time.sleep(1)
            st.rerun()
        else:
            st.error(f"❌ {result.get('message', '启动失败')}")
    
    # 显示选股结果
    if 'low_price_bull_stocks' in st.session_state:
        display_stock_results(
            st.session_state.low_price_bull_stocks,
            st.session_state.get('low_price_bull_selector')
        )


def display_stock_results(stocks_df: pd.DataFrame, selector):
    """显示选股结果"""

    def find_column(df, *patterns):
        """智能查找DataFrame中的列名，支持模糊匹配"""
        for pattern in patterns:
            # 精确匹配
            if pattern in df.columns:
                return pattern
            # 模糊匹配
            for col in df.columns:
                if pattern in col:
                    return col
        return None

    st.markdown("---")
    st.markdown("## 📊 选股结果")

    # 统计信息
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("筛选数量", f"{len(stocks_df)} 只")

    with col2:
        # 智能计算平均净利增长率（过滤无效值）
        growth_col_name = find_column(stocks_df, '净利润', '同比增长率', '净利润增长率')
        if growth_col_name:
            growth_col = stocks_df[growth_col_name]
            valid_growth = growth_col[growth_col.notna() & (growth_col != '') & (growth_col != 'N/A')]
            if len(valid_growth) > 0:
                avg_growth = pd.to_numeric(valid_growth, errors='coerce').mean()
                if not pd.isna(avg_growth):
                    st.metric("平均净利增长率", f"{avg_growth:.1f}%")
                else:
                    st.metric("平均净利增长率", "-")
            else:
                st.metric("平均净利增长率", "-")
        else:
            st.metric("平均净利增长率", "-")

    with col3:
        # 智能计算平均股价（过滤无效值）
        price_col_name = find_column(stocks_df, '收盘价', '股价', '最新价')
        if price_col_name:
            price_col = stocks_df[price_col_name]
            valid_price = price_col[price_col.notna() & (price_col != '') & (price_col != 'N/A')]
            if len(valid_price) > 0:
                avg_price = pd.to_numeric(valid_price, errors='coerce').mean()
                if not pd.isna(avg_price):
                    st.metric("平均股价", f"{avg_price:.2f} 元")
                else:
                    st.metric("平均股价", "-")
            else:
                st.metric("平均股价", "-")
        else:
            st.metric("平均股价", "-")
    
    st.markdown("---")
    
    # 显示股票列表
    st.markdown("### 📋 精选股票列表")
    
    for idx, row in stocks_df.iterrows():
        # 获取股票代码和简称
        code = row.get('股票代码', 'N/A')
        name = row.get('股票简称', 'N/A')
        
        # 获取价格信息作为标题补充（智能匹配列名）
        price = None
        for pattern in ['收盘价', '股价', '最新价']:
            # 先精确匹配
            if pattern in row.index:
                val = row.get(pattern)
                if val is not None and not pd.isna(val) and str(val).strip() not in ['', 'N/A']:
                    price = val
                    break
            # 再模糊匹配
            for col in row.index:
                if pattern in col:
                    val = row.get(col)
                    if val is not None and not pd.isna(val) and str(val).strip() not in ['', 'N/A']:
                        price = val
                        break
            if price is not None:
                break

        price_str = ''
        if price is not None:
            try:
                price_float = float(price)
                price_str = f" | 价格: {price_float:.2f}元"
            except:
                pass
        
        with st.expander(
            f"【第{idx+1}名】{code} - {name}{price_str}",
            expanded=(idx < 3)
        ):
            display_stock_detail(row)
    
    # 完整数据表格
    st.markdown("---")
    st.markdown("### 📊 完整数据表格")
    
    # 选择关键列显示
    display_cols = ['股票代码', '股票简称']

    # 智能匹配列名 - 价格
    for pattern in ['收盘价', '股价', '最新价']:
        matching = [col for col in stocks_df.columns if pattern in col]
        if matching:
            display_cols.append(matching[0])
            break

    # 涨跌幅
    for pattern in ['最新涨跌幅', '涨跌幅']:
        matching = [col for col in stocks_df.columns if pattern in col]
        if matching:
            display_cols.append(matching[0])
            break

    # 净利润增长率
    for pattern in ['净利润', '同比增长率']:
        matching = [col for col in stocks_df.columns if pattern in col]
        if matching:
            display_cols.append(matching[0])
            break

    # 成交额
    for pattern in ['成交额']:
        matching = [col for col in stocks_df.columns if pattern in col]
        if matching:
            display_cols.append(matching[0])
            break

    for col_name in ['总市值', '市盈率', '市净率', '所属行业']:
        matching = [col for col in stocks_df.columns if col_name in col]
        if matching:
            display_cols.append(matching[0])
    
    # 选择存在的列
    final_cols = [col for col in display_cols if col in stocks_df.columns]
    
    if final_cols:
        st.dataframe(stocks_df[final_cols], width='content', height=400)
        
        # 下载按钮
        csv = stocks_df[final_cols].to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            label="📥 下载股票列表CSV",
            data=csv,
            file_name=f"low_price_bull_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
    
    # 量化交易模拟
    st.markdown("---")
    display_strategy_simulation(stocks_df, selector)


def display_stock_detail(row: pd.Series):
    """显示单个股票详情"""

    def is_valid_value(value):
        """判断值是否有效（非None、非NaN、非空字符串、非'N/A'）"""
        if value is None:
            return False
        if pd.isna(value):
            return False
        if str(value).strip() in ['', 'N/A', 'nan', 'None']:
            return False
        return True

    def format_value(value, suffix=''):
        """格式化显示值"""
        try:
            num_value = float(value)
            if abs(num_value) >= 100000000:  # 亿
                return f"{num_value/100000000:.2f}亿{suffix}"
            elif abs(num_value) >= 10000:  # 万
                return f"{num_value/10000:.2f}万{suffix}"
            else:
                return f"{num_value:.2f}{suffix}"
        except (ValueError, TypeError):
            return f"{value}{suffix}"

    def smart_get(row, *patterns):
        """智能获取列值，支持模糊匹配带日期后缀的列名"""
        # 先尝试精确匹配
        for pattern in patterns:
            if pattern in row.index:
                val = row.get(pattern)
                if is_valid_value(val):
                    return val
        # 再尝试模糊匹配（列名包含关键字）
        for pattern in patterns:
            for col in row.index:
                if pattern in col:
                    val = row.get(col)
                    if is_valid_value(val):
                        return val
        return None
    
    # 先检查是否有任何财务数据（使用smart_get）
    has_any_data = False
    financial_fields = [
        ('所属行业', smart_get(row, '所属行业', '所属同花顺行业')),
        ('总市值', smart_get(row, '总市值')),
        ('市盈率', smart_get(row, '市盈率', '市盈率pe')),
        ('市净率', smart_get(row, '市净率', '市净率pb')),
        ('流通市值', smart_get(row, '流通市值')),
        ('换手率', smart_get(row, '换手率'))
    ]

    for _, value in financial_fields:
        if is_valid_value(value):
            has_any_data = True
            break
    
    # 只有当存在有效数据时才显示两列布局
    if has_any_data:
        col1, col2 = st.columns(2)
    else:
        col1 = st.container()
        col2 = None
    
    with col1:
        st.markdown("#### 📊 基本信息")
        
        # 股票代码（必显示）
        code = row.get('股票代码', '')
        if is_valid_value(code):
            st.markdown(f"**股票代码**: {code}")
        
        # 股票简称（必显示）
        name = row.get('股票简称', '')
        if is_valid_value(name):
            st.markdown(f"**股票简称**: {name}")
        
        # 当前价格（匹配 收盘价:不复权[日期] 或 股价 或 最新价）
        price = smart_get(row, '收盘价', '股价', '最新价')
        if is_valid_value(price):
            st.markdown(f"**当前价格**: {format_value(price, '元')}")

        # 净利润增长率（匹配 归属母公司股东的净利润(同比增长率) 或 净利润增长率）
        growth = smart_get(row, '净利润', '同比增长率', '净利润增长率')
        if is_valid_value(growth):
            st.markdown(f"**净利润增长率**: {format_value(growth, '%')}")

        # 成交额
        turnover = smart_get(row, '成交额')
        if is_valid_value(turnover):
            st.markdown(f"**成交额**: {format_value(turnover, '元')}")

        # 涨跌幅（匹配 最新涨跌幅 或 涨跌幅）
        change_pct = smart_get(row, '最新涨跌幅', '涨跌幅')
        if is_valid_value(change_pct):
            try:
                pct_value = float(change_pct)
                pct_color = "#FF0000" if pct_value >= 0 else "#00AA00"
                st.markdown(f"**涨跌幅**: <span style='color:{pct_color};font-weight:bold;'>{format_value(change_pct, '%')}</span>", unsafe_allow_html=True)
            except:
                st.markdown(f"**涨跌幅**: {format_value(change_pct, '%')}")
    
    # 只有当有财务数据时才显示财务指标栏目
    if col2 is not None:
        with col2:
            st.markdown("#### 💼 财务指标")

            # 所属行业
            industry = smart_get(row, '所属行业', '所属同花顺行业')
            if is_valid_value(industry):
                st.markdown(f"**所属行业**: {industry}")

            # 总市值
            market_cap = smart_get(row, '总市值')
            if is_valid_value(market_cap):
                st.markdown(f"**总市值**: {format_value(market_cap, '元')}")

            # 市盈率
            pe = smart_get(row, '市盈率', '市盈率pe')
            if is_valid_value(pe):
                st.markdown(f"**市盈率**: {format_value(pe, '')}")

            # 市净率
            pb = smart_get(row, '市净率', '市净率pb')
            if is_valid_value(pb):
                st.markdown(f"**市净率**: {format_value(pb, '')}")

            # 流通市值
            float_cap = smart_get(row, '流通市值')
            if is_valid_value(float_cap):
                st.markdown(f"**流通市值**: {format_value(float_cap, '元')}")

            # 换手率
            turnover_rate = smart_get(row, '换手率')
            if is_valid_value(turnover_rate):
                st.markdown(f"**换手率**: {format_value(turnover_rate, '%')}")
    
    # 添加监控按钮
    st.markdown("---")
    st.markdown("#### 📊 策略监控")

    from low_price_bull_monitor_ui import add_stock_to_monitor_button

    stock_code = row.get('股票代码', '')
    stock_name = row.get('股票简称', '')
    # 使用智能匹配获取价格
    price = smart_get(row, '收盘价', '股价', '最新价')

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


def display_strategy_simulation(stocks_df: pd.DataFrame, selector):
    """显示量化交易策略模拟"""
    
    st.markdown("## 🎯 策略监控与模拟")
    
    st.info("""
    **监控说明**：
    - 在上方股票列表中点击"➕ 加入策略监控"按钮即可加入
    - 监控条件：① 持股满5天第6天开盘提醒卖出 ② MA5下穿MA20提醒卖出
    - 扫描频率：每分钟扫描1次（可在监控面板配置）
    - 提醒卖出后自动移出监控列表
    - 点击右上角"📊 策略监控"按钮查看监控面板
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🎮 开始策略模拟", type="primary", width='content'):
            st.session_state.show_strategy_simulation = True
    
    with col2:
        if st.button("🔗 连接MiniQMT实盘", type="secondary", width='content'):
            st.warning("⚠️ MiniQMT实盘交易功能需要先配置环境变量，详见系统配置")
    
    # 显示模拟结果
    if st.session_state.get('show_strategy_simulation'):
        run_strategy_simulation(stocks_df)


def run_strategy_simulation(stocks_df: pd.DataFrame):
    """运行策略模拟"""

    def smart_get_row(row, *patterns):
        """智能获取行值，支持模糊匹配列名"""
        for pattern in patterns:
            # 精确匹配
            if pattern in row.index:
                val = row.get(pattern)
                if val is not None and not pd.isna(val) and str(val).strip() not in ['', 'N/A', 'nan', 'None']:
                    return val
            # 模糊匹配
            for col in row.index:
                if pattern in col:
                    val = row.get(col)
                    if val is not None and not pd.isna(val) and str(val).strip() not in ['', 'N/A', 'nan', 'None']:
                        return val
        return None

    st.markdown("---")
    st.markdown("### 📈 策略模拟执行")

    # 创建策略实例
    strategy = LowPriceBullStrategy(initial_capital=1000000.0)

    # 模拟买入（按成交额排序，优先买入成交额小的）
    st.markdown("#### 1️⃣ 模拟买入信号")

    buy_results = []
    current_date = datetime.now().strftime("%Y-%m-%d")

    for idx, row in stocks_df.head(strategy.max_daily_buy).iterrows():
        code = str(row.get('股票代码', '')).split('.')[0]
        name = row.get('股票简称', 'N/A')
        # 智能匹配价格列
        price_val = smart_get_row(row, '收盘价', '股价', '最新价')
        price = float(price_val) if price_val else 0
        
        if price > 0:
            success, message, trade = strategy.buy(code, name, price, current_date)
            buy_results.append({
                'success': success,
                'message': message,
                'trade': trade
            })
    
    # 显示买入结果
    for result in buy_results:
        if result['success']:
            st.success(result['message'])
        else:
            st.warning(f"⚠️ {result['message']}")
    
    # 显示持仓
    st.markdown("---")
    st.markdown("#### 2️⃣ 当前持仓")
    
    positions = strategy.get_positions()
    if positions:
        positions_df = pd.DataFrame(positions)
        st.dataframe(positions_df, width='content')
    else:
        st.info("暂无持仓")
    
    # 显示账户摘要
    st.markdown("---")
    st.markdown("#### 3️⃣ 账户摘要")
    
    summary = strategy.get_portfolio_summary()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("初始资金", f"{summary['initial_capital']:,.0f} 元")
    
    with col2:
        st.metric("可用资金", f"{summary['available_cash']:,.0f} 元")
    
    with col3:
        st.metric("持仓市值", f"{summary['position_value']:,.0f} 元")
    
    with col4:
        st.metric("总资产", f"{summary['total_value']:,.0f} 元")
    
    st.markdown("---")
    
    # 策略说明
    st.markdown("#### 📝 策略执行说明")
    st.markdown("""
    **后续操作**：
    1. **持有期管理**：系统会自动跟踪每只股票的持有天数
    2. **卖出信号监测**：
       - 每日收盘后计算MA5和MA20
       - 如果MA5下穿MA20，触发卖出信号
       - 如果持股满5天，强制卖出
    3. **轮动买入**：卖出后释放资金，继续买入新的符合条件的股票
    
    **风险提示**：
    - ⚠️ 本策略为模拟演示，实际交易存在滑点、手续费等成本
    - ⚠️ 历史业绩不代表未来收益
    - ⚠️ 请谨慎评估风险，理性投资
    """)


def send_dingtalk_notification(stocks_df: pd.DataFrame, top_n: int):
    """发送钉钉通知"""

    def smart_get_row(row, *patterns):
        """智能获取行值，支持模糊匹配列名"""
        for pattern in patterns:
            # 精确匹配
            if pattern in row.index:
                val = row.get(pattern)
                if val is not None and not pd.isna(val) and str(val).strip() not in ['', 'N/A', 'nan', 'None']:
                    return val
            # 模糊匹配
            for col in row.index:
                if pattern in col:
                    val = row.get(col)
                    if val is not None and not pd.isna(val) and str(val).strip() not in ['', 'N/A', 'nan', 'None']:
                        return val
        return None

    try:
        # 检查webhook配置
        webhook_config = notification_service.get_webhook_config_status()

        if not webhook_config['enabled'] or not webhook_config['configured']:
            st.info("💡 未配置Webhook通知，如需接收钉钉消息请在环境配置中设置")
            return

        # 构建消息内容
        keyword = notification_service.config.get('webhook_keyword', 'aiagents通知')

        message_text = f"### {keyword} - 低价擒牛选股完成\n\n"
        message_text += f"**筛选策略**: 股价<10元 + 净利润增长率≥100% + 沪深A股\n\n"
        message_text += f"**筛选数量**: {len(stocks_df)} 只\n\n"
        message_text += f"**精选股票**:\n\n"

        for idx, row in stocks_df.head(top_n).iterrows():
            code = row.get('股票代码', '')
            name = row.get('股票简称', '')

            # 只显示有效的信息
            message_text += f"{idx+1}. **{code} {name}**\n"

            # 股价（使用智能匹配）
            price = smart_get_row(row, '收盘价', '股价', '最新价')
            if price is not None:
                try:
                    price_float = float(price)
                    message_text += f"   - 股价: {price_float:.2f}元\n"
                except:
                    pass

            # 净利润增长率（使用智能匹配）
            growth = smart_get_row(row, '净利润', '同比增长率', '净利润增长率')
            if growth is not None:
                try:
                    growth_float = float(growth)
                    message_text += f"   - 净利增长: {growth_float:.2f}%\n"
                except:
                    pass

            # 成交额（使用智能匹配）
            turnover = smart_get_row(row, '成交额')
            if turnover is not None:
                try:
                    turnover_float = float(turnover)
                    if turnover_float >= 100000000:  # 亿
                        message_text += f"   - 成交额: {turnover_float/100000000:.2f}亿元\n"
                    elif turnover_float >= 10000:  # 万
                        message_text += f"   - 成交额: {turnover_float/10000:.2f}万元\n"
                    else:
                        message_text += f"   - 成交额: {turnover_float:.2f}元\n"
                except:
                    pass

            # 所属行业（使用智能匹配）
            industry = smart_get_row(row, '所属行业', '所属同花顺行业')
            if industry is not None:
                message_text += f"   - 所属行业: {industry}\n"

            message_text += "\n"
        
        message_text += f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        message_text += "_此消息由AI股票分析系统自动发送_"
        
        # 直接发送钉钉Webhook（不使用notification_service的默认格式）
        if notification_service.config['webhook_type'] == 'dingtalk':
            import requests
            
            data = {
                "msgtype": "markdown",
                "markdown": {
                    "title": f"{keyword}",
                    "text": message_text
                }
            }
            
            try:
                response = requests.post(
                    notification_service.config['webhook_url'],
                    json=data,
                    headers={'Content-Type': 'application/json'},
                    timeout=10
                )
                
                if response.status_code == 200:
                    result = response.json()
                    if result.get('errcode') == 0:
                        st.success("✅ 已发送钉钉通知")
                    else:
                        st.warning(f"⚠️ 钉钉通知发送失败: {result.get('errmsg')}")
                else:
                    st.warning(f"⚠️ 钉钉通知请求失败: HTTP {response.status_code}")
            except Exception as e:
                st.warning(f"⚠️ 发送钉钉通知失败: {str(e)}")
        
    except Exception as e:
        st.warning(f"⚠️ 发送通知时出错: {str(e)}")
