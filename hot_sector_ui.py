#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
热门板块UI模块
快速查看今日热门行业和概念板块
"""

import streamlit as st
import pandas as pd
import akshare as ak
from datetime import datetime


def display_hot_sectors():
    """显示热门板块主界面"""

    st.markdown("## 🔥 热门板块 - 今日板块涨跌榜")
    st.markdown("---")

    # 刷新按钮
    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("🔄 刷新数据", type="primary"):
            # 清除缓存强制刷新
            st.cache_data.clear()
            st.rerun()
    with col2:
        st.caption(f"数据更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    st.markdown("---")

    # 创建标签页
    tab1, tab2, tab3 = st.tabs(["🏭 行业板块", "💡 概念板块", "📊 板块对比"])

    with tab1:
        display_industry_sectors()

    with tab2:
        display_concept_sectors()

    with tab3:
        display_sector_comparison()


@st.cache_data(ttl=300)  # 缓存5分钟
def get_industry_sectors():
    """获取行业板块数据"""
    try:
        df = ak.stock_board_industry_name_em()
        return df
    except Exception as e:
        st.error(f"获取行业板块数据失败: {e}")
        return None


@st.cache_data(ttl=300)  # 缓存5分钟
def get_concept_sectors():
    """获取概念板块数据"""
    try:
        df = ak.stock_board_concept_name_em()
        return df
    except Exception as e:
        st.error(f"获取概念板块数据失败: {e}")
        return None


@st.cache_data(ttl=1800)  # 缓存30分钟（月度数据变化慢）
def get_monthly_sector_changes(sector_type='industry'):
    """
    获取板块本月涨跌幅
    sector_type: 'industry' 行业板块, 'concept' 概念板块
    """
    try:
        # 获取板块列表
        if sector_type == 'industry':
            df = ak.stock_board_industry_name_em()
            hist_func = ak.stock_board_industry_hist_em
        else:
            df = ak.stock_board_concept_name_em()
            hist_func = ak.stock_board_concept_hist_em

        # 计算本月起始日期
        today = datetime.now()
        month_start = today.replace(day=1).strftime("%Y%m%d")
        today_str = today.strftime("%Y%m%d")

        results = []
        # 只计算前30个板块（避免请求过多）
        for idx, row in df.head(30).iterrows():
            board_name = row['板块名称']
            try:
                hist = hist_func(symbol=board_name, period="日k",
                               start_date=month_start, end_date=today_str, adjust="")
                if len(hist) >= 2:
                    first_close = hist.iloc[0]['收盘']
                    last_close = hist.iloc[-1]['收盘']
                    month_change = (last_close - first_close) / first_close * 100
                    results.append({
                        '板块名称': board_name,
                        '月涨跌幅': month_change,
                        '今日涨跌幅': row['涨跌幅'],
                        '领涨股票': row['领涨股票']
                    })
            except:
                continue

        if results:
            return pd.DataFrame(results)
        return None
    except Exception as e:
        return None


def display_industry_sectors():
    """显示行业板块"""

    with st.spinner("正在获取行业板块数据..."):
        df = get_industry_sectors()

    if df is None or df.empty:
        st.warning("暂无数据")
        return

    # 统计信息
    col1, col2, col3, col4 = st.columns(4)

    up_count = len(df[df['涨跌幅'] > 0])
    down_count = len(df[df['涨跌幅'] < 0])
    flat_count = len(df[df['涨跌幅'] == 0])
    avg_change = df['涨跌幅'].mean()

    with col1:
        st.metric("板块总数", f"{len(df)} 个")
    with col2:
        st.metric("上涨板块", f"{up_count} 个", delta=f"{up_count/len(df)*100:.1f}%", delta_color="inverse")
    with col3:
        st.metric("下跌板块", f"{down_count} 个", delta=f"-{down_count/len(df)*100:.1f}%", delta_color="inverse")
    with col4:
        avg_delta = "强势" if avg_change > 0.5 else ("弱势" if avg_change < -0.5 else "震荡")
        avg_delta_color = "inverse" if avg_change > 0 else "normal"
        st.metric("平均涨跌", f"{avg_change:.2f}%", delta=avg_delta, delta_color=avg_delta_color)

    st.markdown("---")

    # 涨幅榜和跌幅榜并排显示
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 📈 涨幅榜 TOP10")
        top10 = df.nlargest(10, '涨跌幅')[['板块名称', '涨跌幅', '领涨股票', '领涨股票-涨跌幅', '上涨家数', '下跌家数']]

        for idx, row in top10.iterrows():
            change_color = "🔴" if row['涨跌幅'] > 0 else "🟢"
            pct_color = "#FF0000" if row['涨跌幅'] > 0 else "#00AA00"
            lead_color = "#FF0000" if row['领涨股票-涨跌幅'] > 0 else "#00AA00"
            lead_sign = "+" if row['领涨股票-涨跌幅'] > 0 else ""
            st.markdown(f"""
            **{change_color} {row['板块名称']}** <span style="color:{pct_color};font-weight:bold;">+{row['涨跌幅']:.2f}%</span>
            领涨: {row['领涨股票']} <span style="color:{lead_color};">{lead_sign}{row['领涨股票-涨跌幅']:.2f}%</span> | 涨{int(row['上涨家数'])}跌{int(row['下跌家数'])}
            """, unsafe_allow_html=True)

    with col2:
        st.markdown("### 📉 跌幅榜 TOP10")
        bottom10 = df.nsmallest(10, '涨跌幅')[['板块名称', '涨跌幅', '领涨股票', '领涨股票-涨跌幅', '上涨家数', '下跌家数']]

        for idx, row in bottom10.iterrows():
            change_color = "🔴" if row['涨跌幅'] > 0 else "🟢"
            pct_color = "#FF0000" if row['涨跌幅'] > 0 else "#00AA00"
            lead_color = "#FF0000" if row['领涨股票-涨跌幅'] > 0 else "#00AA00"
            lead_sign = "+" if row['领涨股票-涨跌幅'] > 0 else ""
            st.markdown(f"""
            **{change_color} {row['板块名称']}** <span style="color:{pct_color};font-weight:bold;">{row['涨跌幅']:.2f}%</span>
            领涨: {row['领涨股票']} <span style="color:{lead_color};">{lead_sign}{row['领涨股票-涨跌幅']:.2f}%</span> | 涨{int(row['上涨家数'])}跌{int(row['下跌家数'])}
            """, unsafe_allow_html=True)

    # 完整数据表格
    st.markdown("---")
    st.markdown("### 📊 全部行业板块")

    display_df = df[['板块名称', '涨跌幅', '最新价', '换手率', '上涨家数', '下跌家数', '领涨股票', '领涨股票-涨跌幅']].copy()
    display_df['涨跌幅'] = display_df['涨跌幅'].apply(lambda x: f"{x:.2f}%")
    display_df['换手率'] = display_df['换手率'].apply(lambda x: f"{x:.2f}%")
    display_df['领涨股票-涨跌幅'] = display_df['领涨股票-涨跌幅'].apply(lambda x: f"{x:.2f}%")

    st.dataframe(display_df, use_container_width=True, height=400)


def display_concept_sectors():
    """显示概念板块"""

    with st.spinner("正在获取概念板块数据..."):
        df = get_concept_sectors()

    if df is None or df.empty:
        st.warning("暂无数据")
        return

    # 统计信息
    col1, col2, col3, col4 = st.columns(4)

    up_count = len(df[df['涨跌幅'] > 0])
    down_count = len(df[df['涨跌幅'] < 0])
    avg_change = df['涨跌幅'].mean()

    with col1:
        st.metric("概念总数", f"{len(df)} 个")
    with col2:
        st.metric("上涨概念", f"{up_count} 个", delta=f"{up_count/len(df)*100:.1f}%", delta_color="inverse")
    with col3:
        st.metric("下跌概念", f"{down_count} 个", delta=f"-{down_count/len(df)*100:.1f}%", delta_color="inverse")
    with col4:
        avg_delta = "强势" if avg_change > 0.5 else ("弱势" if avg_change < -0.5 else "震荡")
        avg_delta_color = "inverse" if avg_change > 0 else "normal"
        st.metric("平均涨跌", f"{avg_change:.2f}%", delta=avg_delta, delta_color=avg_delta_color)

    st.markdown("---")

    # 涨幅榜和跌幅榜并排显示
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 📈 涨幅榜 TOP15")
        top15 = df.nlargest(15, '涨跌幅')[['板块名称', '涨跌幅', '领涨股票', '领涨股票-涨跌幅', '上涨家数', '下跌家数']]

        for idx, row in top15.iterrows():
            change_color = "🔴" if row['涨跌幅'] > 0 else "🟢"
            pct_color = "#FF0000" if row['涨跌幅'] > 0 else "#00AA00"
            lead_color = "#FF0000" if row['领涨股票-涨跌幅'] > 0 else "#00AA00"
            lead_sign = "+" if row['领涨股票-涨跌幅'] > 0 else ""
            st.markdown(f"""
            **{change_color} {row['板块名称']}** <span style="color:{pct_color};font-weight:bold;">+{row['涨跌幅']:.2f}%</span>
            领涨: {row['领涨股票']} <span style="color:{lead_color};">{lead_sign}{row['领涨股票-涨跌幅']:.2f}%</span> | 涨{int(row['上涨家数'])}跌{int(row['下跌家数'])}
            """, unsafe_allow_html=True)

    with col2:
        st.markdown("### 📉 跌幅榜 TOP15")
        bottom15 = df.nsmallest(15, '涨跌幅')[['板块名称', '涨跌幅', '领涨股票', '领涨股票-涨跌幅', '上涨家数', '下跌家数']]

        for idx, row in bottom15.iterrows():
            change_color = "🔴" if row['涨跌幅'] > 0 else "🟢"
            pct_color = "#FF0000" if row['涨跌幅'] > 0 else "#00AA00"
            lead_color = "#FF0000" if row['领涨股票-涨跌幅'] > 0 else "#00AA00"
            lead_sign = "+" if row['领涨股票-涨跌幅'] > 0 else ""
            st.markdown(f"""
            **{change_color} {row['板块名称']}** <span style="color:{pct_color};font-weight:bold;">{row['涨跌幅']:.2f}%</span>
            领涨: {row['领涨股票']} <span style="color:{lead_color};">{lead_sign}{row['领涨股票-涨跌幅']:.2f}%</span> | 涨{int(row['上涨家数'])}跌{int(row['下跌家数'])}
            """, unsafe_allow_html=True)

    # 完整数据表格
    st.markdown("---")
    st.markdown("### 📊 全部概念板块")

    display_df = df[['板块名称', '涨跌幅', '最新价', '换手率', '上涨家数', '下跌家数', '领涨股票', '领涨股票-涨跌幅']].copy()
    display_df['涨跌幅'] = display_df['涨跌幅'].apply(lambda x: f"{x:.2f}%")
    display_df['换手率'] = display_df['换手率'].apply(lambda x: f"{x:.2f}%")
    display_df['领涨股票-涨跌幅'] = display_df['领涨股票-涨跌幅'].apply(lambda x: f"{x:.2f}%")

    st.dataframe(display_df, use_container_width=True, height=400)


def display_sector_comparison():
    """显示板块对比"""

    st.markdown("### 🔥 今日最热板块汇总")

    with st.spinner("正在获取数据..."):
        industry_df = get_industry_sectors()
        concept_df = get_concept_sectors()

    if industry_df is None or concept_df is None:
        st.warning("数据获取失败")
        return

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 🏭 行业热门 TOP5")
        top5_industry = industry_df.nlargest(5, '涨跌幅')[['板块名称', '涨跌幅', '领涨股票']]
        for i, (idx, row) in enumerate(top5_industry.iterrows(), 1):
            pct_color = "#FF0000" if row['涨跌幅'] > 0 else "#00AA00"
            sign = "+" if row['涨跌幅'] > 0 else ""
            st.markdown(f"**{i}. {row['板块名称']}** <span style='color:{pct_color};font-weight:bold;'>{sign}{row['涨跌幅']:.2f}%</span>", unsafe_allow_html=True)

    with col2:
        st.markdown("#### 💡 概念热门 TOP5")
        top5_concept = concept_df.nlargest(5, '涨跌幅')[['板块名称', '涨跌幅', '领涨股票']]
        for i, (idx, row) in enumerate(top5_concept.iterrows(), 1):
            pct_color = "#FF0000" if row['涨跌幅'] > 0 else "#00AA00"
            sign = "+" if row['涨跌幅'] > 0 else ""
            st.markdown(f"**{i}. {row['板块名称']}** <span style='color:{pct_color};font-weight:bold;'>{sign}{row['涨跌幅']:.2f}%</span>", unsafe_allow_html=True)

    st.markdown("---")

    # 市场情绪分析
    st.markdown("### 📊 市场情绪")

    industry_up = len(industry_df[industry_df['涨跌幅'] > 0])
    industry_down = len(industry_df[industry_df['涨跌幅'] < 0])
    concept_up = len(concept_df[concept_df['涨跌幅'] > 0])
    concept_down = len(concept_df[concept_df['涨跌幅'] < 0])

    total_up = industry_up + concept_up
    total_down = industry_down + concept_down
    total = len(industry_df) + len(concept_df)

    up_ratio = total_up / total * 100

    col1, col2, col3 = st.columns(3)

    with col1:
        if up_ratio > 70:
            sentiment = "🔴 强势上涨"
            desc = "市场情绪高涨，多数板块上涨"
        elif up_ratio > 50:
            sentiment = "🟡 偏多震荡"
            desc = "市场略偏强势，板块分化"
        elif up_ratio > 30:
            sentiment = "🟠 偏空震荡"
            desc = "市场略偏弱势，注意风险"
        else:
            sentiment = "🟢 弱势下跌"
            desc = "市场情绪低迷，多数板块下跌"

        st.metric("市场情绪", sentiment)
        st.caption(desc)

    with col2:
        st.metric("上涨板块占比", f"{up_ratio:.1f}%", delta=f"{total_up}个板块上涨")

    with col3:
        st.metric("行业/概念涨跌比", f"{industry_up}:{industry_down} / {concept_up}:{concept_down}")

    st.markdown("---")

    # 热门主题词云（简化版）
    st.markdown("### 🏷️ 今日热门主题")

    # 获取涨幅前20的概念关键词
    hot_concepts = concept_df.nlargest(20, '涨跌幅')['板块名称'].tolist()

    # 显示为标签
    tags_html = " ".join([f'<span style="background:#F0B90B;color:#0B0E11;padding:4px 12px;border-radius:16px;margin:4px;display:inline-block;font-weight:500;">{name}</span>' for name in hot_concepts[:15]])
    st.markdown(f'<div style="line-height:2.5;">{tags_html}</div>', unsafe_allow_html=True)

    # 本月最热板块汇总
    st.markdown("---")
    st.markdown("### 📅 本月最热板块汇总")

    month_name = datetime.now().strftime("%Y年%m月")
    st.caption(f"统计周期: {month_name}1日 至今")

    with st.spinner("正在计算本月板块涨跌幅..."):
        monthly_industry = get_monthly_sector_changes('industry')
        monthly_concept = get_monthly_sector_changes('concept')

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 🏭 行业月度涨幅 TOP10")
        if monthly_industry is not None and len(monthly_industry) > 0:
            top10 = monthly_industry.nlargest(10, '月涨跌幅')
            for i, (idx, row) in enumerate(top10.iterrows(), 1):
                change = row['月涨跌幅']
                color = "🔴" if change > 0 else "🟢"
                pct_color = "#FF0000" if change > 0 else "#00AA00"
                sign = "+" if change > 0 else ""
                st.markdown(f"**{i}. {color} {row['板块名称']}** <span style='color:{pct_color};font-weight:bold;'>{sign}{change:.2f}%</span>", unsafe_allow_html=True)
        else:
            st.info("暂无月度数据")

    with col2:
        st.markdown("#### 💡 概念月度涨幅 TOP10")
        if monthly_concept is not None and len(monthly_concept) > 0:
            top10 = monthly_concept.nlargest(10, '月涨跌幅')
            for i, (idx, row) in enumerate(top10.iterrows(), 1):
                change = row['月涨跌幅']
                color = "🔴" if change > 0 else "🟢"
                pct_color = "#FF0000" if change > 0 else "#00AA00"
                sign = "+" if change > 0 else ""
                st.markdown(f"**{i}. {color} {row['板块名称']}** <span style='color:{pct_color};font-weight:bold;'>{sign}{change:.2f}%</span>", unsafe_allow_html=True)
        else:
            st.info("暂无月度数据")

    # 月度跌幅榜
    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 📉 行业月度跌幅 TOP5")
        if monthly_industry is not None and len(monthly_industry) > 0:
            bottom5 = monthly_industry.nsmallest(5, '月涨跌幅')
            for i, (idx, row) in enumerate(bottom5.iterrows(), 1):
                change = row['月涨跌幅']
                color = "🔴" if change > 0 else "🟢"
                pct_color = "#FF0000" if change > 0 else "#00AA00"
                st.markdown(f"**{i}. {color} {row['板块名称']}** <span style='color:{pct_color};font-weight:bold;'>{change:.2f}%</span>", unsafe_allow_html=True)
        else:
            st.info("暂无数据")

    with col2:
        st.markdown("#### 📉 概念月度跌幅 TOP5")
        if monthly_concept is not None and len(monthly_concept) > 0:
            bottom5 = monthly_concept.nsmallest(5, '月涨跌幅')
            for i, (idx, row) in enumerate(bottom5.iterrows(), 1):
                change = row['月涨跌幅']
                color = "🔴" if change > 0 else "🟢"
                pct_color = "#FF0000" if change > 0 else "#00AA00"
                st.markdown(f"**{i}. {color} {row['板块名称']}** <span style='color:{pct_color};font-weight:bold;'>{change:.2f}%</span>", unsafe_allow_html=True)
        else:
            st.info("暂无数据")
