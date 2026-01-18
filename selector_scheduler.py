#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
选股策略后台调度器
支持多种选股策略在后台运行，用户可离开页面
"""

import threading
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
import traceback

from selector_task_db import selector_task_db


class SelectorScheduler:
    """通用选股策略后台调度器"""

    def __init__(self):
        self._task_locks: Dict[str, threading.Lock] = {}  # 每种选股类型一个锁
        self._cancel_events: Dict[str, threading.Event] = {}
        self._running_threads: Dict[str, threading.Thread] = {}
        print("[选股调度器] 初始化完成")

    def _get_lock(self, selector_type: str) -> threading.Lock:
        """获取指定选股类型的锁"""
        if selector_type not in self._task_locks:
            self._task_locks[selector_type] = threading.Lock()
        return self._task_locks[selector_type]

    def start_background_selection(
        self,
        selector_type: str,
        selection_func: Callable,
        params: Dict,
        on_complete: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """
        启动后台选股任务

        Args:
            selector_type: 选股类型 (low_price_bull, small_cap, profit_growth, sector_strategy)
            selection_func: 选股函数
            params: 选股参数
            on_complete: 完成回调函数

        Returns:
            {"task_id": str, "success": bool, "message": str}
        """
        lock = self._get_lock(selector_type)

        if not lock.acquire(blocking=False):
            return {
                "task_id": None,
                "success": False,
                "message": f"{selector_type} 已有任务在运行中"
            }

        try:
            # 检查是否有未完成的任务
            running_tasks = selector_task_db.get_running_tasks(selector_type)
            if running_tasks:
                lock.release()
                return {
                    "task_id": None,
                    "success": False,
                    "message": f"存在运行中的 {selector_type} 任务"
                }

            # 生成任务ID
            task_id = str(uuid.uuid4())

            # 创建取消事件
            cancel_event = threading.Event()
            self._cancel_events[task_id] = cancel_event

            # 创建任务记录
            if not selector_task_db.create_task(
                task_id=task_id,
                selector_type=selector_type,
                params=params
            ):
                lock.release()
                return {
                    "task_id": None,
                    "success": False,
                    "message": "创建任务失败"
                }

            # 启动后台线程
            thread = threading.Thread(
                target=self._run_selection,
                args=(task_id, selector_type, selection_func, params, cancel_event, on_complete, lock),
                daemon=True
            )
            self._running_threads[task_id] = thread
            thread.start()

            print(f"[选股调度器] 后台任务已启动: {selector_type} - {task_id[:8]}...")

            return {
                "task_id": task_id,
                "success": True,
                "message": f"{selector_type} 后台选股任务已启动"
            }

        except Exception as e:
            lock.release()
            return {
                "task_id": None,
                "success": False,
                "message": f"启动任务失败: {str(e)}"
            }

    def _run_selection(
        self,
        task_id: str,
        selector_type: str,
        selection_func: Callable,
        params: Dict,
        cancel_event: threading.Event,
        on_complete: Optional[Callable],
        lock: threading.Lock
    ):
        """执行后台选股任务"""
        try:
            # 更新状态为运行中
            selector_task_db.update_task_status(task_id, 'running')
            selector_task_db.update_task_progress(task_id, "正在获取数据", 10)

            print(f"[选股调度器] 开始执行: {selector_type}")

            # 检查是否被取消
            if cancel_event.is_set():
                selector_task_db.update_task_status(task_id, 'cancelled')
                return

            # 更新进度
            selector_task_db.update_task_progress(task_id, "正在筛选股票", 30)

            # 执行选股函数
            result = selection_func(**params)

            # 检查是否被取消
            if cancel_event.is_set():
                selector_task_db.update_task_status(task_id, 'cancelled')
                return

            # 更新进度
            selector_task_db.update_task_progress(task_id, "正在保存结果", 80)

            # 保存结果
            selector_task_db.save_task_result(task_id, result)

            # 更新状态
            if result.get('success'):
                selector_task_db.update_task_status(task_id, 'completed')
                selector_task_db.update_task_progress(task_id, "完成", 100)
                print(f"[选股调度器] 任务完成: {selector_type}")
            else:
                selector_task_db.update_task_status(
                    task_id, 'failed',
                    error_message=result.get('error', '未知错误')
                )
                print(f"[选股调度器] 任务失败: {selector_type} - {result.get('error')}")

            # 执行完成回调
            if on_complete:
                try:
                    on_complete(task_id, result)
                except Exception as e:
                    print(f"[选股调度器] 回调执行失败: {e}")

        except Exception as e:
            print(f"[选股调度器] 任务异常: {selector_type} - {e}")
            traceback.print_exc()
            selector_task_db.update_task_status(task_id, 'failed', error_message=str(e))
        finally:
            # 清理
            self._cleanup_task(task_id)
            lock.release()

    def get_task_status(self, task_id: str) -> Optional[Dict]:
        """获取任务状态"""
        return selector_task_db.get_task(task_id)

    def get_running_tasks(self, selector_type: Optional[str] = None) -> List[Dict]:
        """获取运行中的任务"""
        return selector_task_db.get_running_tasks(selector_type)

    def get_recent_tasks(self, selector_type: Optional[str] = None, limit: int = 20) -> List[Dict]:
        """获取最近的任务列表"""
        return selector_task_db.get_recent_tasks(selector_type, limit)

    def cancel_task(self, task_id: str) -> Dict[str, Any]:
        """取消任务"""
        if task_id in self._cancel_events:
            self._cancel_events[task_id].set()
            return {
                "success": True,
                "message": "已发送取消请求"
            }
        else:
            task = selector_task_db.get_task(task_id)
            if task and task['status'] in ('pending', 'running'):
                selector_task_db.update_task_status(task_id, 'cancelled')
                return {
                    "success": True,
                    "message": "任务已取消"
                }
            return {
                "success": False,
                "message": "任务不存在或已完成"
            }

    def _cleanup_task(self, task_id: str):
        """清理任务资源"""
        if task_id in self._cancel_events:
            del self._cancel_events[task_id]
        if task_id in self._running_threads:
            del self._running_threads[task_id]


# ==================== 选股函数封装 ====================

def run_low_price_bull_selection(top_n: int = 5, markets: List[str] = None) -> Dict:
    """执行低价擒牛选股"""
    try:
        from low_price_bull_selector import LowPriceBullSelector

        selector = LowPriceBullSelector()
        success, stocks_df, message = selector.get_low_price_stocks(
            top_n=top_n,
            markets=markets
        )

        if success and stocks_df is not None:
            return {
                "success": True,
                "stocks": stocks_df.to_dict('records'),
                "message": message,
                "count": len(stocks_df)
            }
        else:
            return {
                "success": False,
                "error": message
            }
    except Exception as e:
        return {"success": False, "error": str(e)}


def run_small_cap_selection(top_n: int = 5) -> Dict:
    """执行小市值策略选股"""
    try:
        from small_cap_selector import small_cap_selector

        success, stocks_df, message = small_cap_selector.get_small_cap_stocks(top_n=top_n)

        if success and stocks_df is not None:
            return {
                "success": True,
                "stocks": stocks_df.to_dict('records'),
                "message": message,
                "count": len(stocks_df)
            }
        else:
            return {
                "success": False,
                "error": message
            }
    except Exception as e:
        return {"success": False, "error": str(e)}


def run_profit_growth_selection(top_n: int = 5) -> Dict:
    """执行净利增长选股"""
    try:
        from profit_growth_selector import profit_growth_selector

        success, stocks_df, message = profit_growth_selector.get_profit_growth_stocks(top_n=top_n)

        if success and stocks_df is not None:
            return {
                "success": True,
                "stocks": stocks_df.to_dict('records'),
                "message": message,
                "count": len(stocks_df)
            }
        else:
            return {
                "success": False,
                "error": message
            }
    except Exception as e:
        return {"success": False, "error": str(e)}


def run_sector_strategy_analysis(selected_model: str = 'ep-20260110185620-6jfmf') -> Dict:
    """执行智策板块分析"""
    try:
        from sector_strategy import SectorStrategyEngine

        engine = SectorStrategyEngine(model=selected_model)
        result = engine.run_strategy()

        return result
    except Exception as e:
        return {"success": False, "error": str(e)}


def run_main_force_selection(
    start_date: str = None,
    days_ago: int = 5,
    final_n: int = 5,
    max_range_change: float = 50.0,
    min_market_cap: float = 0,
    max_market_cap: float = 10000,
    markets: List[str] = None,
    model: str = 'ep-20260110185620-6jfmf'
) -> Dict:
    """执行主力选股分析"""
    try:
        from main_force_analysis import MainForceAnalyzer

        analyzer = MainForceAnalyzer(model=model)
        result = analyzer.run_full_analysis(
            start_date=start_date,
            days_ago=days_ago,
            final_n=final_n,
            max_range_change=max_range_change,
            min_market_cap=min_market_cap,
            max_market_cap=max_market_cap,
            markets=markets
        )

        return result
    except Exception as e:
        return {"success": False, "error": str(e)}


# 全局实例
selector_scheduler = SelectorScheduler()
