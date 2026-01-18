"""
股票分析后台调度器
支持后台运行股票分析任务，用户可离开页面
"""

import threading
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any

from stock_analysis_task_db import stock_analysis_task_db


class StockAnalysisScheduler:
    """股票分析后台调度器"""

    def __init__(self):
        self._lock = threading.Lock()
        self._cancel_events: Dict[str, threading.Event] = {}
        self._running_threads: Dict[str, threading.Thread] = {}

    def start_background_analysis(
        self,
        symbol: str,
        period: str = "1y",
        enabled_analysts_config: Optional[Dict] = None,
        selected_model: str = "ep-20260110185620-6jfmf"
    ) -> Dict[str, Any]:
        """
        启动单只股票的后台分析

        Args:
            symbol: 股票代码
            period: 数据周期
            enabled_analysts_config: 分析师配置
            selected_model: AI模型

        Returns:
            {"task_id": str, "success": bool, "message": str}
        """
        task_id = str(uuid.uuid4())

        # 默认分析师配置
        if enabled_analysts_config is None:
            enabled_analysts_config = {
                'technical': True,
                'fundamental': True,
                'fund_flow': True,
                'risk': True,
                'sentiment': False,
                'news': False
            }

        config = {
            'enabled_analysts': enabled_analysts_config,
            'selected_model': selected_model
        }

        # 创建任务记录
        if not stock_analysis_task_db.create_task(
            task_id=task_id,
            task_type='single',
            symbols=[symbol],
            period=period,
            config=config
        ):
            return {
                "task_id": None,
                "success": False,
                "message": "创建任务失败"
            }

        # 创建取消事件
        cancel_event = threading.Event()
        self._cancel_events[task_id] = cancel_event

        # 启动后台线程
        thread = threading.Thread(
            target=self._run_single_analysis,
            args=(task_id, symbol, period, enabled_analysts_config, selected_model, cancel_event),
            daemon=True
        )
        self._running_threads[task_id] = thread
        thread.start()

        print(f"[Scheduler] 后台分析任务已启动: {task_id} - {symbol}")

        return {
            "task_id": task_id,
            "success": True,
            "message": f"后台分析任务已启动: {symbol}"
        }

    def start_batch_background_analysis(
        self,
        symbols: List[str],
        period: str = "1y",
        enabled_analysts_config: Optional[Dict] = None,
        selected_model: str = "ep-20260110185620-6jfmf",
        max_workers: int = 3
    ) -> Dict[str, Any]:
        """
        启动批量股票的后台分析

        Args:
            symbols: 股票代码列表
            period: 数据周期
            enabled_analysts_config: 分析师配置
            selected_model: AI模型
            max_workers: 最大并行数

        Returns:
            {"task_id": str, "success": bool, "message": str}
        """
        task_id = str(uuid.uuid4())

        # 默认分析师配置
        if enabled_analysts_config is None:
            enabled_analysts_config = {
                'technical': True,
                'fundamental': True,
                'fund_flow': True,
                'risk': True,
                'sentiment': False,
                'news': False
            }

        config = {
            'enabled_analysts': enabled_analysts_config,
            'selected_model': selected_model,
            'max_workers': max_workers
        }

        # 创建任务记录
        if not stock_analysis_task_db.create_task(
            task_id=task_id,
            task_type='batch',
            symbols=symbols,
            period=period,
            config=config
        ):
            return {
                "task_id": None,
                "success": False,
                "message": "创建任务失败"
            }

        # 创建取消事件
        cancel_event = threading.Event()
        self._cancel_events[task_id] = cancel_event

        # 启动后台线程
        thread = threading.Thread(
            target=self._run_batch_analysis,
            args=(task_id, symbols, period, enabled_analysts_config, selected_model, max_workers, cancel_event),
            daemon=True
        )
        self._running_threads[task_id] = thread
        thread.start()

        print(f"[Scheduler] 批量后台分析任务已启动: {task_id} - {len(symbols)}只股票")

        return {
            "task_id": task_id,
            "success": True,
            "message": f"批量后台分析任务已启动: {len(symbols)}只股票"
        }

    def _run_single_analysis(
        self,
        task_id: str,
        symbol: str,
        period: str,
        enabled_analysts_config: Dict,
        selected_model: str,
        cancel_event: threading.Event
    ):
        """执行单只股票分析（在后台线程中运行）"""
        try:
            # 更新状态为运行中
            stock_analysis_task_db.update_task_status(task_id, 'running')
            stock_analysis_task_db.update_task_progress(task_id, symbol, 0, 0)

            # 检查是否被取消
            if cancel_event.is_set():
                stock_analysis_task_db.update_task_status(task_id, 'cancelled')
                return

            # 导入分析函数（从独立模块导入，避免循环依赖和Streamlit冲突）
            from stock_analysis_core import analyze_single_stock

            print(f"[Scheduler] 开始分析: {symbol}")

            # 执行分析
            result = analyze_single_stock(
                symbol=symbol,
                period=period,
                enabled_analysts_config=enabled_analysts_config,
                selected_model=selected_model
            )

            # 检查是否被取消
            if cancel_event.is_set():
                stock_analysis_task_db.update_task_status(task_id, 'cancelled')
                return

            # 更新进度
            stock_analysis_task_db.update_task_progress(task_id, symbol, 1, 100)

            # 保存结果
            stock_analysis_task_db.save_task_result(task_id, result)

            # 更新状态
            if result.get('success'):
                stock_analysis_task_db.update_task_status(task_id, 'completed')
                print(f"[Scheduler] 分析完成: {symbol}")
            else:
                stock_analysis_task_db.update_task_status(
                    task_id, 'failed',
                    error_message=result.get('error', '未知错误')
                )
                print(f"[Scheduler] 分析失败: {symbol} - {result.get('error')}")

        except Exception as e:
            print(f"[Scheduler] 分析异常: {symbol} - {e}")
            stock_analysis_task_db.update_task_status(task_id, 'failed', error_message=str(e))
        finally:
            # 清理
            self._cleanup_task(task_id)

    def _run_batch_analysis(
        self,
        task_id: str,
        symbols: List[str],
        period: str,
        enabled_analysts_config: Dict,
        selected_model: str,
        max_workers: int,
        cancel_event: threading.Event
    ):
        """执行批量股票分析（在后台线程中运行）"""
        import concurrent.futures

        try:
            # 更新状态为运行中
            stock_analysis_task_db.update_task_status(task_id, 'running')

            # 导入分析函数（从独立模块导入，避免循环依赖和Streamlit冲突）
            from stock_analysis_core import analyze_single_stock

            results = []
            total = len(symbols)
            completed = 0

            print(f"[Scheduler] 开始批量分析: {total}只股票")

            # 使用线程池并行分析
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_symbol = {
                    executor.submit(
                        analyze_single_stock,
                        symbol, period, enabled_analysts_config, selected_model
                    ): symbol for symbol in symbols
                }

                for future in concurrent.futures.as_completed(future_to_symbol):
                    # 检查是否被取消
                    if cancel_event.is_set():
                        executor.shutdown(wait=False)
                        stock_analysis_task_db.update_task_status(task_id, 'cancelled')
                        return

                    symbol = future_to_symbol[future]
                    try:
                        result = future.result(timeout=300)
                        results.append(result)
                        completed += 1

                        # 更新进度
                        progress = (completed / total) * 100
                        stock_analysis_task_db.update_task_progress(
                            task_id, symbol, completed, progress
                        )

                        status = "成功" if result.get('success') else "失败"
                        print(f"[Scheduler] [{completed}/{total}] {symbol} 分析{status}")

                    except concurrent.futures.TimeoutError:
                        results.append({
                            "symbol": symbol,
                            "success": False,
                            "error": "分析超时"
                        })
                        completed += 1
                        stock_analysis_task_db.update_task_progress(
                            task_id, symbol, completed, (completed / total) * 100
                        )
                    except Exception as e:
                        results.append({
                            "symbol": symbol,
                            "success": False,
                            "error": str(e)
                        })
                        completed += 1
                        stock_analysis_task_db.update_task_progress(
                            task_id, symbol, completed, (completed / total) * 100
                        )

            # 保存结果
            stock_analysis_task_db.save_task_result(task_id, results)

            # 统计成功/失败数
            success_count = sum(1 for r in results if r.get('success'))
            failed_count = total - success_count

            if failed_count == 0:
                stock_analysis_task_db.update_task_status(task_id, 'completed')
            else:
                stock_analysis_task_db.update_task_status(
                    task_id, 'completed',
                    error_message=f"成功{success_count}个，失败{failed_count}个"
                )

            print(f"[Scheduler] 批量分析完成: 成功{success_count}个，失败{failed_count}个")

        except Exception as e:
            print(f"[Scheduler] 批量分析异常: {e}")
            stock_analysis_task_db.update_task_status(task_id, 'failed', error_message=str(e))
        finally:
            self._cleanup_task(task_id)

    def get_task_status(self, task_id: str) -> Optional[Dict]:
        """获取任务状态"""
        return stock_analysis_task_db.get_task(task_id)

    def get_running_tasks(self) -> List[Dict]:
        """获取所有运行中的任务"""
        return stock_analysis_task_db.get_running_tasks()

    def get_recent_tasks(self, limit: int = 20) -> List[Dict]:
        """获取最近的任务列表"""
        return stock_analysis_task_db.get_recent_tasks(limit)

    def cancel_task(self, task_id: str) -> Dict[str, Any]:
        """取消任务"""
        # 设置取消标志
        if task_id in self._cancel_events:
            self._cancel_events[task_id].set()
            return {
                "success": True,
                "message": "已发送取消请求"
            }
        else:
            # 任务可能已完成，直接更新状态
            task = stock_analysis_task_db.get_task(task_id)
            if task and task['status'] in ('pending', 'running'):
                stock_analysis_task_db.update_task_status(task_id, 'cancelled')
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


# 全局实例
stock_analysis_scheduler = StockAnalysisScheduler()
