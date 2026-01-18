#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主力选股批量分析后台调度器
支持后台运行批量分析任务，离开页面不中断
"""

import threading
import time
import uuid
from datetime import datetime
from typing import Optional, Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback

from main_force_batch_db import batch_db


class MainForceBatchScheduler:
    """主力选股批量分析后台调度器"""

    def __init__(self):
        self._task_lock = threading.Lock()  # 防止同时启动多个任务
        self._cancel_flag = threading.Event()  # 取消标志
        self._current_task_id: Optional[str] = None
        self._thread: Optional[threading.Thread] = None
        print("[主力批量分析] 调度器初始化完成")

    def start_batch_analysis(
        self,
        stock_codes: List[str],
        analysis_mode: str = "parallel",
        max_workers: int = 3,
        enabled_analysts_config: Optional[Dict] = None,
        selected_model: str = 'deepseek-chat'
    ) -> Dict:
        """
        启动后台批量分析任务

        Args:
            stock_codes: 股票代码列表
            analysis_mode: 分析模式 (sequential/parallel)
            max_workers: 并行线程数
            enabled_analysts_config: 分析师配置
            selected_model: AI模型

        Returns:
            包含 task_id 和状态的字典
        """
        # 尝试获取锁，检查是否有任务在运行
        if not self._task_lock.acquire(blocking=False):
            return {
                "success": False,
                "error": "已有批量分析任务在运行中，请等待完成后再试"
            }

        try:
            # 检查是否有未完成的任务
            running_task = batch_db.get_running_task()
            if running_task:
                self._task_lock.release()
                return {
                    "success": False,
                    "error": f"存在运行中的任务 (ID: {running_task['task_id'][:8]}...)，请等待完成"
                }

            # 生成任务ID
            task_id = str(uuid.uuid4())
            self._current_task_id = task_id
            self._cancel_flag.clear()

            # 创建任务记录
            batch_db.create_task(
                task_id=task_id,
                stock_codes=stock_codes,
                analysis_mode=analysis_mode,
                max_workers=max_workers
            )

            # 在后台线程中启动分析
            self._thread = threading.Thread(
                target=self._run_analysis,
                args=(task_id, stock_codes, analysis_mode, max_workers,
                      enabled_analysts_config, selected_model),
                daemon=True
            )
            self._thread.start()

            return {
                "success": True,
                "task_id": task_id,
                "message": f"后台任务已启动，共 {len(stock_codes)} 只股票"
            }

        except Exception as e:
            self._task_lock.release()
            return {
                "success": False,
                "error": f"启动任务失败: {str(e)}"
            }

    def _run_analysis(
        self,
        task_id: str,
        stock_codes: List[str],
        analysis_mode: str,
        max_workers: int,
        enabled_analysts_config: Optional[Dict],
        selected_model: str
    ):
        """后台执行批量分析"""
        print(f"\n{'='*60}")
        print(f"[主力批量分析] 后台任务启动: {task_id[:8]}...")
        print(f"[主力批量分析] 股票数量: {len(stock_codes)}")
        print(f"[主力批量分析] 分析模式: {analysis_mode}")
        print(f"{'='*60}\n")

        start_time = time.time()
        results = []

        try:
            # 更新任务状态为运行中
            batch_db.update_task_status(task_id, 'running', started_at=datetime.now())

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

            total = len(stock_codes)

            if analysis_mode == "sequential":
                # 顺序分析
                for i, code in enumerate(stock_codes):
                    if self._cancel_flag.is_set():
                        print(f"[主力批量分析] 任务被取消")
                        break

                    # 更新当前分析的股票
                    batch_db.update_task_progress(
                        task_id,
                        completed_count=i,
                        current_stock=code,
                        progress_percent=(i / total) * 100
                    )

                    result = self._analyze_single(code, enabled_analysts_config, selected_model)
                    results.append(result)

                    # 更新进度
                    batch_db.update_task_progress(
                        task_id,
                        completed_count=i + 1,
                        success_count=sum(1 for r in results if r.get('success')),
                        failed_count=sum(1 for r in results if not r.get('success')),
                        progress_percent=((i + 1) / total) * 100
                    )
            else:
                # 并行分析
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(
                            self._analyze_single, code, enabled_analysts_config, selected_model
                        ): code for code in stock_codes
                    }

                    completed = 0
                    for future in as_completed(futures):
                        if self._cancel_flag.is_set():
                            # 取消剩余任务
                            for f in futures:
                                f.cancel()
                            print(f"[主力批量分析] 任务被取消")
                            break

                        code = futures[future]
                        completed += 1

                        try:
                            result = future.result()
                        except Exception as e:
                            result = {"symbol": code, "success": False, "error": str(e)}

                        results.append(result)

                        # 更新进度
                        batch_db.update_task_progress(
                            task_id,
                            completed_count=completed,
                            current_stock=code,
                            success_count=sum(1 for r in results if r.get('success')),
                            failed_count=sum(1 for r in results if not r.get('success')),
                            progress_percent=(completed / total) * 100
                        )

            # 计算统计
            elapsed_time = time.time() - start_time
            success_count = sum(1 for r in results if r.get('success'))
            failed_count = len(results) - success_count

            # 保存到历史记录
            if not self._cancel_flag.is_set():
                history_id = batch_db.save_batch_analysis(
                    batch_count=len(stock_codes),
                    analysis_mode=analysis_mode,
                    success_count=success_count,
                    failed_count=failed_count,
                    total_time=elapsed_time,
                    results=results
                )

                # 更新任务状态为完成
                batch_db.update_task_status(
                    task_id,
                    'completed',
                    completed_at=datetime.now(),
                    history_record_id=history_id
                )

                print(f"\n{'='*60}")
                print(f"[主力批量分析] 任务完成!")
                print(f"[主力批量分析] 成功: {success_count}, 失败: {failed_count}")
                print(f"[主力批量分析] 耗时: {elapsed_time/60:.1f} 分钟")
                print(f"[主力批量分析] 历史记录ID: {history_id}")
                print(f"{'='*60}\n")
            else:
                batch_db.update_task_status(task_id, 'cancelled', completed_at=datetime.now())

        except Exception as e:
            error_msg = str(e)
            print(f"[主力批量分析] 任务执行异常: {error_msg}")
            traceback.print_exc()
            batch_db.update_task_status(
                task_id, 'failed',
                error_message=error_msg,
                completed_at=datetime.now()
            )
        finally:
            self._current_task_id = None
            self._task_lock.release()

    def _analyze_single(
        self,
        code: str,
        enabled_analysts_config: Dict,
        selected_model: str
    ) -> Dict:
        """分析单只股票"""
        try:
            # 延迟导入，避免循环导入
            from app import analyze_single_stock_for_batch

            print(f"  [分析中] {code}")
            result = analyze_single_stock_for_batch(
                symbol=code,
                period='1y',
                enabled_analysts_config=enabled_analysts_config,
                selected_model=selected_model
            )
            print(f"  [完成] {code} - {'成功' if result.get('success') else '失败'}")
            return result
        except Exception as e:
            print(f"  [失败] {code} - {str(e)}")
            return {"symbol": code, "success": False, "error": str(e)}

    def cancel_task(self, task_id: Optional[str] = None) -> Dict:
        """
        取消当前运行的任务

        Args:
            task_id: 任务ID（可选，不指定则取消当前任务）

        Returns:
            操作结果
        """
        if task_id and self._current_task_id != task_id:
            return {"success": False, "error": "任务ID不匹配"}

        if not self._current_task_id:
            return {"success": False, "error": "没有运行中的任务"}

        self._cancel_flag.set()
        return {"success": True, "message": "已发送取消请求，任务将在当前股票分析完成后停止"}

    def get_task_status(self, task_id: Optional[str] = None) -> Optional[Dict]:
        """
        获取任务状态

        Args:
            task_id: 任务ID（可选，不指定则获取当前/最近任务）

        Returns:
            任务状态字典
        """
        if task_id:
            return batch_db.get_task_by_id(task_id)
        elif self._current_task_id:
            return batch_db.get_task_by_id(self._current_task_id)
        else:
            return batch_db.get_running_task()

    def get_latest_completed_task(self) -> Optional[Dict]:
        """获取最近完成的任务"""
        return batch_db.get_latest_completed_task()

    def is_running(self) -> bool:
        """检查是否有任务在运行"""
        return self._current_task_id is not None


# 创建全局实例
main_force_batch_scheduler = MainForceBatchScheduler()
