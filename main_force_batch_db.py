#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主力选股批量分析历史记录数据库模块
"""

import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import pandas as pd

class MainForceBatchDatabase:
    """主力选股批量分析历史数据库管理类"""
    
    def __init__(self, db_path: str = "main_force_batch.db"):
        """初始化数据库连接"""
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """初始化数据库表结构"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 批量分析历史记录表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS batch_analysis_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                analysis_date TEXT NOT NULL,
                batch_count INTEGER NOT NULL,
                analysis_mode TEXT NOT NULL,
                success_count INTEGER NOT NULL,
                failed_count INTEGER NOT NULL,
                total_time REAL NOT NULL,
                results_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 创建索引
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_analysis_date
            ON batch_analysis_history(analysis_date)
        ''')

        # 后台任务状态表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS batch_task_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT UNIQUE NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                stock_codes TEXT NOT NULL,
                analysis_mode TEXT NOT NULL,
                max_workers INTEGER DEFAULT 3,
                total_count INTEGER NOT NULL,
                completed_count INTEGER DEFAULT 0,
                success_count INTEGER DEFAULT 0,
                failed_count INTEGER DEFAULT 0,
                current_stock TEXT,
                progress_percent REAL DEFAULT 0,
                error_message TEXT,
                history_record_id INTEGER,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_task_status
            ON batch_task_status(status)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_task_id
            ON batch_task_status(task_id)
        ''')

        conn.commit()
        conn.close()
    
    def _clean_results_for_json(self, results: List[Dict]) -> List[Dict]:
        """
        清理结果数据，确保可以JSON序列化
        
        Args:
            results: 原始结果列表
            
        Returns:
            清理后的结果列表
        """
        def clean_value(value):
            """递归清理值"""
            # 处理None
            if value is None:
                return None
            # 处理DataFrame - 只保留前100行避免数据过大
            elif isinstance(value, pd.DataFrame):
                if len(value) > 100:
                    return value.head(100).to_dict('records')
                return value.to_dict('records')
            # 处理Series
            elif isinstance(value, pd.Series):
                return value.to_dict()
            # 处理字典 - 递归清理
            elif isinstance(value, dict):
                return {k: clean_value(v) for k, v in value.items()}
            # 处理列表 - 递归清理
            elif isinstance(value, (list, tuple)):
                return [clean_value(v) for v in value]
            # 处理基本类型
            elif isinstance(value, (str, int, float, bool)):
                return value
            # 其他对象转为字符串
            else:
                try:
                    return str(value)
                except:
                    return "无法序列化"
        
        cleaned = []
        for result in results:
            try:
                cleaned_result = {}
                for key, value in result.items():
                    cleaned_result[key] = clean_value(value)
                cleaned.append(cleaned_result)
            except Exception as e:
                # 如果单个结果清理失败，记录错误
                cleaned.append({
                    "error": f"清理失败: {str(e)}",
                    "original_keys": list(result.keys()) if isinstance(result, dict) else []
                })
        return cleaned
    
    def save_batch_analysis(
        self,
        batch_count: int,
        analysis_mode: str,
        success_count: int,
        failed_count: int,
        total_time: float,
        results: List[Dict]
    ) -> int:
        """
        保存批量分析结果
        
        Args:
            batch_count: 分析股票数量
            analysis_mode: 分析模式（sequential/parallel）
            success_count: 成功数量
            failed_count: 失败数量
            total_time: 总耗时（秒）
            results: 分析结果列表
            
        Returns:
            记录ID
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        analysis_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 清理结果数据，确保可以JSON序列化
        cleaned_results = self._clean_results_for_json(results)
        results_json = json.dumps(cleaned_results, ensure_ascii=False, default=str)
        
        cursor.execute('''
            INSERT INTO batch_analysis_history 
            (analysis_date, batch_count, analysis_mode, success_count, failed_count, total_time, results_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (analysis_date, batch_count, analysis_mode, success_count, failed_count, total_time, results_json))
        
        record_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return record_id
    
    def get_all_history(self, limit: int = 50) -> List[Dict]:
        """
        获取所有历史记录
        
        Args:
            limit: 返回记录数量限制
            
        Returns:
            历史记录列表
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, analysis_date, batch_count, analysis_mode, 
                   success_count, failed_count, total_time, results_json, created_at
            FROM batch_analysis_history
            ORDER BY created_at DESC
            LIMIT ?
        ''', (limit,))
        
        rows = cursor.fetchall()
        conn.close()
        
        history = []
        for row in rows:
            try:
                results = json.loads(row[7])
            except:
                results = []
            
            history.append({
                'id': row[0],
                'analysis_date': row[1],
                'batch_count': row[2],
                'analysis_mode': row[3],
                'success_count': row[4],
                'failed_count': row[5],
                'total_time': row[6],
                'results': results,
                'created_at': row[8]
            })
        
        return history
    
    def get_record_by_id(self, record_id: int) -> Optional[Dict]:
        """
        根据ID获取单条记录
        
        Args:
            record_id: 记录ID
            
        Returns:
            记录详情
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, analysis_date, batch_count, analysis_mode, 
                   success_count, failed_count, total_time, results_json, created_at
            FROM batch_analysis_history
            WHERE id = ?
        ''', (record_id,))
        
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return None
        
        try:
            results = json.loads(row[7])
        except:
            results = []
        
        return {
            'id': row[0],
            'analysis_date': row[1],
            'batch_count': row[2],
            'analysis_mode': row[3],
            'success_count': row[4],
            'failed_count': row[5],
            'total_time': row[6],
            'results': results,
            'created_at': row[8]
        }
    
    def delete_record(self, record_id: int) -> bool:
        """
        删除记录
        
        Args:
            record_id: 记录ID
            
        Returns:
            是否删除成功
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM batch_analysis_history WHERE id = ?', (record_id,))
        
        affected_rows = cursor.rowcount
        conn.commit()
        conn.close()
        
        return affected_rows > 0
    
    def get_statistics(self) -> Dict:
        """
        获取统计信息

        Returns:
            统计数据
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 总记录数
        cursor.execute('SELECT COUNT(*) FROM batch_analysis_history')
        total_records = cursor.fetchone()[0]

        # 总分析股票数
        cursor.execute('SELECT SUM(batch_count) FROM batch_analysis_history')
        total_stocks = cursor.fetchone()[0] or 0

        # 总成功数
        cursor.execute('SELECT SUM(success_count) FROM batch_analysis_history')
        total_success = cursor.fetchone()[0] or 0

        # 总失败数
        cursor.execute('SELECT SUM(failed_count) FROM batch_analysis_history')
        total_failed = cursor.fetchone()[0] or 0

        # 平均耗时
        cursor.execute('SELECT AVG(total_time) FROM batch_analysis_history')
        avg_time = cursor.fetchone()[0] or 0

        conn.close()

        return {
            'total_records': total_records,
            'total_stocks_analyzed': total_stocks,
            'total_success': total_success,
            'total_failed': total_failed,
            'average_time': round(avg_time, 2),
            'success_rate': round(total_success / total_stocks * 100, 2) if total_stocks > 0 else 0
        }

    # ==================== 后台任务管理方法 ====================

    def create_task(
        self,
        task_id: str,
        stock_codes: List[str],
        analysis_mode: str,
        max_workers: int = 3
    ) -> int:
        """
        创建新的后台任务记录

        Args:
            task_id: 任务UUID
            stock_codes: 股票代码列表
            analysis_mode: 分析模式 (sequential/parallel)
            max_workers: 并行线程数

        Returns:
            记录ID
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO batch_task_status
            (task_id, stock_codes, analysis_mode, max_workers, total_count, status)
            VALUES (?, ?, ?, ?, ?, 'pending')
        ''', (task_id, json.dumps(stock_codes), analysis_mode, max_workers, len(stock_codes)))

        record_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return record_id

    def update_task_status(
        self,
        task_id: str,
        status: str,
        error_message: Optional[str] = None,
        history_record_id: Optional[int] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None
    ):
        """
        更新任务状态

        Args:
            task_id: 任务ID
            status: 新状态 (pending/running/completed/failed/cancelled)
            error_message: 错误信息
            history_record_id: 关联的历史记录ID
            started_at: 开始时间
            completed_at: 完成时间
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        updates = ["status = ?"]
        values = [status]

        if error_message is not None:
            updates.append("error_message = ?")
            values.append(error_message)
        if history_record_id is not None:
            updates.append("history_record_id = ?")
            values.append(history_record_id)
        if started_at is not None:
            updates.append("started_at = ?")
            values.append(started_at.strftime("%Y-%m-%d %H:%M:%S"))
        if completed_at is not None:
            updates.append("completed_at = ?")
            values.append(completed_at.strftime("%Y-%m-%d %H:%M:%S"))

        values.append(task_id)

        cursor.execute(f'''
            UPDATE batch_task_status
            SET {", ".join(updates)}
            WHERE task_id = ?
        ''', values)

        conn.commit()
        conn.close()

    def update_task_progress(
        self,
        task_id: str,
        completed_count: int,
        current_stock: Optional[str] = None,
        success_count: Optional[int] = None,
        failed_count: Optional[int] = None,
        progress_percent: Optional[float] = None
    ):
        """
        更新任务进度

        Args:
            task_id: 任务ID
            completed_count: 已完成数量
            current_stock: 当前分析的股票
            success_count: 成功数量
            failed_count: 失败数量
            progress_percent: 进度百分比
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        updates = ["completed_count = ?"]
        values = [completed_count]

        if current_stock is not None:
            updates.append("current_stock = ?")
            values.append(current_stock)
        if success_count is not None:
            updates.append("success_count = ?")
            values.append(success_count)
        if failed_count is not None:
            updates.append("failed_count = ?")
            values.append(failed_count)
        if progress_percent is not None:
            updates.append("progress_percent = ?")
            values.append(progress_percent)

        values.append(task_id)

        cursor.execute(f'''
            UPDATE batch_task_status
            SET {", ".join(updates)}
            WHERE task_id = ?
        ''', values)

        conn.commit()
        conn.close()

    def get_task_by_id(self, task_id: str) -> Optional[Dict]:
        """
        根据ID获取任务

        Args:
            task_id: 任务ID

        Returns:
            任务信息字典
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM batch_task_status WHERE task_id = ?
        ''', (task_id,))

        row = cursor.fetchone()
        description = cursor.description
        conn.close()

        if not row:
            return None

        return self._row_to_task_dict(row, description)

    def get_running_task(self) -> Optional[Dict]:
        """
        获取正在运行的任务

        Returns:
            任务信息字典，如果没有运行中的任务返回None
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM batch_task_status
            WHERE status IN ('pending', 'running')
            ORDER BY created_at DESC
            LIMIT 1
        ''')

        row = cursor.fetchone()
        description = cursor.description
        conn.close()

        if not row:
            return None

        return self._row_to_task_dict(row, description)

    def get_latest_completed_task(self) -> Optional[Dict]:
        """
        获取最近完成的任务

        Returns:
            任务信息字典
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM batch_task_status
            WHERE status = 'completed'
            ORDER BY completed_at DESC
            LIMIT 1
        ''')

        row = cursor.fetchone()
        description = cursor.description
        conn.close()

        if not row:
            return None

        return self._row_to_task_dict(row, description)

    def _row_to_task_dict(self, row, description) -> Dict:
        """
        将数据库行转换为字典

        Args:
            row: 数据库行
            description: 列描述

        Returns:
            任务信息字典
        """
        columns = [col[0] for col in description]
        task = dict(zip(columns, row))

        # 解析 JSON 字段
        if task.get('stock_codes'):
            try:
                task['stock_codes'] = json.loads(task['stock_codes'])
            except:
                task['stock_codes'] = []

        return task


# 全局数据库实例
batch_db = MainForceBatchDatabase()

