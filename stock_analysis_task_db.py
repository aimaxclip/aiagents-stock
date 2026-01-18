"""
股票分析任务数据库模块
用于存储后台分析任务的状态和结果
"""

import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
import pandas as pd


class StockAnalysisTaskDB:
    """股票分析任务数据库"""

    def __init__(self, db_path: str = "stock_analysis_tasks.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """初始化数据库表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 创建任务状态表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analysis_tasks (
                task_id TEXT PRIMARY KEY,
                task_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                symbols TEXT NOT NULL,
                period TEXT NOT NULL,
                config TEXT,
                total_count INTEGER DEFAULT 1,
                completed_count INTEGER DEFAULT 0,
                current_symbol TEXT,
                progress_percent REAL DEFAULT 0,
                results TEXT,
                error_message TEXT,
                started_at TEXT,
                completed_at TEXT,
                created_at TEXT NOT NULL
            )
        ''')

        # 创建索引
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_status ON analysis_tasks(status)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_created_at ON analysis_tasks(created_at)
        ''')

        conn.commit()
        conn.close()

    def _get_conn(self):
        """获取数据库连接"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _clean_for_json(self, obj: Any) -> Any:
        """递归清理对象使其可JSON序列化"""
        if obj is None:
            return None
        if isinstance(obj, (str, int, float, bool)):
            return obj
        if isinstance(obj, pd.DataFrame):
            if len(obj) > 100:
                return obj.head(100).to_dict('records')
            return obj.to_dict('records')
        if isinstance(obj, pd.Series):
            return obj.to_dict()
        if isinstance(obj, dict):
            return {k: self._clean_for_json(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self._clean_for_json(item) for item in obj]
        # 其他类型转字符串
        try:
            return str(obj)
        except:
            return None

    def create_task(self, task_id: str, task_type: str, symbols: List[str],
                    period: str, config: Optional[Dict] = None) -> bool:
        """创建新任务"""
        try:
            conn = self._get_conn()
            cursor = conn.cursor()

            cursor.execute('''
                INSERT INTO analysis_tasks
                (task_id, task_type, status, symbols, period, config,
                 total_count, created_at)
                VALUES (?, ?, 'pending', ?, ?, ?, ?, ?)
            ''', (
                task_id,
                task_type,
                json.dumps(symbols, ensure_ascii=False),
                period,
                json.dumps(config, ensure_ascii=False) if config else None,
                len(symbols),
                datetime.now().isoformat()
            ))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"[TaskDB] 创建任务失败: {e}")
            return False

    def update_task_status(self, task_id: str, status: str,
                           error_message: Optional[str] = None) -> bool:
        """更新任务状态"""
        try:
            conn = self._get_conn()
            cursor = conn.cursor()

            updates = ["status = ?"]
            params = [status]

            if status == 'running':
                updates.append("started_at = ?")
                params.append(datetime.now().isoformat())
            elif status in ('completed', 'failed', 'cancelled'):
                updates.append("completed_at = ?")
                params.append(datetime.now().isoformat())

            if error_message:
                updates.append("error_message = ?")
                params.append(error_message)

            params.append(task_id)

            cursor.execute(f'''
                UPDATE analysis_tasks
                SET {", ".join(updates)}
                WHERE task_id = ?
            ''', params)

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"[TaskDB] 更新任务状态失败: {e}")
            return False

    def update_task_progress(self, task_id: str, current_symbol: str,
                             completed_count: int, progress_percent: float) -> bool:
        """更新任务进度"""
        try:
            conn = self._get_conn()
            cursor = conn.cursor()

            cursor.execute('''
                UPDATE analysis_tasks
                SET current_symbol = ?, completed_count = ?, progress_percent = ?
                WHERE task_id = ?
            ''', (current_symbol, completed_count, progress_percent, task_id))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"[TaskDB] 更新任务进度失败: {e}")
            return False

    def save_task_result(self, task_id: str, results: Any) -> bool:
        """保存任务结果"""
        try:
            conn = self._get_conn()
            cursor = conn.cursor()

            # 清理结果数据
            cleaned_results = self._clean_for_json(results)
            results_json = json.dumps(cleaned_results, ensure_ascii=False, default=str)

            cursor.execute('''
                UPDATE analysis_tasks
                SET results = ?
                WHERE task_id = ?
            ''', (results_json, task_id))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"[TaskDB] 保存任务结果失败: {e}")
            return False

    def get_task(self, task_id: str) -> Optional[Dict]:
        """获取任务详情"""
        try:
            conn = self._get_conn()
            cursor = conn.cursor()

            cursor.execute('''
                SELECT * FROM analysis_tasks WHERE task_id = ?
            ''', (task_id,))

            row = cursor.fetchone()
            conn.close()

            if row:
                return self._row_to_dict(row)
            return None
        except Exception as e:
            print(f"[TaskDB] 获取任务失败: {e}")
            return None

    def get_running_tasks(self) -> List[Dict]:
        """获取所有运行中的任务"""
        try:
            conn = self._get_conn()
            cursor = conn.cursor()

            cursor.execute('''
                SELECT * FROM analysis_tasks
                WHERE status IN ('pending', 'running')
                ORDER BY created_at DESC
            ''')

            rows = cursor.fetchall()
            conn.close()

            return [self._row_to_dict(row) for row in rows]
        except Exception as e:
            print(f"[TaskDB] 获取运行中任务失败: {e}")
            return []

    def get_recent_tasks(self, limit: int = 20) -> List[Dict]:
        """获取最近的任务列表"""
        try:
            conn = self._get_conn()
            cursor = conn.cursor()

            cursor.execute('''
                SELECT * FROM analysis_tasks
                ORDER BY created_at DESC
                LIMIT ?
            ''', (limit,))

            rows = cursor.fetchall()
            conn.close()

            return [self._row_to_dict(row) for row in rows]
        except Exception as e:
            print(f"[TaskDB] 获取最近任务失败: {e}")
            return []

    def delete_task(self, task_id: str) -> bool:
        """删除任务"""
        try:
            conn = self._get_conn()
            cursor = conn.cursor()

            cursor.execute('''
                DELETE FROM analysis_tasks WHERE task_id = ?
            ''', (task_id,))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"[TaskDB] 删除任务失败: {e}")
            return False

    def cleanup_old_tasks(self, days: int = 7) -> int:
        """清理指定天数前的已完成任务"""
        try:
            conn = self._get_conn()
            cursor = conn.cursor()

            cursor.execute('''
                DELETE FROM analysis_tasks
                WHERE status IN ('completed', 'failed', 'cancelled')
                AND datetime(created_at) < datetime('now', ?)
            ''', (f'-{days} days',))

            deleted_count = cursor.rowcount
            conn.commit()
            conn.close()
            return deleted_count
        except Exception as e:
            print(f"[TaskDB] 清理旧任务失败: {e}")
            return 0

    def _row_to_dict(self, row: sqlite3.Row) -> Dict:
        """将数据库行转换为字典"""
        d = dict(row)

        # 解析JSON字段
        if d.get('symbols'):
            try:
                d['symbols'] = json.loads(d['symbols'])
            except:
                pass

        if d.get('config'):
            try:
                d['config'] = json.loads(d['config'])
            except:
                pass

        if d.get('results'):
            try:
                d['results'] = json.loads(d['results'])
            except:
                pass

        return d


# 全局实例
stock_analysis_task_db = StockAnalysisTaskDB()
