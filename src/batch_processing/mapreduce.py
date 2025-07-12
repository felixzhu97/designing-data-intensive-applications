"""
MapReduce批处理系统
展示《数据密集型应用系统设计》中的MapReduce概念
"""

import threading
import time
from typing import Dict, List, Any, Optional, Callable, Iterator, Tuple
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import hashlib


@dataclass
class MapReduceJob:
    """MapReduce作业"""
    job_id: str
    job_name: str
    input_data: List[Any]
    map_function: Callable[[Any], List[Tuple[Any, Any]]]
    reduce_function: Callable[[Any, List[Any]], Any]
    combiner_function: Optional[Callable[[Any, List[Any]], Any]] = None
    num_reducers: int = 4
    created_at: datetime = field(default_factory=datetime.utcnow)
    status: str = "PENDING"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'job_id': self.job_id,
            'job_name': self.job_name,
            'input_size': len(self.input_data),
            'num_reducers': self.num_reducers,
            'created_at': self.created_at.isoformat(),
            'status': self.status
        }


@dataclass
class MapTask:
    """Map任务"""
    task_id: str
    job_id: str
    input_data: List[Any]
    map_function: Callable[[Any], List[Tuple[Any, Any]]]
    combiner_function: Optional[Callable[[Any, List[Any]], Any]] = None
    status: str = "PENDING"
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    output: Optional[Dict[Any, List[Any]]] = None
    
    def execute(self) -> Dict[Any, List[Any]]:
        """执行Map任务"""
        self.status = "RUNNING"
        self.start_time = datetime.utcnow()
        
        try:
            # 执行Map函数
            intermediate_results = defaultdict(list)
            
            for item in self.input_data:
                key_value_pairs = self.map_function(item)
                for key, value in key_value_pairs:
                    intermediate_results[key].append(value)
            
            # 如果有Combiner，执行本地聚合
            if self.combiner_function:
                combined_results = {}
                for key, values in intermediate_results.items():
                    combined_results[key] = [self.combiner_function(key, values)]
                intermediate_results = combined_results
            
            self.output = dict(intermediate_results)
            self.status = "COMPLETED"
            self.end_time = datetime.utcnow()
            
            return self.output
            
        except Exception as e:
            self.status = "FAILED"
            self.end_time = datetime.utcnow()
            raise e
    
    def get_stats(self) -> Dict[str, Any]:
        """获取任务统计信息"""
        duration = None
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        
        return {
            'task_id': self.task_id,
            'job_id': self.job_id,
            'status': self.status,
            'input_size': len(self.input_data),
            'output_keys': len(self.output) if self.output else 0,
            'duration': duration,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None
        }


@dataclass
class ReduceTask:
    """Reduce任务"""
    task_id: str
    job_id: str
    partition_id: int
    input_data: Dict[Any, List[Any]]
    reduce_function: Callable[[Any, List[Any]], Any]
    status: str = "PENDING"
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    output: Optional[Dict[Any, Any]] = None
    
    def execute(self) -> Dict[Any, Any]:
        """执行Reduce任务"""
        self.status = "RUNNING"
        self.start_time = datetime.utcnow()
        
        try:
            results = {}
            
            for key, values in self.input_data.items():
                # 对每个键执行Reduce函数
                reduced_value = self.reduce_function(key, values)
                results[key] = reduced_value
            
            self.output = results
            self.status = "COMPLETED"
            self.end_time = datetime.utcnow()
            
            return self.output
            
        except Exception as e:
            self.status = "FAILED"
            self.end_time = datetime.utcnow()
            raise e
    
    def get_stats(self) -> Dict[str, Any]:
        """获取任务统计信息"""
        duration = None
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        
        return {
            'task_id': self.task_id,
            'job_id': self.job_id,
            'partition_id': self.partition_id,
            'status': self.status,
            'input_keys': len(self.input_data),
            'output_keys': len(self.output) if self.output else 0,
            'duration': duration,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None
        }


class Combiner:
    """组合器"""
    
    @staticmethod
    def sum_combiner(key: Any, values: List[Any]) -> Any:
        """求和组合器"""
        return sum(values)
    
    @staticmethod
    def count_combiner(key: Any, values: List[Any]) -> Any:
        """计数组合器"""
        return len(values)
    
    @staticmethod
    def max_combiner(key: Any, values: List[Any]) -> Any:
        """最大值组合器"""
        return max(values)
    
    @staticmethod
    def min_combiner(key: Any, values: List[Any]) -> Any:
        """最小值组合器"""
        return min(values)
    
    @staticmethod
    def avg_combiner(key: Any, values: List[Any]) -> Any:
        """平均值组合器"""
        return sum(values) / len(values)


class MapReduceExecutor:
    """MapReduce执行器"""
    
    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self.jobs: Dict[str, MapReduceJob] = {}
        self.job_results: Dict[str, Dict[Any, Any]] = {}
        self.lock = threading.RLock()
    
    def submit_job(self, job: MapReduceJob) -> str:
        """提交作业"""
        with self.lock:
            self.jobs[job.job_id] = job
            job.status = "SUBMITTED"
            return job.job_id
    
    def execute_job(self, job_id: str) -> Dict[Any, Any]:
        """执行作业"""
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                raise ValueError(f"Job {job_id} not found")
            
            if job.status != "SUBMITTED":
                raise ValueError(f"Job {job_id} is not in SUBMITTED state")
            
            job.status = "RUNNING"
        
        try:
            # 第一阶段：Map
            map_results = self._execute_map_phase(job)
            
            # 第二阶段：Shuffle & Sort
            shuffled_data = self._shuffle_and_sort(map_results, job.num_reducers)
            
            # 第三阶段：Reduce
            reduce_results = self._execute_reduce_phase(job, shuffled_data)
            
            # 合并结果
            final_results = {}
            for result in reduce_results:
                final_results.update(result)
            
            with self.lock:
                job.status = "COMPLETED"
                self.job_results[job_id] = final_results
            
            return final_results
            
        except Exception as e:
            with self.lock:
                job.status = "FAILED"
            raise e
    
    def _execute_map_phase(self, job: MapReduceJob) -> List[Dict[Any, List[Any]]]:
        """执行Map阶段"""
        # 将输入数据分割成多个块
        chunk_size = max(1, len(job.input_data) // self.max_workers)
        chunks = [
            job.input_data[i:i + chunk_size] 
            for i in range(0, len(job.input_data), chunk_size)
        ]
        
        # 创建Map任务
        map_tasks = []
        for i, chunk in enumerate(chunks):
            task = MapTask(
                task_id=f"{job.job_id}_map_{i}",
                job_id=job.job_id,
                input_data=chunk,
                map_function=job.map_function,
                combiner_function=job.combiner_function
            )
            map_tasks.append(task)
        
        # 并行执行Map任务
        map_results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_task = {executor.submit(task.execute): task for task in map_tasks}
            
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    map_results.append(result)
                except Exception as e:
                    print(f"Map task {task.task_id} failed: {e}")
                    raise e
        
        return map_results
    
    def _shuffle_and_sort(self, map_results: List[Dict[Any, List[Any]]], num_reducers: int) -> List[Dict[Any, List[Any]]]:
        """Shuffle和Sort阶段"""
        # 将Map输出按键分区
        partitioned_data = [defaultdict(list) for _ in range(num_reducers)]
        
        for map_result in map_results:
            for key, values in map_result.items():
                # 使用哈希函数确定分区
                partition_id = self._partition_key(key, num_reducers)
                partitioned_data[partition_id][key].extend(values)
        
        # 对每个分区内的数据进行排序
        sorted_partitions = []
        for partition_data in partitioned_data:
            sorted_partition = {}
            for key in sorted(partition_data.keys()):
                sorted_partition[key] = partition_data[key]
            sorted_partitions.append(sorted_partition)
        
        return sorted_partitions
    
    def _partition_key(self, key: Any, num_reducers: int) -> int:
        """分区函数"""
        # 使用哈希函数将键分配到不同的分区
        key_str = str(key)
        hash_value = int(hashlib.md5(key_str.encode()).hexdigest(), 16)
        return hash_value % num_reducers
    
    def _execute_reduce_phase(self, job: MapReduceJob, shuffled_data: List[Dict[Any, List[Any]]]) -> List[Dict[Any, Any]]:
        """执行Reduce阶段"""
        # 创建Reduce任务
        reduce_tasks = []
        for i, partition_data in enumerate(shuffled_data):
            if partition_data:  # 只为非空分区创建任务
                task = ReduceTask(
                    task_id=f"{job.job_id}_reduce_{i}",
                    job_id=job.job_id,
                    partition_id=i,
                    input_data=partition_data,
                    reduce_function=job.reduce_function
                )
                reduce_tasks.append(task)
        
        # 并行执行Reduce任务
        reduce_results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_task = {executor.submit(task.execute): task for task in reduce_tasks}
            
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    reduce_results.append(result)
                except Exception as e:
                    print(f"Reduce task {task.task_id} failed: {e}")
                    raise e
        
        return reduce_results
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """获取作业状态"""
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return None
            
            return job.to_dict()
    
    def get_job_result(self, job_id: str) -> Optional[Dict[Any, Any]]:
        """获取作业结果"""
        with self.lock:
            return self.job_results.get(job_id)
    
    def get_all_jobs(self) -> List[Dict[str, Any]]:
        """获取所有作业"""
        with self.lock:
            return [job.to_dict() for job in self.jobs.values()]
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self.lock:
            status_counts = defaultdict(int)
            for job in self.jobs.values():
                status_counts[job.status] += 1
            
            return {
                'total_jobs': len(self.jobs),
                'job_status_counts': dict(status_counts),
                'completed_jobs': len(self.job_results),
                'max_workers': self.max_workers
            }


# 预定义的常用MapReduce函数
class CommonMapReduceFunctions:
    """常用MapReduce函数"""
    
    @staticmethod
    def word_count_map(line: str) -> List[Tuple[str, int]]:
        """词频统计Map函数"""
        words = line.strip().split()
        return [(word.lower(), 1) for word in words]
    
    @staticmethod
    def word_count_reduce(word: str, counts: List[int]) -> int:
        """词频统计Reduce函数"""
        return sum(counts)
    
    @staticmethod
    def log_analysis_map(log_entry: str) -> List[Tuple[str, int]]:
        """日志分析Map函数"""
        parts = log_entry.split()
        if len(parts) >= 9:
            status_code = parts[8]
            return [(status_code, 1)]
        return []
    
    @staticmethod
    def log_analysis_reduce(status_code: str, counts: List[int]) -> int:
        """日志分析Reduce函数"""
        return sum(counts)
    
    @staticmethod
    def sales_analysis_map(sales_record: Dict[str, Any]) -> List[Tuple[str, float]]:
        """销售分析Map函数"""
        region = sales_record.get('region', 'unknown')
        amount = sales_record.get('amount', 0)
        return [(region, amount)]
    
    @staticmethod
    def sales_analysis_reduce(region: str, amounts: List[float]) -> float:
        """销售分析Reduce函数"""
        return sum(amounts)
    
    @staticmethod
    def user_activity_map(activity_log: Dict[str, Any]) -> List[Tuple[str, int]]:
        """用户活动Map函数"""
        user_id = activity_log.get('user_id', 'unknown')
        return [(user_id, 1)]
    
    @staticmethod
    def user_activity_reduce(user_id: str, activities: List[int]) -> int:
        """用户活动Reduce函数"""
        return sum(activities) 