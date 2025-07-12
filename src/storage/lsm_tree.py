"""
LSM-Tree存储引擎实现
展示Log-Structured Merge Tree的设计，适用于写密集型应用
"""
import os
import json
import time
import hashlib
import threading
from typing import Dict, List, Optional, Tuple, Any, Iterator
from dataclasses import dataclass
from datetime import datetime
import heapq
import bisect
from collections import defaultdict

from ..data_models.base import BaseModel


@dataclass
class KeyValue:
    """键值对"""
    key: str
    value: Any
    timestamp: float
    deleted: bool = False
    
    def __lt__(self, other):
        return self.key < other.key


class BloomFilter:
    """布隆过滤器 - 用于快速判断key是否不存在"""
    
    def __init__(self, capacity: int = 10000, error_rate: float = 0.01):
        self.capacity = capacity
        self.error_rate = error_rate
        
        # 计算最优参数
        self.bit_array_size = int(-capacity * math.log(error_rate) / (math.log(2) ** 2))
        self.hash_count = int(self.bit_array_size * math.log(2) / capacity)
        
        # 位数组
        self.bit_array = [False] * self.bit_array_size
        self.item_count = 0
    
    def _hash(self, item: str, seed: int) -> int:
        """哈希函数"""
        hash_value = hashlib.md5((item + str(seed)).encode()).hexdigest()
        return int(hash_value, 16) % self.bit_array_size
    
    def add(self, item: str):
        """添加元素"""
        for i in range(self.hash_count):
            index = self._hash(item, i)
            self.bit_array[index] = True
        self.item_count += 1
    
    def contains(self, item: str) -> bool:
        """检查元素是否可能存在"""
        for i in range(self.hash_count):
            index = self._hash(item, i)
            if not self.bit_array[index]:
                return False
        return True
    
    @property
    def false_positive_rate(self) -> float:
        """当前假阳性率"""
        if self.item_count == 0:
            return 0.0
        return (1 - math.exp(-self.hash_count * self.item_count / self.bit_array_size)) ** self.hash_count


class MemTable:
    """内存表 - 使用跳表实现有序存储"""
    
    def __init__(self, max_size: int = 1024 * 1024):  # 1MB
        self.data: Dict[str, KeyValue] = {}
        self.max_size = max_size
        self.current_size = 0
        self.lock = threading.RWLock()
    
    def put(self, key: str, value: Any) -> bool:
        """插入键值对"""
        with self.lock.write_lock():
            kv = KeyValue(key, value, time.time())
            
            # 估算大小
            size_estimate = len(key) + len(str(value)) + 32  # 额外开销
            
            if key in self.data:
                # 更新现有key
                old_size = len(self.data[key].key) + len(str(self.data[key].value)) + 32
                self.current_size = self.current_size - old_size + size_estimate
            else:
                # 新key
                self.current_size += size_estimate
            
            self.data[key] = kv
            return self.current_size >= self.max_size
    
    def get(self, key: str) -> Optional[KeyValue]:
        """获取值"""
        with self.lock.read_lock():
            return self.data.get(key)
    
    def delete(self, key: str):
        """删除键（软删除）"""
        with self.lock.write_lock():
            kv = KeyValue(key, None, time.time(), deleted=True)
            self.data[key] = kv
    
    def scan(self, start_key: str = None, end_key: str = None) -> Iterator[KeyValue]:
        """范围扫描"""
        with self.lock.read_lock():
            sorted_keys = sorted(self.data.keys())
            
            start_idx = 0
            if start_key:
                start_idx = bisect.bisect_left(sorted_keys, start_key)
            
            end_idx = len(sorted_keys)
            if end_key:
                end_idx = bisect.bisect_right(sorted_keys, end_key)
            
            for i in range(start_idx, end_idx):
                key = sorted_keys[i]
                yield self.data[key]
    
    def to_sstable(self, filename: str):
        """转换为SSTable"""
        with self.lock.read_lock():
            sorted_items = sorted(self.data.values(), key=lambda x: x.key)
            
            # 写入数据文件
            with open(filename + ".data", "w") as f:
                for kv in sorted_items:
                    f.write(json.dumps({
                        "key": kv.key,
                        "value": kv.value,
                        "timestamp": kv.timestamp,
                        "deleted": kv.deleted
                    }) + "\n")
            
            # 写入索引文件
            with open(filename + ".index", "w") as f:
                offset = 0
                for kv in sorted_items:
                    f.write(f"{kv.key},{offset}\n")
                    # 计算下一行的偏移量
                    line = json.dumps({
                        "key": kv.key,
                        "value": kv.value,
                        "timestamp": kv.timestamp,
                        "deleted": kv.deleted
                    }) + "\n"
                    offset += len(line.encode())
    
    def clear(self):
        """清空内存表"""
        with self.lock.write_lock():
            self.data.clear()
            self.current_size = 0


class SSTable:
    """Sorted String Table - 不可变的有序文件"""
    
    def __init__(self, filename: str):
        self.filename = filename
        self.index: Dict[str, int] = {}  # key -> file offset
        self.bloom_filter = BloomFilter()
        self._load_index()
        self._load_bloom_filter()
    
    def _load_index(self):
        """加载索引"""
        index_file = self.filename + ".index"
        if os.path.exists(index_file):
            with open(index_file, "r") as f:
                for line in f:
                    key, offset = line.strip().split(",")
                    self.index[key] = int(offset)
    
    def _load_bloom_filter(self):
        """加载布隆过滤器"""
        # 从索引重建布隆过滤器
        for key in self.index.keys():
            self.bloom_filter.add(key)
    
    def get(self, key: str) -> Optional[KeyValue]:
        """获取值"""
        # 先检查布隆过滤器
        if not self.bloom_filter.contains(key):
            return None
        
        # 检查索引
        if key not in self.index:
            return None
        
        # 从文件读取
        offset = self.index[key]
        data_file = self.filename + ".data"
        
        with open(data_file, "r") as f:
            f.seek(offset)
            line = f.readline()
            data = json.loads(line)
            
            return KeyValue(
                key=data["key"],
                value=data["value"],
                timestamp=data["timestamp"],
                deleted=data["deleted"]
            )
    
    def scan(self, start_key: str = None, end_key: str = None) -> Iterator[KeyValue]:
        """范围扫描"""
        data_file = self.filename + ".data"
        
        with open(data_file, "r") as f:
            for line in f:
                data = json.loads(line)
                kv = KeyValue(
                    key=data["key"],
                    value=data["value"],
                    timestamp=data["timestamp"],
                    deleted=data["deleted"]
                )
                
                # 检查范围
                if start_key and kv.key < start_key:
                    continue
                if end_key and kv.key > end_key:
                    break
                
                yield kv
    
    def size(self) -> int:
        """文件大小"""
        data_file = self.filename + ".data"
        return os.path.getsize(data_file) if os.path.exists(data_file) else 0


class CompactionStrategy:
    """压缩策略基类"""
    
    def should_compact(self, sstables: List[SSTable]) -> bool:
        """是否应该进行压缩"""
        raise NotImplementedError
    
    def select_files(self, sstables: List[SSTable]) -> List[SSTable]:
        """选择要压缩的文件"""
        raise NotImplementedError


class SizeTieredCompaction(CompactionStrategy):
    """大小分层压缩策略"""
    
    def __init__(self, max_sstables: int = 4, size_ratio: float = 2.0):
        self.max_sstables = max_sstables
        self.size_ratio = size_ratio
    
    def should_compact(self, sstables: List[SSTable]) -> bool:
        """当同一层级的SSTable数量超过阈值时触发压缩"""
        if len(sstables) < self.max_sstables:
            return False
        
        # 按大小分组
        size_groups = defaultdict(list)
        for sstable in sstables:
            size = sstable.size()
            # 计算大小层级
            level = int(math.log2(size / 1024)) if size > 0 else 0
            size_groups[level].append(sstable)
        
        # 检查是否有层级需要压缩
        for level, tables in size_groups.items():
            if len(tables) >= self.max_sstables:
                return True
        
        return False
    
    def select_files(self, sstables: List[SSTable]) -> List[SSTable]:
        """选择同一层级的文件进行压缩"""
        size_groups = defaultdict(list)
        for sstable in sstables:
            size = sstable.size()
            level = int(math.log2(size / 1024)) if size > 0 else 0
            size_groups[level].append(sstable)
        
        # 选择最小层级中的所有文件
        min_level = min(size_groups.keys())
        return size_groups[min_level]


class LSMTree:
    """LSM-Tree存储引擎"""
    
    def __init__(self, data_dir: str, memtable_size: int = 1024 * 1024):
        self.data_dir = data_dir
        self.memtable_size = memtable_size
        
        # 确保目录存在
        os.makedirs(data_dir, exist_ok=True)
        
        # 内存表
        self.memtable = MemTable(memtable_size)
        self.immutable_memtables: List[MemTable] = []
        
        # SSTable列表
        self.sstables: List[SSTable] = []
        
        # 压缩策略
        self.compaction_strategy = SizeTieredCompaction()
        
        # 锁
        self.write_lock = threading.Lock()
        self.compaction_lock = threading.Lock()
        
        # 加载现有的SSTable
        self._load_sstables()
        
        # 启动后台压缩线程
        self.compaction_thread = threading.Thread(target=self._background_compaction, daemon=True)
        self.compaction_thread.start()
    
    def _load_sstables(self):
        """加载现有的SSTable文件"""
        for filename in os.listdir(self.data_dir):
            if filename.endswith(".data"):
                base_name = filename[:-5]  # 移除.data后缀
                sstable_path = os.path.join(self.data_dir, base_name)
                self.sstables.append(SSTable(sstable_path))
        
        # 按文件名排序（通常包含时间戳）
        self.sstables.sort(key=lambda x: x.filename)
    
    def put(self, key: str, value: Any):
        """插入键值对"""
        with self.write_lock:
            # 写入当前memtable
            should_flush = self.memtable.put(key, value)
            
            if should_flush:
                # 将当前memtable标记为不可变
                self.immutable_memtables.append(self.memtable)
                
                # 创建新的memtable
                self.memtable = MemTable(self.memtable_size)
                
                # 异步刷新到磁盘
                threading.Thread(target=self._flush_memtable, daemon=True).start()
    
    def get(self, key: str) -> Optional[Any]:
        """获取值"""
        # 1. 先查找当前memtable
        kv = self.memtable.get(key)
        if kv:
            return None if kv.deleted else kv.value
        
        # 2. 查找不可变memtable（按时间倒序）
        for memtable in reversed(self.immutable_memtables):
            kv = memtable.get(key)
            if kv:
                return None if kv.deleted else kv.value
        
        # 3. 查找SSTable（按时间倒序，新的文件优先）
        for sstable in reversed(self.sstables):
            kv = sstable.get(key)
            if kv:
                return None if kv.deleted else kv.value
        
        return None
    
    def delete(self, key: str):
        """删除键"""
        with self.write_lock:
            should_flush = self.memtable.put(key, None)  # 墓碑标记
            self.memtable.delete(key)
            
            if should_flush:
                self.immutable_memtables.append(self.memtable)
                self.memtable = MemTable(self.memtable_size)
                threading.Thread(target=self._flush_memtable, daemon=True).start()
    
    def scan(self, start_key: str = None, end_key: str = None) -> Iterator[Tuple[str, Any]]:
        """范围扫描"""
        # 合并所有数据源的结果
        iterators = []
        
        # 添加memtable迭代器
        iterators.append(self.memtable.scan(start_key, end_key))
        
        # 添加不可变memtable迭代器
        for memtable in self.immutable_memtables:
            iterators.append(memtable.scan(start_key, end_key))
        
        # 添加SSTable迭代器
        for sstable in self.sstables:
            iterators.append(sstable.scan(start_key, end_key))
        
        # 使用归并排序合并结果
        seen_keys = set()
        for kv in heapq.merge(*iterators, key=lambda x: x.key):
            if kv.key not in seen_keys:
                seen_keys.add(kv.key)
                if not kv.deleted:
                    yield kv.key, kv.value
    
    def _flush_memtable(self):
        """将不可变memtable刷新到磁盘"""
        if not self.immutable_memtables:
            return
        
        memtable = self.immutable_memtables.pop(0)
        timestamp = int(time.time() * 1000)
        filename = os.path.join(self.data_dir, f"sstable_{timestamp}")
        
        # 转换为SSTable
        memtable.to_sstable(filename)
        
        # 添加到SSTable列表
        sstable = SSTable(filename)
        self.sstables.append(sstable)
        
        print(f"Flushed memtable to {filename}")
    
    def _background_compaction(self):
        """后台压缩线程"""
        while True:
            try:
                time.sleep(10)  # 每10秒检查一次
                
                with self.compaction_lock:
                    if self.compaction_strategy.should_compact(self.sstables):
                        self._compact()
            except Exception as e:
                print(f"Compaction error: {e}")
    
    def _compact(self):
        """执行压缩"""
        files_to_compact = self.compaction_strategy.select_files(self.sstables)
        
        if len(files_to_compact) < 2:
            return
        
        print(f"Compacting {len(files_to_compact)} files")
        
        # 创建新的SSTable
        timestamp = int(time.time() * 1000)
        new_filename = os.path.join(self.data_dir, f"compacted_{timestamp}")
        
        # 合并所有选中的SSTable
        self._merge_sstables(files_to_compact, new_filename)
        
        # 删除旧文件
        for sstable in files_to_compact:
            self._remove_sstable(sstable)
        
        # 添加新文件
        new_sstable = SSTable(new_filename)
        self.sstables.append(new_sstable)
        
        print(f"Compaction completed: {new_filename}")
    
    def _merge_sstables(self, sstables: List[SSTable], output_filename: str):
        """合并多个SSTable"""
        iterators = [sstable.scan() for sstable in sstables]
        
        with open(output_filename + ".data", "w") as data_file, \
             open(output_filename + ".index", "w") as index_file:
            
            offset = 0
            last_key = None
            
            # 使用归并排序合并所有SSTable
            for kv in heapq.merge(*iterators, key=lambda x: x.key):
                # 跳过重复的key（保留最新的）
                if kv.key == last_key:
                    continue
                
                last_key = kv.key
                
                # 跳过已删除的记录
                if kv.deleted:
                    continue
                
                # 写入数据
                line = json.dumps({
                    "key": kv.key,
                    "value": kv.value,
                    "timestamp": kv.timestamp,
                    "deleted": kv.deleted
                }) + "\n"
                
                data_file.write(line)
                
                # 写入索引
                index_file.write(f"{kv.key},{offset}\n")
                offset += len(line.encode())
    
    def _remove_sstable(self, sstable: SSTable):
        """删除SSTable文件"""
        try:
            os.remove(sstable.filename + ".data")
            os.remove(sstable.filename + ".index")
            self.sstables.remove(sstable)
        except OSError as e:
            print(f"Error removing SSTable {sstable.filename}: {e}")
    
    def stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "memtable_size": self.memtable.current_size,
            "immutable_memtables": len(self.immutable_memtables),
            "sstables_count": len(self.sstables),
            "total_sstable_size": sum(sstable.size() for sstable in self.sstables),
            "bloom_filter_false_positive_rate": 
                sum(sstable.bloom_filter.false_positive_rate for sstable in self.sstables) / 
                len(self.sstables) if self.sstables else 0
        }


# 读写锁实现
class RWLock:
    """读写锁"""
    
    def __init__(self):
        self._read_ready = threading.Condition(threading.RLock())
        self._readers = 0
    
    def read_lock(self):
        """获取读锁"""
        return self._ReadLock(self)
    
    def write_lock(self):
        """获取写锁"""
        return self._WriteLock(self)
    
    class _ReadLock:
        def __init__(self, rwlock):
            self.rwlock = rwlock
        
        def __enter__(self):
            with self.rwlock._read_ready:
                self.rwlock._readers += 1
        
        def __exit__(self, exc_type, exc_val, exc_tb):
            with self.rwlock._read_ready:
                self.rwlock._readers -= 1
                if self.rwlock._readers == 0:
                    self.rwlock._read_ready.notifyAll()
    
    class _WriteLock:
        def __init__(self, rwlock):
            self.rwlock = rwlock
        
        def __enter__(self):
            self.rwlock._read_ready.acquire()
            while self.rwlock._readers > 0:
                self.rwlock._read_ready.wait()
        
        def __exit__(self, exc_type, exc_val, exc_tb):
            self.rwlock._read_ready.release()


# 为了完整性，添加math模块导入
import math

# 扩展threading模块
threading.RWLock = RWLock 