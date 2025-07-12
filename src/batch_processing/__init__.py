"""
批处理系统包
展示《数据密集型应用系统设计》中的批处理概念
"""

from .mapreduce import *
from .etl import *
from .pipeline import *

__all__ = [
    # MapReduce
    "MapReduceJob",
    "MapReduceExecutor",
    "MapTask",
    "ReduceTask",
    "Combiner",
    
    # ETL
    "ETLPipeline",
    "DataExtractor",
    "DataTransformer",
    "DataLoader",
    "DataQualityChecker",
    
    # 数据管道
    "DataPipeline",
    "PipelineStage",
    "PipelineExecutor",
    "BatchProcessor",
] 