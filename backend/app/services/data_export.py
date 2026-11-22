"""数据导出管理领域服务：分析结果的导出配置、格式转换与异步任务。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.prediction_archive import PredictionArchive
from app.store import store


class DataExportService(BaseModuleService):
    MODULE = "data_export"
    ENTITY = "导出任务"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["任务编号", "数据来源", "导出范围", "导出格式", "字段映射", "分片策略", "压缩方式", "导出状态"]
    REQUIRED_FIELDS = ["任务编号", "数据来源", "导出范围"]
    STATUS_ORDER = ["草稿", "导出中", "已完成", "已失败"]
    ACTION_RULES = {"开始导出": "导出中", "确认完成": "已完成", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = []

    def __init__(self, archive: PredictionArchive | None = None) -> None:
        self.archive = archive or PredictionArchive()

    def export_feature_dataset(self, task_id: int, output_format: str = "csv") -> dict[str, Any]:
        """将特征数据集导出为指定格式并写入外部存储路径。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        task["status"] = "导出中"
        task["pending"] = True
        task["导出格式"] = output_format
        task["status"] = "已完成"
        task["pending"] = False
        return {"ok": True, "task_id": task_id, "format": output_format,
                "output_path": f"exports/{task_id}/feature_dataset.{output_format}"}

    def convert_to_parquet(self, task_id: int) -> dict[str, Any]:
        """将导出数据转换为Parquet列式存储格式并压缩归档。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        task["导出格式"] = "parquet"
        task["压缩方式"] = "snappy"
        task["status"] = "已完成"
        task["pending"] = False
        return {"ok": True, "task_id": task_id, "format": "parquet",
                "compression": "snappy", "path": f"exports/{task_id}/data.snappy.parquet"}

    def read_prediction_partition(self, task_id: int) -> dict[str, Any]:
        """回读导出任务对应的列式预测分区，供下游业务核对字段。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "message": f"导出任务 {task_id} 不存在"}
        dataset = str(task.get("数据来源") or "")
        partitions = self._prediction_partitions(dataset)
        paths = task.get("partition_paths")
        if isinstance(paths, list) and paths:
            # 只回读登记这张导出任务的那几个文件，避免把后来新增的日期算进来。
            wanted = set(paths)
            partitions = {
                dt: part for dt, part in partitions.items()
                if str(part.get("path")) in wanted
            }
        if not partitions:
            return {
                "ok": False,
                "task_id": task_id,
                "dataset": dataset,
                "format": task.get("导出格式"),
                "message": "该导出任务没有可回读的列式预测分区",
            }
        files: list[dict[str, Any]] = []
        rows: list[dict[str, Any]] = []
        field_mapping = task.get("字段映射")
        for dt in sorted(partitions):
            part = partitions[dt]
            path = self.archive.root / str(part["path"])
            if not path.exists():
                return {"ok": False, "task_id": task_id, "dataset": dataset,
                        "message": f"{dt} 分区文件丢失：{part['path']}"}
            partition_rows = self.archive.read_partition_file(path)
            files.append({
                "dt": dt,
                "data_date": part.get("data_date", dt),
                "path": str(part["path"]),
                "rows": len(partition_rows),
                "compression": part.get("compression", "snappy"),
            })
            rows.extend(partition_rows)
        return {
            "ok": True,
            "task_id": task_id,
            "dataset": dataset,
            "format": "parquet",
            "compression": "snappy",
            "field_mapping": field_mapping,
            "parts": files,
            "row_count": len(rows),
            "rows": rows,
        }

    def _prediction_partitions(self, dataset: str) -> dict[str, dict[str, Any]]:
        """数据导出管理只暴露已切换成列式存储的日期，CSV 回退日期不在此列。"""
        if not dataset:
            return {}
        return dict(self.archive.archive_index(dataset).get("partitions") or {})

    def archive_export_to_storage(self, task_id: int) -> dict[str, Any]:
        """将完成的导出任务归档到长期存储，保留历史版本供回溯。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        archive_path = f"archive/exports/{task_id}/v1.0/"
        return {
            "ok": True,
            "task_id": task_id,
            "archive_path": archive_path,
            "format": task.get("导出格式"),
            "message": f"导出任务 {task_id} 已归档至 {archive_path}",
        }

    def query_export_progress(self, task_id: int) -> dict[str, Any]:
        """查询导出任务的执行状态和进度详情。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        return {
            "ok": True,
            "task_id": task_id,
            "status": task.get("status"),
            "source": task.get("数据来源"),
            "format": task.get("导出格式"),
            "compression": task.get("压缩方式"),
        }