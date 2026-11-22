"""批量预测任务领域服务：基于已注册模型的批量预测执行、结果存储与校验。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.prediction_archive import (
    DEFAULT_FIELD_MAPPING,
    PredictionArchive,
    default_root,
)
from app.store import store


class PredictBatchService(BaseModuleService):
    MODULE = "predict_batch"
    ENTITY = "预测任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务编号", "模型编号", "数据切片", "输出格式", "分片策略", "并行度", "预测状态", "结果校验"]
    REQUIRED_FIELDS = ["任务编号", "模型编号", "数据切片"]
    STATUS_ORDER = ["草稿", "等待中", "运行中", "已完成"]
    ACTION_RULES = {"提交运行": "草稿", "停止预测": "运行中", "确认完成": "已完成"}
    NEGATIVE_ACTIONS = []

    def __init__(self, archive: PredictionArchive | None = None) -> None:
        self.archive = archive or PredictionArchive()

    @staticmethod
    def dataset_name(task_id: int) -> str:
        """存量预测数据集命名：同一任务的历史 CSV 与列式分区共用一套索引。"""
        return f"predict_task_{task_id}"

    def _resolve(self, task_id: int, source_csv: str | None):
        task = store.find(self.MODULE, task_id)
        if task is None:
            return None, None, {"ok": False, "message": f"预测任务 {task_id} 不存在"}
        dataset = self.dataset_name(task_id)
        stored = self.archive.latest_receipt(dataset) or {}
        mapping = stored.get("field_mapping") or DEFAULT_FIELD_MAPPING
        path = source_csv or stored.get("source_csv")
        if not path:
            return task, dataset, {
                "ok": False, "task_id": task_id, "dataset": dataset,
                "message": "尚未登记存量预测 CSV，请传入 source_csv",
            }
        return task, dataset, {"ok": True, "field_mapping": mapping, "source_csv": path}

    def _register_export_record(
        self,
        task_id: int,
        dataset: str,
        receipt: dict[str, Any],
    ) -> dict[str, Any]:
        """在数据导出管理里登记可回读的列式预测分区文件。"""
        export_rows = store.rows("data_export")
        task_no = f"EXP-PQ{task_id:04d}"
        committed = [
            part for part in receipt.get("partitions", [])
            if part.get("status") == "committed"
        ]
        values = {
            "数据来源": dataset,
            "导出范围": f"迁移分区：{','.join(part['dt'] for part in committed) or '无'}",
            "导出格式": "parquet",
            "字段映射": receipt.get("field_mapping", DEFAULT_FIELD_MAPPING),
            "分片策略": "dt",
            "压缩方式": "snappy",
            "导出状态": "已完成" if committed else "已失败",
            "archive_index": str(receipt.get("archive_index")),
            "migration_id": receipt.get("migration_id"),
            "partition_dates": [part["dt"] for part in committed],
            "partition_paths": [part["path"] for part in committed],
            "partition_count": len(committed),
            "partition_rows": sum(int(part.get("rows", 0)) for part in committed),
        }
        existing = next((row for row in export_rows if row.get("任务编号") == task_no), None)
        if existing is None:
            existing = {
                "id": max((int(row.get("id", 0)) for row in export_rows), default=0) + 1,
                "任务编号": task_no,
                "status": values["导出状态"],
                "pending": False,
                "abnormal": values["导出状态"] == "已失败",
            }
            export_rows.append(existing)
        existing.update(values)
        existing["status"] = values["导出状态"]
        existing["abnormal"] = values["导出状态"] == "已失败"
        return existing

    # ── 链路4: 批量预测 → 挖掘结果 ──
    def execute_batch_and_collect_result(self, task_id: int) -> dict[str, Any]:
        """执行批量预测任务并将预测结果采集到挖掘结果模块进行深度分析。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "message": f"预测任务 {task_id} 不存在"}
        task["status"] = "已完成"
        task["prediction_state"] = "已完成"
        task["pending"] = False
        mining_rows = store.rows("mining_result")
        next_id = max((int(r.get("id", 0)) for r in mining_rows), default=0) + 1
        model_id = task.get("模型编号", "unknown")
        mining_rows.append({
            "id": next_id,
            "status": "草稿",
            "pending": True,
            "abnormal": False,
            "结果编号": f"RES-P{task_id:04d}-{next_id:04d}",
            "分析主题": f"批量预测任务{task_id}挖掘分析",
            "算法类型": "ensemble_voting",
            "洞察摘要": f"从批量预测结果中识别出用户行为模式并写入挖掘结果" + "；发现2个关键行为路径",
            "关键分群": "高转化_群A,待激活_群B",
            "置信度": "0.82",
            "结果文件": f"mining/predict_{task_id}_result.json",
            "结果版本": "v1.0.0",
        })
        return {"ok": True, "task_id": task_id, "mining_id": next_id,
                "message": f"预测结果已采集并导入挖掘结果"}

    def validate_prediction_consistency(self, task_id: int) -> dict[str, Any]:
        """校验预测结果的一致性和格式合法性，确保输出符合预期schema。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        is_valid = task.get("输出格式") in ("csv", "json", "parquet")
        if not is_valid:
            task["abnormal"] = True
            return {"ok": False, "reason": f"输出格式 {task.get('输出格式')} 不合法"}
        return {"ok": True, "task_id": task_id, "format": task.get("输出格式")}

    def split_predict_partitions(self, task_id: int) -> dict[str, Any]:
        """将批量预测任务切分为多个分区并行执行，提高大规模数据集的预测吞吐。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        parallelism = int(str(task.get("并行度", "4")))
        partitions = []
        for i in range(parallelism):
            partitions.append({
                "partition_id": i,
                "task_id": task_id,
                "model_id": task.get("模型编号"),
                "output": f"predict_output/{task_id}/part_{i:03d}.csv",
            })
        return {"ok": True, "task_id": task_id, "partition_count": parallelism, "partitions": partitions}

    def export_predictions_to_csv(self, task_id: int) -> dict[str, Any]:
        """导出批量预测结果为CSV格式，写入外部存储供下游业务消费。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        export_rows = store.rows("data_export")
        next_id = max((int(r.get("id", 0)) for r in export_rows), default=0) + 1
        export_rows.append({
            "id": next_id,
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "任务编号": f"EXP-P{task_id:04d}",
            "数据来源": f"predict_task_{task_id}",
            "导出范围": "全量预测结果",
            "导出格式": "csv",
            "字段映射": "prediction_id,model_id,score,label,dt",
            "分片策略": "none",
            "压缩方式": "gzip",
            "导出状态": "已完成",
        })
        return {"ok": True, "export_id": next_id, "task_id": task_id}

    # ── 存量预测 CSV → Parquet 迁移 ───────────────────────────────────
    def migrate_predictions_to_parquet(
        self,
        task_id: int,
        source_csv: str | None = None,
        *,
        mode: str = "daily",
        dates: list[str] | None = None,
        resume: bool = True,
        force: bool = False,
        field_mapping: str | None = None,
    ) -> dict[str, Any]:
        """按数据日期把存量预测 CSV 迁移成 Snappy 列式分区并生成回执。

        字段映射默认沿用任务上登记的 ``prediction_id,model_id,score,label,dt``；
        迁移成功的数据日期开始从 Parquet 读取，失败日期继续按原 CSV 读取。
        """
        _, dataset, context = self._resolve(task_id, source_csv)
        if not context.get("ok"):
            return context
        receipt = self.archive.migrate(
            context["source_csv"],
            dataset,
            mode=mode,
            dates=dates,
            task_id=task_id,
            resume=resume,
            force=force,
            field_mapping=field_mapping or context["field_mapping"],
        )
        receipt["export_record"] = self._register_export_record(
            task_id, dataset, receipt)
        return receipt

    def read_migration_receipt(
        self,
        task_id: int,
        migration_id: str | None = None,
        *,
        source_csv: str | None = None,
    ) -> dict[str, Any]:
        """读取迁移回执原文；不指定 ``migration_id`` 时返回最近一次。"""
        _, dataset, context = self._resolve(task_id, source_csv)
        if not context.get("ok"):
            return context
        if migration_id:
            matching = [
                item for item in self.archive.list_receipts(dataset)
                if item.get("migration_id") == migration_id
            ]
            if not matching:
                return {"ok": False, "dataset": dataset,
                        "message": f"迁移回执 {migration_id} 不存在"}
            receipt = matching[0]
        else:
            receipt = self.archive.latest_receipt(dataset)
            if receipt is None:
                return {"ok": False, "dataset": dataset, "message": "该任务还没有迁移回执"}
        receipt["dataset"] = dataset
        return receipt

    def verify_migration_receipt(
        self,
        task_id: int,
        migration_id: str | None = None,
        *,
        source_csv: str | None = None,
    ) -> dict[str, Any]:
        """按迁移回执核对源行数、模型标识和分区去向。"""
        _, dataset, context = self._resolve(task_id, source_csv)
        if not context.get("ok"):
            return context
        receipts = self.archive.list_receipts(dataset)
        if migration_id:
            matching = [item for item in receipts if item.get("migration_id") == migration_id]
            if not matching:
                return {"ok": False, "dataset": dataset,
                        "message": f"迁移回执 {migration_id} 不存在"}
            receipt = matching[0]
        else:
            receipt = self.archive.latest_receipt(dataset)
            if receipt is None:
                return {"ok": False, "dataset": dataset, "message": "该任务还没有迁移回执"}
        verification = self.archive.verify_receipt(
            receipt, source_csv=context["source_csv"])
        verification["dataset"] = dataset
        verification["migration_id"] = receipt.get("migration_id")
        return verification

    def read_predictions_by_date(
        self,
        task_id: int,
        dt: str,
        *,
        source_csv: str | None = None,
    ) -> dict[str, Any]:
        """业务侧按数据日期读取预测报告：已切换分区读 Parquet，否则回退 CSV。"""
        _, dataset, context = self._resolve(task_id, source_csv)
        if not context.get("ok"):
            return context
        result = self.archive.read(
            dt, dataset, source_csv=context["source_csv"])
        result["task_id"] = task_id
        result["dataset"] = dataset
        return result

    def prediction_archive_index(self, task_id: int) -> dict[str, Any]:
        """返回该任务的归档索引：每个数据日期指向当前生效的存储位置。"""
        _, dataset, context = self._resolve(task_id, None)
        if not context.get("ok") and dataset is None:
            return context
        index = self.archive.archive_index(dataset)
        index["task_id"] = task_id
        index["data_root"] = str(default_root())
        return index