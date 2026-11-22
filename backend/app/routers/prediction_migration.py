"""存量预测数据迁移接口：迁移、回执核对、按日期回读和归档索引。

路径前缀独立于通用资源路由，保持现有 ``/api/{module}/{id}`` 契约不变。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services import service_for

router = APIRouter(prefix="/api/predict_batch", tags=["存量预测迁移"])


class PredictionMigrationPayload(BaseModel):
    source_csv: str | None = None
    mode: str = Field(default="daily", description="daily 按日切换，batch 整批切换")
    dates: list[str] | None = None
    field_mapping: str | None = None
    resume: bool = True
    force: bool = False


def _service():
    service = service_for("predict_batch")
    if service is None:
        raise HTTPException(status_code=404, detail="批量预测任务模块未注册")
    return service


@router.post("/{task_id}/migration")
def migrate_predictions(task_id: int, payload: PredictionMigrationPayload) -> dict[str, Any]:
    """把存量预测 CSV 迁移为按数据日期的 Snappy 列式分区并生成回执。"""
    service = _service()
    return service.migrate_predictions_to_parquet(
        task_id,
        payload.source_csv,
        mode=payload.mode,
        dates=payload.dates,
        resume=payload.resume,
        force=payload.force,
        field_mapping=payload.field_mapping,
    )


@router.get("/{task_id}/migration/receipt")
def get_migration_receipt(
    task_id: int,
    migration_id: str | None = Query(default=None),
    source_csv: str | None = Query(default=None),
) -> dict[str, Any]:
    """返回迁移回执；不传 ``migration_id`` 时取最近一次。"""
    service = _service()
    return service.read_migration_receipt(
        task_id, migration_id, source_csv=source_csv)


@router.get("/{task_id}/migration/verify")
def verify_migration_receipt(
    task_id: int,
    migration_id: str | None = Query(default=None),
    source_csv: str | None = Query(default=None),
) -> dict[str, Any]:
    """按回执核对源行数、模型标识、分区行数与归档索引关联。"""
    service = _service()
    return service.verify_migration_receipt(
        task_id, migration_id, source_csv=source_csv)


@router.get("/{task_id}/predictions")
def read_predictions(
    task_id: int,
    dt: str = Query(description="源数据日期，如 2026-11-20"),
    source_csv: str | None = Query(default=None),
) -> dict[str, Any]:
    """业务按数据日期读取预测结果：已切换读 Parquet，否则回退原 CSV。"""
    service = _service()
    return service.read_predictions_by_date(task_id, dt, source_csv=source_csv)


@router.get("/{task_id}/archive-index")
def get_archive_index(task_id: int) -> dict[str, Any]:
    """返回归档索引：每个数据日期与当前生效存储位置的对应关系。"""
    service = _service()
    return service.prediction_archive_index(task_id)


@router.get("/{task_id}/export/{export_id}/read")
def read_export_partition(task_id: int, export_id: int) -> dict[str, Any]:
    """回读数据导出管理里登记的列式预测分区文件。"""
    service = service_for("data_export")
    if service is None:
        raise HTTPException(status_code=404, detail="数据导出管理模块未注册")
    return service.read_prediction_partition(export_id)
