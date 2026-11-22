"""存量预测 CSV → Parquet 迁移的端到端测试。

覆盖：模型号前导零/空标签/小数精度的回读、按日与整批两种切换边界、
坏行定位、修正后恢复、归档索引关联原始数据日期，以及中断恢复后的
行数不重复不丢失。
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.data_export import DataExportService
from app.services.predict_batch import PredictBatchService
from app.services.prediction_archive import (
    PredictionArchive,
    format_score,
    parse_field_mapping,
    parse_score,
)

CSV_HEADER = "prediction_id,model_id,score,label,dt\n"
DAY1 = "2026-11-20"
DAY2 = "2026-11-21"


def make_workspace():
    root = Path(tempfile.mkdtemp(prefix="pred-archive-"))
    csv_path = root / "predictions.csv"
    archive = PredictionArchive(root / "data")
    service = PredictBatchService(archive=archive)
    return root, csv_path, archive, service


def write_csv(csv_path: Path, body: str) -> None:
    csv_path.write_text(CSV_HEADER + body, encoding="utf-8")


BASE_BODY = (
    "p1,001,0.912345678901234567,1,2026-11-20\n"
    "p2,001,0.25,,2026-11-20\n"
    "p3,0002,0.5,0,2026-11-21\n"
    "p4,0002,oops,1,2026-11-21\n"
)
FIXED_BODY = BASE_BODY.replace("oops", "0.75")


def test_roundtrip_preserves_leading_zero_score_and_empty_label():
    """模型号 001、小数 score、空 label 回读后保持原含义。"""
    _, csv_path, archive, service = make_workspace()
    write_csv(csv_path, BASE_BODY.replace("oops", "0.75"))

    receipt = service.migrate_predictions_to_parquet(1, str(csv_path))
    assert receipt["ok"], receipt
    assert receipt["models"] == ["0002", "001"]

    result = archive.read(DAY1, "predict_task_1")
    assert result["format"] == "parquet"
    assert result["compression"] == "snappy"
    rows = {row["prediction_id"]: row for row in result["rows"]}
    assert rows["p1"]["model_id"] == "001", "前导零不能丢"
    assert rows["p1"]["score"] == "0.912345678901234567", "小数精度不能丢"
    assert rows["p1"]["label"] == "1"
    assert rows["p2"]["label"] is None, "空标签回读必须是 NULL"
    assert all(row["dt"] == DAY1 for row in result["rows"])

    day2 = {row["prediction_id"]: row for row in archive.read(DAY2, "predict_task_1")["rows"]}
    assert day2["p3"]["model_id"] == "0002"
    assert day2["p3"]["label"] == "0", "0 标签不能当成空"

    compression = archive.partition_compression(
        archive.root / receipt["partitions"][0]["path"])
    assert compression and all(value == "SNAPPY" for value in compression), compression


def test_score_parsing_rejects_lossy_values():
    """超出目标精度的小数判定为坏行，不做静默四舍五入。"""
    assert format_score(parse_score("0.9100")) == "0.91"
    assert str(parse_score("1e-3")) in ("0.001", "1E-3")
    for bad in ("", "abc", "nan", "1." + "1" * 25):
        try:
            parse_score(bad)
        except ValueError:
            continue
        raise AssertionError(f"{bad!r} 应该被判为坏行")


def test_field_mapping_from_batch_task_is_reusable():
    """批量预测任务已有的字段映射可复用到迁移，列名不同也能对应。"""
    mapped = parse_field_mapping(
        "prediction_id=pred_id,model_id=model_code,score=raw_score,label=cls,dt=biz_date")
    assert mapped["model_id"] == "model_code"

    _, csv_path, archive, service = make_workspace()
    csv_path.write_text(
        "pred_id,model_code,raw_score,cls,biz_date\n"
        "x1,001,0.5,,2026-11-20\n",
        encoding="utf-8",
    )
    receipt = service.migrate_predictions_to_parquet(
        1, str(csv_path), field_mapping=(
            "prediction_id=pred_id,model_id=model_code,score=raw_score,"
            "label=cls,dt=biz_date"))
    assert receipt["ok"], receipt
    rows = archive.read(DAY1, "predict_task_1")["rows"]
    assert rows == [{
        "prediction_id": "x1", "model_id": "001", "score": "0.5",
        "label": None, "dt": DAY1,
    }], rows


def test_daily_mode_switches_ready_day_and_keeps_failed_day_on_csv():
    """第一天全部就绪走 Parquet，第二天坏行失败仍按旧 CSV 定位原始行。"""
    _, csv_path, archive, service = make_workspace()
    write_csv(csv_path, BASE_BODY)

    receipt = service.migrate_predictions_to_parquet(1, str(csv_path), mode="daily")
    assert not receipt["ok"]
    assert receipt["switch"]["committed_dates"] == [DAY1]
    assert receipt["switch"]["failed_dates"] == [DAY2]

    day1 = archive.read(DAY1, "predict_task_1")
    assert day1["format"] == "parquet" and day1["row_count"] == 2

    day2 = archive.read(DAY2, "predict_task_1")
    assert day2["format"] == "csv"
    assert day2["row_count"] == 2, "失败日期仍要返回完整输入行"
    assert not day2["complete"]
    bad = [row for row in day2["rows"] if not row["_valid"]]
    assert bad and bad[0]["_source_line"] == 5, "坏行要能定位到原 CSV 行号"
    assert bad[0]["score"] == "oops"

    failures = receipt["failures"][0]
    assert failures["dt"] == DAY2
    assert failures["issues"][0]["source_line"] == 5


def test_batch_mode_changes_day_one_storage_format():
    """整批切换边界下，只要有一天坏行，第一天也保持 CSV。"""
    _, csv_path, archive, service = make_workspace()
    write_csv(csv_path, BASE_BODY)

    receipt = service.migrate_predictions_to_parquet(1, str(csv_path), mode="batch")
    assert not receipt["ok"]
    assert receipt["switch"]["committed_dates"] == []
    assert receipt["switch"]["block_reason"] == "batch_validation_failed"

    day1 = archive.read(DAY1, "predict_task_1")
    assert day1["format"] == "csv", "整批失败时第一天不能提前切到 Parquet"
    assert day1["row_count"] == 2
    assert not list((archive.root / "parquet").glob("dt=*/part-*.parquet"))


def test_fix_bad_row_then_resume_keeps_dates_and_row_counts():
    """修正坏行后恢复迁移，索引回到原数据日期且行数不重复不丢失。"""
    _, csv_path, archive, service = make_workspace()
    write_csv(csv_path, BASE_BODY)
    first = service.migrate_predictions_to_parquet(1, str(csv_path))
    assert not first["ok"]

    write_csv(csv_path, FIXED_BODY)
    second = service.migrate_predictions_to_parquet(1, mode="daily")
    assert second["ok"], second
    assert second["switch"]["skipped_dates"] == [DAY1], "已就绪日期不应重复写入"
    assert second["switch"]["switched_dates"] == [DAY2]

    index = service.prediction_archive_index(1)["partitions"]
    assert sorted(index) == [DAY1, DAY2]
    assert index[DAY1]["data_date"] == DAY1, "归档索引必须关联回原数据日期"
    assert index[DAY2]["data_date"] == DAY2
    for dt in (DAY1, DAY2):
        assert Path(archive.root / index[dt]["path"]).exists()

    verification = service.verify_migration_receipt(1)
    assert verification["ok"], verification["mismatches"]
    assert verification["checks"]["coverage"] == {
        "parquet_rows": 4, "fallback_rows": 0, "total": 4,
        "source_rows": 4, "ok": True,
    }
    assert verification["checks"]["duplicates"]["ok"]

    # 每个可读分区对应一套完整输入
    for dt in (DAY1, DAY2):
        report = archive.read(dt, "predict_task_1")
        assert report["format"] == "parquet"
        assert report["row_count"] == 2
        assert {row["dt"] for row in report["rows"]} == {dt}


def test_interrupted_resume_does_not_duplicate_or_lose_rows():
    """模拟写完分区但未提交索引就中断：恢复后只保留一份可读结果。"""
    _, csv_path, archive, service = make_workspace()
    write_csv(csv_path, FIXED_BODY)

    first = service.migrate_predictions_to_parquet(1, str(csv_path), mode="daily")
    assert first["ok"], first

    # 中断残留：多写了一个未被索引引用的 revision 文件。
    index = archive.archive_index("predict_task_1")["partitions"]
    stray = archive.root / "parquet" / f"dt={DAY2}" / "part-INTERRUPTED.parquet"
    stray.write_bytes(Path(archive.root / index[DAY2]["path"]).read_bytes())
    assert stray.exists()

    before = archive.read(DAY1, "predict_task_1")
    resume = service.migrate_predictions_to_parquet(1, mode="daily")
    assert resume["ok"], resume
    assert resume["switch"]["skipped_dates"] == [DAY1, DAY2]
    assert not stray.exists(), "恢复后不应留下同日期第二份数据"

    after = archive.read(DAY1, "predict_task_1")
    assert before["row_count"] == after["row_count"] == 2
    assert before["rows"] == after["rows"]

    verification = service.verify_migration_receipt(1)
    assert verification["ok"], verification["mismatches"]
    assert verification["checks"]["coverage"]["total"] == 4


def test_data_export_registers_readable_parquet_partitions():
    """数据导出管理登记可回读的列式预测分区文件。"""
    _, csv_path, archive, service = make_workspace()
    write_csv(csv_path, FIXED_BODY)
    receipt = service.migrate_predictions_to_parquet(1, str(csv_path))
    assert receipt["ok"], receipt

    record = receipt["export_record"]
    assert record["导出格式"] == "parquet"
    assert record["压缩方式"] == "snappy"
    assert record["partition_count"] == 2
    assert record["partition_rows"] == 4

    exporter = DataExportService(archive=archive)
    replayed = exporter.read_prediction_partition(record["id"])
    assert replayed["ok"], replayed
    assert replayed["row_count"] == 4
    assert {row["model_id"] for row in replayed["rows"]} == {"001", "0002"}
    assert any(row["label"] is None for row in replayed["rows"])


def test_verify_receipt_detects_missing_partition_and_model_drift():
    """回执核对能发现分区丢失和模型标识不一致。"""
    _, csv_path, archive, service = make_workspace()
    write_csv(csv_path, FIXED_BODY)
    receipt = service.migrate_predictions_to_parquet(1, str(csv_path))
    assert receipt["ok"], receipt

    good = archive.verify_receipt(receipt)
    assert good["ok"], good["mismatches"]

    path = archive.root / receipt["partitions"][0]["path"]
    path.unlink()
    broken = archive.verify_receipt(receipt)
    assert not broken["ok"]
    assert any("分区文件不存在" in message for message in broken["mismatches"])

    # 模型标识漂移：回执里的 001 改成 002 应被判为不一致。
    tampered = dict(receipt)
    tampered["models"] = ["002"]
    drift = archive.verify_receipt(tampered)
    assert not drift["ok"]
    assert any("模型标识" in message for message in drift["mismatches"])


if __name__ == "__main__":
    tests = [
        test_roundtrip_preserves_leading_zero_score_and_empty_label,
        test_score_parsing_rejects_lossy_values,
        test_field_mapping_from_batch_task_is_reusable,
        test_daily_mode_switches_ready_day_and_keeps_failed_day_on_csv,
        test_batch_mode_changes_day_one_storage_format,
        test_fix_bad_row_then_resume_keeps_dates_and_row_counts,
        test_interrupted_resume_does_not_duplicate_or_lose_rows,
        test_data_export_registers_readable_parquet_partitions,
        test_verify_receipt_detects_missing_partition_and_model_drift,
    ]
    passed = failed = 0
    for test in tests:
        try:
            test()
            print(f"[PASS] {test.__name__}")
            passed += 1
        except Exception as exc:  # noqa: BLE001 - 测试入口统一打印失败原因
            print(f"[FAIL] {test.__name__}: {exc}")
            failed += 1
    print(f"\n总测试: {len(tests)}, 通过: {passed}, 失败: {failed}")
    if failed:
        sys.exit(1)
