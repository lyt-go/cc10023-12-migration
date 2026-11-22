"""存量预测 CSV → Parquet 的分区归档与读切换。

批量预测任务最初的产物是 CSV，字段映射固定为
``prediction_id,model_id,score,label,dt``。本模块把存量 CSV 按 ``dt`` 迁移成
Snappy 压缩的列式分区，并管理读入口的切换：

* ``model_id`` 全程按字符串处理，``001`` 回读后仍然是 ``001``；
* ``score`` 以 ``decimal128(38, 18)`` 存储，避免浮点回读丢精度；超出 18 位
  小数且无法无损表示的值判为坏行，而不是悄悄四舍五入；
* ``label`` 的空值写成 NULL，回读为 ``None``，与“空标签”的原含义一致；
* 每个数据日期是一套完整输入，只有该日期所有行都解析成功才切换；
* 失败/未切换的日期继续读原始 CSV，坏行用 ``source_line`` 定位；
* ``daily`` 模式逐日切换，``batch`` 模式整批成功后才切换；
* 归档索引里的 ``data_date`` 永远是源数据日期，而不是迁移执行日期。

同一日期重复迁移是“校验通过后原子覆盖”，且读入口在索引提交前始终指向旧
CSV，因此中断后恢复不会产生重复行或丢行。
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import tempfile
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from threading import RLock
from typing import Any, Iterable

try:  # POSIX 下用文件锁串行化跨进程迁移；其他平台退化为进程内锁。
    import fcntl
except ImportError:  # pragma: no cover - 仅非 POSIX 环境
    fcntl = None  # type: ignore[assignment]

PREDICTION_FIELDS = ["prediction_id", "model_id", "score", "label", "dt"]
DEFAULT_FIELD_MAPPING = ",".join(PREDICTION_FIELDS)

SCORE_PRECISION = 38
SCORE_SCALE = 18
SCORE_QUANTUM = Decimal(1).scaleb(-SCORE_SCALE)
SCORE_LIMIT = Decimal(10) ** (SCORE_PRECISION - SCORE_SCALE)

MIGRATION_MODES = ("daily", "batch")

_INDEX_LOCK = RLock()
_MIGRATION_LOCK = RLock()


def default_root() -> Path:
    """迁移工作区；部署或测试时可用 ``PREDICTION_DATA_ROOT`` 覆盖。"""
    configured = os.environ.get("PREDICTION_DATA_ROOT")
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parents[2] / "var" / "predictions"


def _arrow_modules():
    """延迟导入 pyarrow：只有真正读写 Parquet 时才要求列式依赖存在。"""
    import pyarrow as pa  # type: ignore[import-not-found]
    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    return pa, pq


def prediction_schema():
    """与存量 CSV 字段一一对应的列式 schema。"""
    pa, _ = _arrow_modules()
    return pa.schema(
        [
            pa.field("prediction_id", pa.string(), nullable=False),
            pa.field("model_id", pa.string(), nullable=False),
            pa.field("score", pa.decimal128(SCORE_PRECISION, SCORE_SCALE), nullable=False),
            pa.field("label", pa.string(), nullable=True),
            pa.field("dt", pa.date32(), nullable=False),
        ]
    )


# ── 字段解析 ──────────────────────────────────────────────────────────────

def parse_score(raw: Any) -> Decimal:
    """把 CSV 分数解析成可无损写入 ``decimal128(38,18)`` 的 Decimal。"""
    text = "" if raw is None else str(raw).strip()
    if not text:
        raise ValueError("score 为空")
    try:
        value = Decimal(text)
    except (InvalidOperation, ValueError):
        raise ValueError(f"score 无法解析为小数：{text!r}") from None
    if not value.is_finite():
        raise ValueError(f"score 不是有限小数：{text!r}")
    with localcontext() as context:
        context.prec = 80
        if value.as_tuple().exponent < -SCORE_SCALE:
            quantized = value.quantize(SCORE_QUANTUM)
            if quantized != value:
                raise ValueError(
                    f"score 超过 {SCORE_SCALE} 位小数，无法无损迁移：{text!r}"
                )
            value = quantized
        if abs(value) >= SCORE_LIMIT:
            raise ValueError(f"score 超出 decimal128(38,18) 取值范围：{text!r}")
        return value


def format_score(value: Any) -> str:
    """把 Decimal 还原为普通十进制文本：不带指数、无多余尾零。"""
    if value is None:
        return ""
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    text = format(decimal_value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if text in ("", "-0"):
        return "0"
    return text


def parse_dt(raw: Any) -> str:
    """解析源数据日期，统一成 ``YYYY-MM-DD`` 分区键。"""
    text = "" if raw is None else str(raw).strip()
    if not text:
        raise ValueError("dt 为空")
    candidate = text[:10]
    if len(text) > 10 and text[10] not in ("T", " "):
        raise ValueError(f"dt 不是日期格式：{text!r}")
    try:
        return date.fromisoformat(candidate).isoformat()
    except ValueError:
        raise ValueError(f"dt 不是日期格式：{text!r}") from None


def parse_dt_safe(raw: Any) -> str | None:
    try:
        return parse_dt(raw)
    except ValueError:
        return None


def normalize_label(raw: Any) -> str | None:
    """空标签写成 NULL；``0`` 等非空标签保留为字符串。"""
    if raw is None:
        return None
    text = str(raw).strip()
    return text if text else None


def parse_field_mapping(mapping: str | dict[str, str] | None) -> dict[str, str]:
    """解析批量预测任务已有的字段映射，返回 ``目标字段 -> CSV 列名``。

    支持 ``prediction_id,model_id,score,label,dt`` 这种同名映射，也支持
    ``prediction_id=pred_id,model_id=model_code,...`` 这种列名不同的存量数据。
    """
    if mapping is None:
        mapping = DEFAULT_FIELD_MAPPING
    if isinstance(mapping, dict):
        pairs = {str(key).strip(): str(value).strip() for key, value in mapping.items()}
    else:
        pairs = {}
        for token in str(mapping).split(","):
            token = token.strip()
            if not token:
                continue
            if "=" in token:
                target, source = token.split("=", 1)
            else:
                target = source = token
            pairs[target.strip()] = source.strip()
    unknown = sorted(set(pairs) - set(PREDICTION_FIELDS))
    if unknown:
        raise ValueError(f"字段映射包含未知预测字段：{', '.join(unknown)}")
    missing = [field for field in PREDICTION_FIELDS if not pairs.get(field)]
    if missing:
        raise ValueError(f"字段映射缺少字段：{', '.join(missing)}")
    return {field: pairs[field] for field in PREDICTION_FIELDS}


def _safe_name(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("._")
    return cleaned or "dataset"


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def _row_digest(raw_rows: Iterable[dict[str, Any]]) -> str:
    """源行内容摘要，用于判断某个日期的输入是否仍是同一套。"""
    digest = hashlib.sha256()
    for raw in raw_rows:
        digest.update(
            json.dumps(
                [raw.get(field) for field in PREDICTION_FIELDS],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        digest.update(b"\n")
    return digest.hexdigest()


# ── 归档与迁移 ────────────────────────────────────────────────────────────

class PredictionArchive:
    """按数据日期管理预测分区的迁移、归档索引与回读。"""

    def __init__(self, root: str | Path | None = None) -> None:
        self.root = Path(root).resolve() if root is not None else default_root().resolve()

    # ── 路径 ────────────────────────────────────────────────────────────
    @property
    def index_path(self) -> Path:
        return self.root / "archive" / "index.json"

    def partition_path(self, dt: str, revision: str | None = None) -> Path:
        name = f"part-{revision}.parquet" if revision else "part-00000.parquet"
        return self.root / "parquet" / f"dt={dt}" / name

    def _receipt_dir(self, dataset: str) -> Path:
        return self.root / "receipts" / _safe_name(dataset)

    def _latest_receipt_path(self, dataset: str) -> Path:
        return self._receipt_dir(dataset) / "latest.json"

    @contextmanager
    def _migration_lock(self):
        """迁移写索引必须串行：同一天不能被两个进程/线程同时切换。"""
        lock_path = self.root / "archive" / ".migration.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with _MIGRATION_LOCK, lock_path.open("a+") as handle:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                if fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    # ── 源 CSV ─────────────────────────────────────────────────────────
    def read_source(
        self,
        source_csv: str | Path,
        field_mapping: str | dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """读取存量 CSV，保留原始文本、物理行号和映射后的规范化字段。

        ``field_mapping`` 与批量预测任务已有的字段映射共用一套含义：把 CSV
        里的列名映射到 ``prediction_id/model_id/score/label/dt``。
        """
        path = Path(source_csv)
        if not path.is_absolute():
            path = path.resolve()
        if not path.exists():
            raise FileNotFoundError(f"存量预测 CSV 不存在：{path}")
        columns = parse_field_mapping(field_mapping)
        records: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            header = list(reader.fieldnames or [])
            missing = [column for column in columns.values() if column not in header]
            if missing:
                raise ValueError(f"CSV 缺少字段：{', '.join(missing)}")
            for row in reader:
                raw = {field: row.get(column) for field, column in columns.items()}
                records.append({"source_line": reader.line_num, "raw": raw})
        return {"path": str(path), "header": header, "records": records,
                "field_mapping": ",".join(f"{k}={v}" for k, v in columns.items())}

    @staticmethod
    def _parse_record(record: dict[str, Any]) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        raw = record["raw"]
        line = int(record["source_line"])
        issues: list[dict[str, Any]] = []

        prediction_id = "" if raw.get("prediction_id") is None else str(raw["prediction_id"]).strip()
        if not prediction_id:
            issues.append({"field": "prediction_id", "source_line": line,
                           "raw_value": raw.get("prediction_id"),
                           "reason": "prediction_id 为空"})

        model_id = "" if raw.get("model_id") is None else str(raw["model_id"]).strip()
        if not model_id:
            issues.append({"field": "model_id", "source_line": line,
                           "raw_value": raw.get("model_id"), "reason": "model_id 为空"})

        score: Decimal | None = None
        try:
            score = parse_score(raw.get("score"))
        except ValueError as exc:
            issues.append({"field": "score", "source_line": line,
                           "raw_value": raw.get("score"), "reason": str(exc)})

        label = normalize_label(raw.get("label"))

        dt: str | None = None
        try:
            dt = parse_dt(raw.get("dt"))
        except ValueError as exc:
            issues.append({"field": "dt", "source_line": line,
                           "raw_value": raw.get("dt"), "reason": str(exc)})

        if issues:
            return None, issues
        return {
            "prediction_id": prediction_id,
            "model_id": model_id,
            "score": score,
            "label": label,
            "dt": dt,
            "source_line": line,
        }, []

    def _scan_source(self, source: dict[str, Any]) -> dict[str, Any]:
        """一次扫描同时产出按日期分组的有效行、逐日期坏行和无法归档的坏行。"""
        rows_by_dt: dict[str, list[dict[str, Any]]] = {}
        raw_by_dt: dict[str, list[dict[str, Any]]] = {}
        issues_by_dt: dict[str, list[dict[str, Any]]] = {}
        orphan_issues: list[dict[str, Any]] = []
        lines_by_dt: dict[str, set[int]] = {}
        for record in source["records"]:
            line = int(record["source_line"])
            dt = parse_dt_safe(record["raw"].get("dt"))
            if dt is None:
                _, issues = self._parse_record(record)
                orphan_issues.extend(issues)
                continue
            lines_by_dt.setdefault(dt, set()).add(line)
            raw_by_dt.setdefault(dt, []).append(record["raw"])
            row, issues = self._parse_record(record)
            if row is None:
                issues_by_dt.setdefault(dt, []).extend(issues)
            else:
                rows_by_dt.setdefault(dt, []).append(row)
        return {
            "rows_by_dt": rows_by_dt,
            "raw_by_dt": raw_by_dt,
            "issues_by_dt": issues_by_dt,
            "orphan_issues": orphan_issues,
            "lines_by_dt": lines_by_dt,
        }

    # ── 归档索引 ───────────────────────────────────────────────────────
    def _load_index(self, *, strict: bool = False) -> dict[str, Any] | None:
        """读取归档索引。

        ``strict=True`` 时，索引存在但无法解析会抛出异常，供清理旧文件等
        破坏性操作在索引状态不明时直接放弃，而不是把它当成空索引。
        """
        with _INDEX_LOCK:
            if not self.index_path.exists():
                return {"version": 1, "datasets": {}}
            try:
                with self.index_path.open("r", encoding="utf-8") as stream:
                    payload = json.load(stream)
            except (json.JSONDecodeError, OSError):
                if strict:
                    raise
                return {"version": 1, "datasets": {}}
        if not isinstance(payload, dict):
            if strict:
                raise ValueError("归档索引结构损坏")
            return {"version": 1, "datasets": {}}
        payload.setdefault("version", 1)
        payload.setdefault("datasets", {})
        return payload

    def _save_index(self, payload: dict[str, Any]) -> None:
        with _INDEX_LOCK:
            _atomic_write_json(self.index_path, payload)

    def archive_index(self, dataset: str | None = None) -> dict[str, Any]:
        """返回归档索引；指定 ``dataset`` 时只返回该数据集。"""
        payload = self._load_index()
        if dataset is None:
            return payload
        entry = payload.get("datasets", {}).get(dataset, {})
        return {
            "version": payload.get("version", 1),
            "dataset": dataset,
            "source_csv": entry.get("source_csv"),
            "updated_at": entry.get("updated_at"),
            "partitions": entry.get("partitions", {}),
        }

    @staticmethod
    def _dataset_entry(index: dict[str, Any], dataset: str) -> dict[str, Any]:
        datasets = index.setdefault("datasets", {})
        entry = datasets.setdefault(dataset, {"partitions": {}})
        entry.setdefault("partitions", {})
        return entry

    def latest_receipt(self, dataset: str) -> dict[str, Any] | None:
        path = self._latest_receipt_path(dataset)
        if not path.exists():
            return None
        try:
            with path.open("r", encoding="utf-8") as stream:
                payload = json.load(stream)
        except (json.JSONDecodeError, OSError):
            return None
        return payload if isinstance(payload, dict) else None

    def list_receipts(self, dataset: str) -> list[dict[str, Any]]:
        """按时间倒序列出某个数据集的全部迁移回执。"""
        receipt_dir = self._receipt_dir(dataset)
        if not receipt_dir.exists():
            return []
        receipts: list[dict[str, Any]] = []
        for path in receipt_dir.glob("*.json"):
            if path.name == "latest.json":
                continue
            try:
                with path.open("r", encoding="utf-8") as stream:
                    payload = json.load(stream)
            except (json.JSONDecodeError, OSError):
                continue
            if isinstance(payload, dict):
                receipts.append(payload)
        receipts.sort(key=lambda item: str(item.get("created_at", "")), reverse=True)
        return receipts

    def _save_receipt(self, dataset: str, receipt: dict[str, Any]) -> Path:
        receipt_dir = self._receipt_dir(dataset)
        path = receipt_dir / f"{receipt['migration_id']}.json"
        _atomic_write_json(path, receipt)
        _atomic_write_json(self._latest_receipt_path(dataset), receipt)
        return path

    # ── Parquet 分区写入/回读 ──────────────────────────────────────────
    def _write_partition(
        self,
        *,
        dataset: str,
        dt: str,
        rows: list[dict[str, Any]],
        source_csv: str,
        source_digest: str,
        task_id: int | None,
        migration_id: str,
        field_mapping: str,
    ) -> dict[str, Any]:
        pa, pq = _arrow_modules()
        records = [
            {
                "prediction_id": row["prediction_id"],
                "model_id": row["model_id"],
                "score": row["score"],
                "label": row["label"],
                "dt": date.fromisoformat(str(row["dt"])),
            }
            for row in rows
        ]
        table = pa.Table.from_pylist(records, schema=prediction_schema())
        path = self.partition_path(dt, migration_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        handle, tmp_name = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        os.close(handle)
        metadata = {
            "dataset": dataset,
            "data_date": dt,
            "source_csv": source_csv,
            "source_rows": str(len(rows)),
            "source_digest": source_digest,
            "field_mapping": field_mapping,
            "migration_id": migration_id,
        }
        if task_id is not None:
            metadata["task_id"] = str(task_id)
        existing_metadata = dict(table.schema.metadata or {})
        existing_metadata.update(
            {str(key).encode("utf-8"): str(value).encode("utf-8")
             for key, value in metadata.items()}
        )
        table = table.replace_schema_metadata(existing_metadata)
        try:
            pq.write_table(
                table,
                tmp_name,
                compression="snappy",
                version="2.6",
                use_dictionary=True,
                write_statistics=True,
            )
            os.replace(tmp_name, path)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except FileNotFoundError:
                pass
            raise
        committed_at = _now()
        return {
            "dt": dt,
            "data_date": dt,
            "format": "parquet",
            "compression": "snappy",
            "path": str(path.relative_to(self.root)),
            "rows": len(rows),
            "source_rows": len(rows),
            "source_digest": source_digest,
            "field_mapping": field_mapping,
            "migration_id": migration_id,
            "committed_at": committed_at,
        }

    def read_partition_file(self, path: str | Path) -> list[dict[str, Any]]:
        """按原始字段名回读一个 Parquet 分区。

        用 ``ParquetFile`` 直接读单文件，避免数据集发现把 ``dt=`` 目录名解释成
        额外的 Hive 分区列而与文件内的 ``dt`` 冲突。
        """
        _, pq = _arrow_modules()
        table = pq.ParquetFile(Path(path)).read()
        rows: list[dict[str, Any]] = []
        for record in table.to_pylist():
            dt_value = record["dt"]
            rows.append({
                "prediction_id": record["prediction_id"],
                "model_id": record["model_id"],
                "score": format_score(record["score"]),
                "label": record["label"],
                "dt": dt_value.isoformat() if hasattr(dt_value, "isoformat") else str(dt_value),
            })
        return rows

    def partition_compression(self, path: str | Path) -> list[str]:
        """读取分区所有列块的压缩算法，用于迁移回执核对。"""
        _, pq = _arrow_modules()
        metadata = pq.ParquetFile(Path(path)).metadata
        return [
            str(metadata.row_group(row_group).column(column).compression)
            for row_group in range(metadata.num_row_groups)
            for column in range(metadata.row_group(row_group).num_columns)
        ]

    # ── 读入口：已切换读 Parquet，未切换读 CSV ─────────────────────────
    def read(
        self,
        dt: str,
        dataset: str,
        include_source: bool = False,
        source_csv: str | Path | None = None,
        field_mapping: str | dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """按数据日期读取报告：索引有已提交分区读列式文件，否则回退 CSV。"""
        normalized_dt = parse_dt(dt)
        index = self._load_index()
        entry = index.get("datasets", {}).get(dataset, {})
        partition = entry.get("partitions", {}).get(normalized_dt)
        if partition:
            path = self.root / str(partition["path"])
            if path.exists():
                rows = self.read_partition_file(path)
                return {
                    "ok": True,
                    "dt": normalized_dt,
                    "format": "parquet",
                    "compression": partition.get("compression", "snappy"),
                    "path": str(partition["path"]),
                    "row_count": len(rows),
                    "complete": True,
                    "issues": [],
                    "rows": rows,
                }
            # 索引指向的文件丢失：宁可回退 CSV，也不给业务半份结果。
        csv_path = source_csv or entry.get("source_csv")
        if not csv_path:
            return {
                "ok": False,
                "dt": normalized_dt,
                "format": None,
                "row_count": 0,
                "complete": False,
                "issues": [],
                "rows": [],
                "error": f"数据集 {dataset} 没有可读的源 CSV 或 Parquet 分区",
            }
        source = self.read_source(
            csv_path, field_mapping or entry.get("field_mapping"))
        rows: list[dict[str, Any]] = []
        issues: list[dict[str, Any]] = []
        for record in source["records"]:
            if parse_dt_safe(record["raw"].get("dt")) != normalized_dt:
                continue
            parsed, row_issues = self._parse_record(record)
            if parsed is None:
                issues.extend(row_issues)
                raw = record["raw"]
                rows.append({
                    "prediction_id": raw.get("prediction_id"),
                    "model_id": raw.get("model_id"),
                    "score": raw.get("score"),
                    "label": normalize_label(raw.get("label")),
                    "dt": normalized_dt,
                    "_valid": False,
                    "_issues": row_issues,
                    "_source_line": int(record["source_line"]),
                })
            else:
                row = {
                    "prediction_id": parsed["prediction_id"],
                    "model_id": parsed["model_id"],
                    "score": format_score(parsed["score"]),
                    "label": parsed["label"],
                    "dt": parsed["dt"],
                    "_valid": True,
                }
                if include_source:
                    row["_source_line"] = parsed["source_line"]
                rows.append(row)
        return {
            "ok": True,
            "dt": normalized_dt,
            "format": "csv",
            "compression": None,
            "path": str(csv_path),
            "row_count": len(rows),
            "complete": not issues,
            "issues": issues,
            "rows": rows,
        }

    # ── 迁移 ───────────────────────────────────────────────────────────
    def migrate(
        self,
        source_csv: str | Path,
        dataset: str,
        *,
        mode: str = "daily",
        dates: list[str] | None = None,
        task_id: int | None = None,
        resume: bool = True,
        force: bool = False,
        field_mapping: str = DEFAULT_FIELD_MAPPING,
    ) -> dict[str, Any]:
        """把存量预测 CSV 迁移为按数据日期的 Snappy 列式分区。

        ``mode="daily"``：成功日期立即切换，失败日期继续读 CSV。
        ``mode="batch"``：所有目标日期都成功才一起切换，否则整批保持 CSV。
        ``resume=True``：源内容未变化的已提交日期直接跳过，不重复写入。
        """
        mode = (mode or "daily").strip().lower()
        migration_id = f"MIG-{datetime.now().strftime('%Y%m%dT%H%M%S%f')}"
        source_path = str(Path(source_csv).resolve())

        if mode not in MIGRATION_MODES:
            return self._error_receipt(
                dataset, migration_id, task_id, mode, source_path, field_mapping,
                f"不支持的切换模式：{mode}")
        try:
            columns = parse_field_mapping(field_mapping)
            normalized_mapping = ",".join(f"{k}={v}" for k, v in columns.items())
            field_mapping = normalized_mapping
            source = self.read_source(source_path, field_mapping)
        except (FileNotFoundError, ValueError) as exc:
            return self._error_receipt(
                dataset, migration_id, task_id, mode, source_path,
                str(field_mapping), str(exc))

        with self._migration_lock():
            return self._migrate_locked(
                source,
                dataset,
                source_path=source_path,
                migration_id=migration_id,
                mode=mode,
                dates=dates,
                task_id=task_id,
                resume=resume,
                force=force,
                field_mapping=field_mapping,
            )

    def _migrate_locked(
        self,
        source: dict[str, Any],
        dataset: str,
        *,
        source_path: str,
        migration_id: str,
        mode: str,
        dates: list[str] | None,
        task_id: int | None,
        resume: bool,
        force: bool,
        field_mapping: str,
    ) -> dict[str, Any]:
        scan = self._scan_source(source)
        rows_by_dt: dict[str, list[dict[str, Any]]] = scan["rows_by_dt"]
        raw_by_dt: dict[str, list[dict[str, Any]]] = scan["raw_by_dt"]
        issues_by_dt: dict[str, list[dict[str, Any]]] = scan["issues_by_dt"]
        lines_by_dt: dict[str, set[int]] = scan["lines_by_dt"]
        orphan_issues: list[dict[str, Any]] = scan["orphan_issues"]

        source_dates = sorted(set(lines_by_dt))
        if dates is None:
            target_dates = list(source_dates)
        else:
            try:
                target_dates = sorted({parse_dt(value) for value in dates})
            except ValueError as exc:
                return self._error_receipt(
                    dataset, migration_id, task_id, mode, source_path,
                    field_mapping, str(exc))

        index = self._load_index()
        entry = self._dataset_entry(index, dataset)
        entry["source_csv"] = source_path
        entry["field_mapping"] = field_mapping
        entry["updated_at"] = _now()
        # 先落盘源 CSV 指向，保证迁移失败时读入口仍能按日期回退到旧 CSV。
        self._save_index(index)
        partitions: dict[str, Any] = entry["partitions"]

        plans: dict[str, dict[str, Any]] = {}
        for dt in target_dates:
            raw_rows = raw_by_dt.get(dt, [])
            digest = _row_digest(raw_rows)
            existing = partitions.get(dt)
            file_present = bool(existing) and (self.root / str(existing.get("path", ""))).exists()
            already_current = (
                not force
                and resume
                and existing is not None
                and existing.get("source_digest") == digest
                and file_present
            )
            plans[dt] = {
                "dt": dt,
                "rows": rows_by_dt.get(dt, []),
                "source_rows": len(raw_rows),
                "digest": digest,
                "existing": existing,
                "already_current": already_current,
                "issues": issues_by_dt.get(dt, []),
                "present": dt in lines_by_dt,
            }

        switched: list[str] = []
        skipped: list[str] = []
        invalidated: list[str] = []
        block_reason: str | None = None

        def commit(dt: str) -> None:
            plan = plans[dt]
            written = self._write_partition(
                dataset=dataset,
                dt=dt,
                rows=plan["rows"],
                source_csv=source_path,
                source_digest=plan["digest"],
                task_id=task_id,
                migration_id=migration_id,
                field_mapping=field_mapping,
            )
            partitions[dt] = written
            entry["updated_at"] = _now()
            self._save_index(index)
            switched.append(dt)

        def drop_stale(dt: str) -> None:
            if partitions.pop(dt, None) is not None:
                invalidated.append(dt)
                entry["updated_at"] = _now()
                self._save_index(index)

        if mode == "batch":
            blocking = bool(orphan_issues)
            for plan in plans.values():
                if not plan["present"]:
                    blocking = True
                if plan["issues"] and not plan["already_current"]:
                    blocking = True
            if blocking:
                block_reason = "batch_validation_failed"
                for dt in target_dates:
                    if not plans[dt]["already_current"]:
                        drop_stale(dt)
            else:
                for dt in target_dates:
                    if plans[dt]["already_current"]:
                        skipped.append(dt)
                    else:
                        commit(dt)
        else:
            for dt in target_dates:
                plan = plans[dt]
                if plan["already_current"]:
                    skipped.append(dt)
                    continue
                if plan["issues"] or not plan["present"]:
                    drop_stale(dt)
                    continue
                commit(dt)

        # 索引切换后才清理不再被引用的旧 revision，中断时旧文件仍在。
        self._prune_superseded(dataset)

        receipt = self._build_receipt(
            dataset=dataset,
            migration_id=migration_id,
            task_id=task_id,
            mode=mode,
            source_path=source_path,
            field_mapping=field_mapping,
            source=source,
            scan=scan,
            target_dates=target_dates,
            plans=plans,
            switched=switched,
            skipped=skipped,
            invalidated=invalidated,
            orphan_issues=orphan_issues,
            block_reason=block_reason,
        )
        receipt["receipt_path"] = str(self._save_receipt(dataset, receipt))
        return receipt

    def _prune_superseded(self, dataset: str) -> None:
        """删除所有索引都不再引用的旧 revision，避免残留同日期多份数据。"""
        try:
            index = self._load_index(strict=True)
        except (json.JSONDecodeError, OSError, ValueError):
            # 索引不明时不删任何文件，宁可留下暂存文件也不误删生效分区。
            return
        if index is None:
            return
        referenced = {
            str(part.get("path"))
            for entry in index.get("datasets", {}).values()
            for part in (entry.get("partitions") or {}).values()
            if part.get("path")
        }
        base = self.root / "parquet"
        if not base.exists():
            return
        for path in base.glob("dt=*/part-*.parquet"):
            if str(path.relative_to(self.root)) in referenced:
                continue
            try:
                path.unlink()
            except FileNotFoundError:
                continue

    def _error_receipt(
        self,
        dataset: str,
        migration_id: str,
        task_id: int | None,
        mode: str,
        source_path: str,
        field_mapping: str,
        reason: str,
    ) -> dict[str, Any]:
        receipt = {
            "ok": False,
            "migration_id": migration_id,
            "dataset": dataset,
            "task_id": task_id,
            "mode": mode,
            "source_csv": source_path,
            "field_mapping": field_mapping,
            "source_rows": 0,
            "migrated_rows": 0,
            "models": [],
            "partitions": [],
            "failures": [{"dt": None, "issues": [{
                "field": "source_csv", "source_line": 0,
                "raw_value": source_path, "reason": reason}]}],
            "switch": {"mode": mode, "committed_dates": [], "switched_dates": [],
                       "skipped_dates": [], "failed_dates": [], "block_reason": "source_unreadable"},
            "archive_index": str(self.index_path),
            "created_at": _now(),
        }
        receipt["receipt_path"] = str(self._save_receipt(dataset, receipt))
        return receipt

    def _build_receipt(
        self,
        *,
        dataset: str,
        migration_id: str,
        task_id: int | None,
        mode: str,
        source_path: str,
        field_mapping: str,
        source: dict[str, Any],
        scan: dict[str, Any],
        target_dates: list[str],
        plans: dict[str, dict[str, Any]],
        switched: list[str],
        skipped: list[str],
        invalidated: list[str],
        orphan_issues: list[dict[str, Any]],
        block_reason: str | None,
    ) -> dict[str, Any]:
        final_partitions = (
            self._load_index().get("datasets", {}).get(dataset, {}).get("partitions", {})
        )
        receipt_partitions: list[dict[str, Any]] = []
        committed_dates: list[str] = []
        failed_dates: list[str] = []
        for dt in target_dates:
            plan = plans[dt]
            committed = final_partitions.get(dt)
            date_issues = plan["issues"]
            if committed:
                path = self.root / str(committed["path"])
                committed_dates.append(dt)
                if not path.exists():
                    receipt_partitions.append({
                        "dt": dt, "data_date": dt, "status": "missing",
                        "format": "parquet", "compression": "snappy",
                        "path": committed["path"], "rows": 0,
                        "source_rows": plan["source_rows"],
                        "source_digest": committed.get("source_digest"),
                        "readable": False,
                    })
                    continue
                receipt_partitions.append({
                    "dt": dt,
                    "data_date": dt,
                    "status": "committed",
                    "format": "parquet",
                    "compression": committed.get("compression", "snappy"),
                    "path": committed["path"],
                    "absolute_path": str(path),
                    "rows": int(committed.get("rows", 0)),
                    "source_rows": int(committed.get("source_rows", plan["source_rows"])),
                    "source_digest": committed.get("source_digest"),
                    "readable": True,
                })
                continue
            failed_dates.append(dt)
            status = "failed" if date_issues else "fallback"
            receipt_partitions.append({
                "dt": dt,
                "data_date": dt,
                "status": status,
                "format": "csv",
                "compression": None,
                "path": source_path,
                "rows": plan["source_rows"],
                "source_rows": plan["source_rows"],
                "source_digest": plan["digest"],
                "readable": True,
                "reason": block_reason or ("invalid_rows" if date_issues else "not_migrated"),
                "issues": date_issues,
                "source_lines": sorted({int(i["source_line"]) for i in date_issues}),
            })

        models = sorted({
            str(record["raw"].get("model_id")).strip()
            for record in source["records"]
            if record["raw"].get("model_id") is not None
        })
        migrated_rows = sum(
            int(part.get("rows", 0)) for part in receipt_partitions
            if part.get("status") == "committed"
        )
        ok = (
            not orphan_issues
            and not any(part.get("status") in ("failed", "missing", "fallback")
                        for part in receipt_partitions)
        )
        failures: list[dict[str, Any]] = []
        for dt in target_dates:
            if plans[dt]["issues"]:
                failures.append({"dt": dt, "issues": plans[dt]["issues"]})
        if orphan_issues:
            failures.append({"dt": None, "issues": orphan_issues})

        return {
            "ok": ok,
            "migration_id": migration_id,
            "dataset": dataset,
            "task_id": task_id,
            "mode": mode,
            "source_csv": source_path,
            "field_mapping": field_mapping,
            "source_rows": len(source["records"]),
            "migrated_rows": migrated_rows,
            "models": models,
            "partitions": receipt_partitions,
            "failures": failures,
            "switch": {
                "mode": mode,
                "committed_dates": sorted(committed_dates),
                "switched_dates": sorted(switched),
                "skipped_dates": sorted(skipped),
                "invalidated_dates": sorted(invalidated),
                "failed_dates": sorted(failed_dates),
                "block_reason": block_reason,
            },
            "archive_index": str(self.index_path),
            "created_at": _now(),
        }

    # ── 回执核对 ───────────────────────────────────────────────────────
    def verify_receipt(
        self,
        receipt: dict[str, Any],
        *,
        source_csv: str | Path | None = None,
    ) -> dict[str, Any]:
        """按迁移回执核对源行数、模型标识和分区去向。"""
        mismatches: list[str] = []
        checks: dict[str, Any] = {}
        dataset = str(receipt.get("dataset") or "")
        source_path = str(source_csv or receipt.get("source_csv") or "")
        source = self.read_source(source_path, receipt.get("field_mapping"))
        records = source["records"]

        checks["source_rows"] = {
            "receipt": receipt.get("source_rows"),
            "actual": len(records),
            "ok": receipt.get("source_rows") == len(records),
        }
        if not checks["source_rows"]["ok"]:
            mismatches.append(
                f"源行数不一致：回执 {receipt.get('source_rows')}，实际 {len(records)}")

        actual_models = sorted({
            str(record["raw"].get("model_id")).strip()
            for record in records
            if record["raw"].get("model_id") is not None
        })
        checks["models"] = {
            "receipt": sorted(receipt.get("models") or []),
            "actual": actual_models,
            "ok": sorted(receipt.get("models") or []) == actual_models,
        }
        if not checks["models"]["ok"]:
            mismatches.append("模型标识与源 CSV 不一致")

        source_counts: dict[str, int] = {}
        source_by_line: dict[int, dict[str, Any]] = {}
        source_digests: dict[str, str] = {}
        raw_by_dt: dict[str, list[dict[str, Any]]] = {}
        for record in records:
            source_by_line[int(record["source_line"])] = record
            dt = parse_dt_safe(record["raw"].get("dt"))
            if dt is not None:
                source_counts[dt] = source_counts.get(dt, 0) + 1
                raw_by_dt.setdefault(dt, []).append(record["raw"])
        for dt, raw_rows in raw_by_dt.items():
            source_digests[dt] = _row_digest(raw_rows)

        partition_checks: list[dict[str, Any]] = []
        parquet_rows_total = 0
        fallback_rows_total = 0
        seen_ids: list[str] = []
        target_dates: set[str] = set()
        for part in receipt.get("partitions") or []:
            dt = str(part.get("dt"))
            target_dates.add(dt)
            item: dict[str, Any] = {"dt": dt, "status": part.get("status"),
                                    "format": part.get("format")}
            if part.get("status") == "committed":
                path = self.root / str(part.get("path"))
                item["path"] = str(path)
                if not path.exists():
                    item["ok"] = False
                    item["error"] = "分区文件不存在"
                    mismatches.append(f"{dt} 分区文件不存在：{path}")
                    partition_checks.append(item)
                    continue
                rows = self.read_partition_file(path)
                compression = self.partition_compression(path)
                item["rows"] = len(rows)
                item["source_rows"] = source_counts.get(dt, 0)
                item["compression"] = sorted(set(compression))
                item["dt_matches"] = all(row["dt"] == dt for row in rows)
                item["rows_ok"] = len(rows) == source_counts.get(dt, 0)
                item["source_digest_matches"] = (
                    part.get("source_digest") == source_digests.get(dt))
                item["compression_ok"] = bool(compression) and all(
                    value.upper() == "SNAPPY" for value in compression)
                item["ok"] = (item["rows_ok"] and item["dt_matches"]
                              and item["compression_ok"]
                              and item["source_digest_matches"])
                if not item["rows_ok"]:
                    mismatches.append(
                        f"{dt} 分区行数 {len(rows)} 与源 {source_counts.get(dt, 0)} 不一致")
                if not item["dt_matches"]:
                    mismatches.append(f"{dt} 分区内出现其他数据日期")
                if not item["source_digest_matches"]:
                    mismatches.append(f"{dt} 分区内容摘要与当前源 CSV 不一致")
                if not item["compression_ok"]:
                    mismatches.append(f"{dt} 分区未使用 Snappy 压缩")
                parquet_rows_total += len(rows)
                seen_ids.extend(str(row["prediction_id"]) for row in rows)
            else:
                # 失败或未切换日期仍按原 CSV 提供完整输入，坏行能用行号定位。
                item["rows"] = source_counts.get(dt, 0)
                item["source_rows"] = source_counts.get(dt, 0)
                item["ok"] = item["rows"] == item["source_rows"]
                fallback_rows_total += item["rows"]
                for issue in part.get("issues") or []:
                    line = int(issue.get("source_line") or 0)
                    record = source_by_line.get(line)
                    issue_ok = (record is not None
                                and parse_dt_safe(record["raw"].get("dt")) == dt)
                    if not issue_ok:
                        mismatches.append(f"{dt} 坏行行号 {line} 无法在源 CSV 中定位")
                        item["ok"] = False
            partition_checks.append(item)

        checks["partitions"] = partition_checks
        expected_rows = sum(source_counts.get(dt, 0) for dt in target_dates)
        coverage = parquet_rows_total + fallback_rows_total
        checks["coverage"] = {
            "parquet_rows": parquet_rows_total,
            "fallback_rows": fallback_rows_total,
            "total": coverage,
            "source_rows": expected_rows,
            "ok": coverage == expected_rows,
        }
        if coverage != expected_rows:
            mismatches.append(f"读回总行数 {coverage} 与源行数 {expected_rows} 不一致")
        duplicate_ids = sorted({pid for pid in seen_ids if seen_ids.count(pid) > 1})
        checks["duplicates"] = {"duplicate_prediction_ids": duplicate_ids,
                                "ok": not duplicate_ids}
        if duplicate_ids:
            mismatches.append(f"分区内重复 prediction_id：{', '.join(duplicate_ids[:5])}")

        # 归档索引必须把每个已提交分区关联回它原来的数据日期。
        index = self._load_index()
        indexed = index.get("datasets", {}).get(dataset, {}).get("partitions", {})
        index_checks: list[dict[str, Any]] = []
        for part in receipt.get("partitions") or []:
            if part.get("status") != "committed":
                continue
            dt = str(part.get("dt"))
            entry = indexed.get(dt)
            ok = bool(entry) and entry.get("data_date") == dt and (
                str(entry.get("path")) == str(part.get("path")))
            index_checks.append({"dt": dt, "data_date": (entry or {}).get("data_date"),
                                 "ok": ok})
            if not ok:
                mismatches.append(f"归档索引未把 {dt} 关联回原数据日期")
        checks["archive_index"] = index_checks

        return {"ok": not mismatches, "mismatches": mismatches, "checks": checks}
