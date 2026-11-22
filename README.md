# 特征工程与数据挖掘工作台

面向特征构建、特征选择、数据切片、模型训练与挖掘结果管理的数据分析特征工程平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 特征源管理 | `feature_source` | 特征源 | 源编号、源名称、数据表名 |
| 特征定义 | `feature_define` | 特征定义 | 特征编号、特征名称、特征类型 |
| 特征组管理 | `feature_group` | 特征组 | 组编号、组名称、包含特征 |
| 数据切片配置 | `data_slice` | 切片配置 | 切片编号、切片名称、数据来源 |
| 特征计算任务 | `feature_compute` | 计算任务 | 任务编号、关联特征、数据切片 |
| 特征筛选策略 | `feature_selection` | 筛选策略 | 策略编号、特征集、筛选方法 |
| 特征重要性分析 | `feature_iv` | 重要性评估 | 评估编号、关联特征、信息值IV |
| 特征快照管理 | `feature_snapshot` | 特征快照 | 快照编号、特征集、快照日期 |
| 训练流水线 | `train_pipeline` | 流水线配置 | 流水线编号、流水线名称、特征集 |
| 模型注册中心 | `model_registry` | 模型注册 | 模型编号、模型名称、算法类型 |
| 批量预测任务 | `predict_batch` | 预测任务 | 任务编号、模型编号、数据切片 |
| 挖掘结果管理 | `mining_result` | 挖掘结果 | 结果编号、分析主题、算法类型 |
| 聚类分析配置 | `cluster_config` | 聚类配置 | 配置编号、特征集、算法类型 |
| 特征相关性矩阵 | `feature_corr` | 相关性矩阵 | 矩阵编号、特征集、计算方法 |
| 特征漂移监控 | `drift_monitor` | 漂移监控 | 监控编号、关联特征、基准分布 |
| 数据导出管理 | `data_export` | 导出任务 | 任务编号、数据来源、导出范围 |
| 流水线调度 | `pipeline_schedule` | 调度配置 | 调度编号、关联流水线、Cron表达式 |
| 业务场景标注 | `scenario_label` | 场景标注 | 标注编号、场景名称、关联特征 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 存量预测 CSV → Parquet 迁移

批量预测任务的历史产物是字段映射为 `prediction_id,model_id,score,label,dt`
的 CSV。`backend/app/services/prediction_archive.py` 负责按 `dt` 把它们迁移成
Snappy 压缩的列式分区，并提供统一的按日期读入口：

- `model_id` 按字符串存储，`001` 回读仍是 `001`；`score` 用
  `decimal128(38,18)` 保存，回读为十进制字符串；空 `label` 存成 NULL。
- 每个数据日期是一套完整输入。`daily` 模式逐日切换，`batch` 模式整批成功
  才切换；失败日期继续读原始 CSV，坏行用 CSV 物理行号定位。
- 迁移回执（`receipts/<dataset>/<migration_id>.json`）记录源行数、模型标识、
  分区去向和失败行；`verify` 会回读分区核对行数、Snappy 压缩、内容摘要与
  归档索引（`archive/index.json`）里的 `data_date`。
- 同一日期重复迁移是原子覆盖，索引提交前读入口始终指向旧数据，所以中断后
  恢复不会重复或丢行；迁移成功会在数据导出管理登记可回读的列式分区文件。

数据默认落在 `backend/var/predictions/`（已加入 `.gitignore`），可用
`PREDICTION_DATA_ROOT` 指向共享存储。依赖版本见 `backend/requirements.txt`
（`pyarrow==21.0.0`）。

| 接口 | 用途 |
| --- | --- |
| `POST /api/predict_batch/{id}/migration` | 执行迁移，参数 `source_csv`、`mode`、`dates`、`field_mapping` |
| `GET /api/predict_batch/{id}/migration/receipt` | 读取迁移回执（默认最近一次） |
| `GET /api/predict_batch/{id}/migration/verify` | 按回执核对源行数、模型标识、分区去向 |
| `GET /api/predict_batch/{id}/predictions?dt=YYYY-MM-DD` | 按日期回读，已切换读 Parquet，否则读 CSV |
| `GET /api/predict_batch/{id}/archive-index` | 数据日期与当前生效存储位置的对应关系 |
| `GET /api/predict_batch/{id}/export/{export_id}/read` | 回读数据导出管理登记的列式分区 |

```bash
cd backend
.venv/bin/uvicorn app.main:app --port 8000     # 启动
.venv/bin/python tests/test_prediction_migration.py
```

领域：数据工程
子方向：数据分析与数据应用
