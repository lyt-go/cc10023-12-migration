"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import prediction_migration, resource

# prediction_migration 含更具体的 /api/predict_batch/{id}/... 子路径，必须先注册，
# 否则会被通用资源路由的 /api/{module}/{entry_id} 抢先匹配。
ROUTERS = [prediction_migration, resource]
