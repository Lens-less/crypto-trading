/**
 * 稳定 token → 中文文案(沿承旧 app.js 的 TOKEN_LABELS 语义)。
 * 关键语义文案不改:running/stopping/pending 等必须写明「最后记录」,
 * 避免把 journal 历史事实解释成当前进程状态。
 */
const TOKEN_LABELS = new Map<string, string>([
  // 投影状态
  ["complete", "完整"],
  ["windowed", "窗口化"],
  ["degraded", "降级"],
  ["loading", "正在加载"],
  // journal 页边界
  ["snapshot_end", "快照末尾"],
  ["page_limit", "分页上限"],
  ["partial_tail", "部分尾记录"],
  // 执行批次状态与恢复
  ["completed", "已完成"],
  ["partial", "部分完成"],
  ["incomplete", "未完成"],
  ["failed", "失败"],
  ["conflict", "冲突"],
  ["outcome_unknown", "结果未知"],
  ["planned", "已计划"],
  ["none", "无"],
  ["reconcile_required", "需要对账"],
  ["investigate", "需要调查"],
  // 投影警告代码
  ["conflicting_duplicate", "冲突重复"],
  ["duplicate_ignored", "重复已忽略"],
  ["invalid_execution_event", "无效执行事件"],
  ["metadata_conflict", "元数据冲突"],
  ["orphan_outcome", "孤立结果"],
  ["out_of_order_planned", "计划记录乱序"],
  ["resolved_batch_evicted", "已完结批次被淘汰"],
  ["terminal_conflict", "终态冲突"],
  ["timestamp_regressed", "时间戳回退"],
  // 预警
  ["pending", "最后记录:未决"],
  ["dropped", "已丢弃"],
  ["succeeded", "已送达"],
  ["timed_out", "已超时"],
  ["volatility_up", "波动上破"],
  ["volatility_down", "波动下破"],
  ["upper_limit", "上沿提醒"],
  ["lower_limit", "下沿提醒"],
  ["backpressure", "排队拥塞"],
  ["adapter_closed", "适配器已关闭"],
  ["device_unavailable", "设备不可用"],
  ["rejected", "已拒绝"],
  ["worker_failed", "通知 worker 异常终止"],
  ["timeout", "超时"],
  // 市场与监控
  ["spot", "现货"],
  ["perpetual", "永续"],
  ["waiting", "等待行情"],
  ["no_opportunity", "暂无机会"],
  ["opportunity", "发现机会"],
  ["analysis_rejected", "分析拒绝"],
  ["missing", "缺失"],
  ["fresh", "新鲜"],
  ["stale", "陈旧"],
  ["future", "未来时间"],
  ["continuous", "连续"],
  ["gap", "缺口"],
  ["source_gap", "数据缺口"],
  ["duplicate", "重复"],
  ["out_of_order", "乱序"],
  ["duplicate_timestamp", "重复时间戳"],
  ["out_of_order_timestamp", "时间戳乱序"],
  ["out_of_order_receipt", "接收乱序"],
  ["unavailable", "不可用"],
  ["invalid_config", "配置无效"],
  ["snapshot_mismatch", "快照不一致"],
  ["missing_market_data", "缺少行情数据"],
  ["invalid_financial_value", "金额数值无效"],
  // 只读任务
  ["registered", "已登记"],
  ["running", "最后记录:运行中"],
  ["stopping", "最后记录:停止中"],
  ["starting", "最后记录:启动中"],
  ["stopped", "已停止"],
  ["healthy", "健康"],
  ["unknown", "未知"],
  ["stop_requested", "收到停止请求"],
  ["source_ended", "数据源结束"],
  ["shutdown_timed_out", "停止超时"],
  ["startup_failed", "启动失败"],
  ["source_contract", "数据源契约失败"],
  ["monitor_contract", "监控契约失败"],
  ["journal_unavailable", "日志不可用"],
  ["task_panicked", "任务异常终止"],
  ["task_cancelled", "任务被取消"],
  ["invalid_request", "请求无效"],
  ["recovery_required", "需要恢复"],
  ["account_contract", "账户契约失败"],
  ["execution_incomplete", "执行未完成"],
  ["execution_failed", "执行失败"],
  ["arbitrage_monitor", "套利监控"],
  ["arbitrage_paper", "Paper 套利"],
  ["grid_paper", "Paper 网格"],
  ["price_alert", "价格预警"],
  ["scanner", "扫描器"],
  ["volume_maker", "Paper 刷量"],
  // 能力等级
  ["available", "可用"],
  ["read-only", "只读"],
  ["paper-once", "单次模拟"],
  ["validate-only", "仅校验"],
  ["contract-only", "仅契约"],
  // 适配器支持矩阵
  ["implemented", "已实现"],
  ["protocol-only", "仅协议"],
  ["request-only", "仅请求"],
  ["config-only", "仅配置"],
  ["not-applicable", "不适用"],
  // scanner 排行
  ["benchmark", "基准"],
  ["standard", "标准"],
  ["explicit_benchmark_then_apr_desc", "显式 benchmark 优先,按 APR 降序"],
  // settings 投影
  ["stdout_stderr", "标准输出 / 标准错误"],
  ["journal_projection", "journal 投影"],
  ["not_configured", "未配置"],
  ["not_accepted", "不接受"],
  ["not_projected", "未投影"],
  ["grid", "Paper 网格"],
  ["arbitrage", "Paper 套利"],
  // submit 回执
  ["applied", "已写入"],
  // 系统运行信号
  ["normal", "正常"],
  ["engaged", "已触发"],
  ["not_available", "暂不可用"],
]);

/** 稳定 token → 中文;未知 token 原样返回(错误脱敏:不做自由拼接)。 */
export function humanizeToken(token: string | null | undefined): string {
  if (token === null || token === undefined || token === "") {
    return "--";
  }
  return TOKEN_LABELS.get(token) ?? token;
}

/**
 * 凭据配置状态专用文案:只表示配置完整性,不返回值。
 * 单独成表以避免与执行批次的 "partial"(部分完成)语义冲突。
 */
const CREDENTIAL_LABELS = new Map<string, string>([
  ["configured", "已配置"],
  ["partial", "部分配置"],
  ["not_configured", "未配置"],
  ["not_accepted", "不接受"],
  ["not_projected", "未投影"],
]);

export function credentialLabel(token: string | null | undefined): string {
  if (token === null || token === undefined || token === "") {
    return "--";
  }
  return CREDENTIAL_LABELS.get(token) ?? token;
}

/**
 * Paper 预留阶段专用文案:与预警投递的 "pending"(最后记录:未决)
 * 语义分离,预留阶段是账户敞口状态而非通知状态。
 */
const RESERVATION_PHASE_LABELS = new Map<string, string>([
  ["pending", "待处理"],
  ["uncertain", "不确定"],
  ["committed", "已提交"],
  ["released", "已释放"],
]);

export function reservationPhaseLabel(token: string | null | undefined): string {
  if (token === null || token === undefined || token === "") {
    return "--";
  }
  return RESERVATION_PHASE_LABELS.get(token) ?? token;
}
