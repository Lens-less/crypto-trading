# Python legacy archive

`python-legacy/` 是旧 Python 项目的冻结归档，不是当前项目，也不参与 Rust 的构建、测试、配置加载或运行。

## 归档来源与完整性

- 来源提交：`620737399bfe3c331f9989fc77d631536f2e89df`
- 归档文件：366 个 Git tree 文件条目
- 可执行位：195 个 `100755` 条目按来源提交保留
- 原始 blob 总大小：6,694,241 字节
- 校验方式：逐文件比较路径、blob ID 和 Git mode
- 校验结果：366/366 匹配，0 个缺失、内容不一致或 mode 不一致

本说明文件位于快照外部，因此 `python-legacy/` 内部仍保持来源提交的原始目录结构和内容。

## 使用规则

1. 把 `python-legacy/` 视为只读证据，只用于审计、行为对照或恢复。
2. 新功能和修复只进入 `../rust/`。
3. 不要让 Rust 代码、测试或 CI 从本归档读取配置或源码。
4. `../rust/config/` 是当前配置的独立副本；对它的修改不会回写本归档。

旧项目的原始 README、依赖清单、源码、脚本和配置均保留在 [`python-legacy/`](python-legacy/) 中。

## W3 split gate

没有为本次重构配置外部归档仓库或目标 URL，因此 R6 不能诚实地标记为已拆库。
[`python-legacy/packaging/manifest-2026-08-12.tsv`](python-legacy/packaging/manifest-2026-08-12.tsv)
冻结了 366 个源文件的路径、大小与 SHA-256，供未来导出后逐项验收；完整门禁见
[`python-legacy/packaging/MIGRATION-GATE-2026-08-12.md`](python-legacy/packaging/MIGRATION-GATE-2026-08-12.md)。
