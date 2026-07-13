# crypto-trading

本仓库已经按“当前主线”和“历史归档”完成物理拆分。仓库根目录只负责导航和 CI，不再同时承载两套可运行项目。

## 目录边界

```text
crypto-trading/
├── rust/                    # 当前主线：唯一需要开发、构建和运行的项目
│   ├── crates/              # Rust workspace 源码
│   ├── config/              # 当前运行配置，可随 Rust 主线演进
│   ├── Cargo.toml
│   └── README.md
├── archive/
│   ├── README.md            # 归档清单和校验信息
│   └── python-legacy/       # 冻结的旧 Python 项目，不参与当前运行
└── .github/                 # 仓库级 CI
```

| 目录 | 状态 | 使用规则 |
| --- | --- | --- |
| [`rust/`](rust/README.md) | 当前项目 | 新开发、构建、测试、运行都只在这里进行 |
| [`archive/python-legacy/`](archive/python-legacy/) | 只读归档 | 仅用于审计、对照和恢复；不要在此继续开发 |

两处 `config/` 看起来相似，但所有权不同：

- `rust/config/` 是当前 Rust 项目的独立配置副本。
- `archive/python-legacy/config/` 属于冻结的 Python 快照，不能被当前代码引用或修改。

## 当前项目入口

```powershell
cd rust
cargo build --workspace
cargo test --workspace --all-targets --all-features
cargo run -- --help
```

旧项目的来源提交、文件数量与逐文件校验结果见 [`archive/README.md`](archive/README.md)。
