use std::{hint::black_box, time::Instant};

use chrono::Utc;
use crypto_trading_runtime::{DecisionRecord, JsonlHistory};
use serde_json::json;
use uuid::Uuid;

const SIBLING_COUNTS: [usize; 3] = [0, 100, 10_000];
const WARMUP_APPENDS: usize = 4;
const MEASURED_APPENDS: usize = 32;

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime");

    println!("siblings,appends,total_micros,mean_micros");
    for sibling_count in SIBLING_COUNTS {
        let root =
            std::env::temp_dir().join(format!("history-inspection-scaling-{}", Uuid::new_v4()));
        std::fs::create_dir(&root).expect("create benchmark directory");
        for index in 0..sibling_count {
            std::fs::File::create(root.join(format!("unrelated-{index}.tmp")))
                .expect("create unrelated sibling");
        }

        let path = root.join("decisions.jsonl");
        let history = JsonlHistory::new(&path);
        runtime.block_on(async {
            for index in 0..WARMUP_APPENDS {
                history
                    .append(&record("warmup", index))
                    .await
                    .expect("warmup append");
            }
        });

        let started = Instant::now();
        runtime.block_on(async {
            for index in 0..MEASURED_APPENDS {
                history
                    .append(black_box(&record("measured", index)))
                    .await
                    .expect("measured append");
            }
        });
        let elapsed_micros = started.elapsed().as_micros();
        let mean_micros = elapsed_micros / u128::from(MEASURED_APPENDS as u64);
        println!("{sibling_count},{MEASURED_APPENDS},{elapsed_micros},{mean_micros}");

        drop(history);
        std::fs::remove_dir_all(&root).expect("remove benchmark directory");
    }
}

fn record(phase: &str, index: usize) -> DecisionRecord {
    DecisionRecord {
        timestamp: Utc::now(),
        strategy: "history-inspection-scaling".to_owned(),
        symbol: "BTC-USDT".to_owned(),
        decision: phase.to_owned(),
        details: json!({ "index": index }),
    }
}
