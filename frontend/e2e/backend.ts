/**
 * 后端进程治具:spawn 真实的 `crypto-trading-web` 二进制并等待就绪。
 *
 * 所有 spec 都通过这里启动/停止后端,保证:
 * - 只读模式与写模式共用同一套启动、健康探测与清理逻辑;
 * - 被测的是嵌入 dist 的交付二进制,而不是 dev server;
 * - Windows 本地与 ubuntu CI 走同一条路径(仅二进制扩展名不同)。
 */
import { type ChildProcess, spawn } from "node:child_process";
import { copyFileSync, existsSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

/** 仓库根(playwright 的 cwd 固定为 frontend/)。 */
export const REPO_ROOT = resolve(process.cwd(), "..");

/** e2e 固定 journal 代次;与 fixture 契约无关,只需稳定。 */
export const E2E_JOURNAL_ID = "88888888-8888-4888-8888-888888888888";

/** 只读 spec 共用的组合 journal(monitor/tasks/executions 均非空)。 */
export const WEB_API_JOURNAL = join(
  REPO_ROOT,
  "rust",
  "fixtures",
  "web-api",
  "journal.jsonl",
);

function binaryPath(): string {
  const override = process.env.CT_WEB_BIN;
  if (override !== undefined && override !== "") {
    return override;
  }
  const name =
    process.platform === "win32"
      ? "crypto-trading-web.exe"
      : "crypto-trading-web";
  return join(REPO_ROOT, "rust", "target", "debug", name);
}

export interface BackendOptions {
  port: number;
  historyPath: string;
  /** 追加的 CLI 参数(写模式 profile 等)。 */
  extraArgs?: string[];
  /** 追加的子进程环境变量(bearer token 等)。 */
  env?: Record<string, string>;
}

export interface Backend {
  readonly baseUrl: string;
  readonly process: ChildProcess;
  /** 结束进程并等待退出;幂等。 */
  stop(): Promise<void>;
}

async function waitForHealth(baseUrl: string): Promise<void> {
  const deadline = Date.now() + 30_000;
  for (;;) {
    try {
      const response = await fetch(`${baseUrl}/api/v1/health`, {
        cache: "no-store",
      });
      if (response.ok) {
        return;
      }
    } catch {
      // 尚未就绪,继续轮询。
    }
    if (Date.now() > deadline) {
      throw new Error(`backend at ${baseUrl} did not become healthy in 30s`);
    }
    await new Promise((resolveSleep) => setTimeout(resolveSleep, 200));
  }
}

export async function startBackend(options: BackendOptions): Promise<Backend> {
  const binary = binaryPath();
  if (!existsSync(binary)) {
    throw new Error(
      `missing web binary at ${binary}; build it first:\n` +
        "  (cd frontend && pnpm build)\n" +
        "  (cd rust && cargo build --locked -p crypto-trading-web-app --bin crypto-trading-web)\n" +
        "or point CT_WEB_BIN at an existing build",
    );
  }
  const args = [
    "--history-path",
    options.historyPath,
    "--journal-id",
    E2E_JOURNAL_ID,
    "--port",
    String(options.port),
    ...(options.extraArgs ?? []),
  ];
  const child = spawn(binary, args, {
    cwd: REPO_ROOT,
    env: { ...process.env, ...options.env },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  child.stdout?.on("data", (chunk: Buffer) => {
    output += chunk.toString();
  });
  child.stderr?.on("data", (chunk: Buffer) => {
    output += chunk.toString();
  });
  const exited = new Promise<void>((resolveExit) => {
    child.once("exit", () => resolveExit());
  });

  const baseUrl = `http://127.0.0.1:${options.port}`;
  try {
    await Promise.race([
      waitForHealth(baseUrl),
      exited.then(() => {
        throw new Error(`backend exited during startup:\n${output}`);
      }),
    ]);
  } catch (error) {
    child.kill();
    throw error;
  }

  let stopped = false;
  return {
    baseUrl,
    process: child,
    async stop(): Promise<void> {
      if (stopped) {
        return;
      }
      stopped = true;
      if (child.exitCode === null && !child.killed) {
        child.kill();
      }
      await Promise.race([
        exited,
        new Promise((resolveSleep) => setTimeout(resolveSleep, 5_000)),
      ]);
    },
  };
}

/** 把一个 fixture journal 复制到临时目录,供写模式安全追加。 */
export function copyJournalToTemp(sourcePath: string): string {
  const directory = mkdtempSync(join(tmpdir(), "ct-e2e-journal-"));
  const target = join(directory, "operations.jsonl");
  copyFileSync(sourcePath, target);
  return target;
}
