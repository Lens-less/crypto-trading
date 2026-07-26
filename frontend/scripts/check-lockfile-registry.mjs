// Verifies that pnpm-lock.yaml resolves every package from the public npm
// registry. pnpm omits the registry URL for default-registry packages, so any
// URL that does appear (tarball overrides, git dependencies, mirrors) is
// exactly the case this gate exists to catch.
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const lockfilePath = join(dirname(fileURLToPath(import.meta.url)), "..", "pnpm-lock.yaml");
const lockfile = readFileSync(lockfilePath, "utf8");

const problems = [];

for (const match of lockfile.matchAll(/https?:\/\/[^\s"'()]+/g)) {
  const url = new URL(match[0]);
  if (url.protocol !== "https:" || url.host !== "registry.npmjs.org") {
    problems.push(`non-registry URL: ${match[0]}`);
  }
}

// Non-registry dependency protocols never carry an npm integrity guarantee.
for (const match of lockfile.matchAll(/^\s*(?:resolution|version):\s*.*(git\+|github:|link:|file:)[^\s]*/gm)) {
  problems.push(`non-registry dependency protocol: ${match[0].trim()}`);
}

if (problems.length > 0) {
  console.error("pnpm-lock.yaml violates the registry policy (registry.npmjs.org only):");
  for (const problem of problems) {
    console.error(`  - ${problem}`);
  }
  process.exit(1);
}

console.log("pnpm-lock.yaml resolves exclusively from registry.npmjs.org");
