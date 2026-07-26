// License allowlist gate for the frontend dependency tree.
//
// Reads the JSON output of `pnpm licenses list --json` from stdin and fails
// when any package carries a license outside the allowlist, unless the
// package is a blessed, individually reviewed exception. Adding a license or
// an exception is a redistribution decision and belongs in a reviewed commit.

const ALLOWED_LICENSES = new Set([
  "MIT",
  "Apache-2.0",
  "BSD-2-Clause",
  "BSD-3-Clause",
  "ISC",
  "0BSD",
  "Unicode-3.0",
  "Zlib",
  "CC0-1.0",
]);

// Blessed exceptions: dev-time tooling that never ships inside the built
// bundle (the production dependency tree is MIT/Apache-2.0 only).
const BLESSED_PACKAGES = new Map([
  ["@csstools/color-helpers", "MIT-0"], // more permissive than MIT
  ["argparse", "Python-2.0"], // permissive; js-yaml CLI parsing, dev only
  ["caniuse-lite", "CC-BY-4.0"], // browser-support data, dev only
  ["minimatch", "BlueOak-1.0.0"], // permissive; dev-only glob matching
]);

// lightningcss ships per-platform binary packages (lightningcss-linux-x64-gnu,
// lightningcss-win32-x64-msvc, ...), all MPL-2.0. MPL-2.0 is file-level
// copyleft on lightningcss's own sources, acceptable for a dev-only CSS
// minifier that is not redistributed with this project.
function isBlessed(name, license) {
  if (BLESSED_PACKAGES.get(name) === license) {
    return true;
  }
  return license === "MPL-2.0" && (name === "lightningcss" || name.startsWith("lightningcss-"));
}

function licenseExpressionAllowed(expression) {
  const trimmed = expression.replaceAll("(", "").replaceAll(")", "").trim();
  if (trimmed.includes(" OR ")) {
    return trimmed.split(" OR ").some((part) => licenseExpressionAllowed(part));
  }
  if (trimmed.includes(" AND ")) {
    return trimmed.split(" AND ").every((part) => licenseExpressionAllowed(part));
  }
  return ALLOWED_LICENSES.has(trimmed);
}

let input = "";
process.stdin.setEncoding("utf8");
for await (const chunk of process.stdin) {
  input += chunk;
}

const report = JSON.parse(input);
const violations = [];
let packageCount = 0;

for (const [license, packages] of Object.entries(report)) {
  for (const pkg of packages) {
    packageCount += 1;
    if (!licenseExpressionAllowed(license) && !isBlessed(pkg.name, license)) {
      violations.push(`${pkg.name}@${(pkg.versions ?? []).join(",")}: ${license}`);
    }
  }
}

if (packageCount === 0) {
  console.error("license report was empty; did `pnpm licenses list --json` run after install?");
  process.exit(1);
}

if (violations.length > 0) {
  console.error("packages outside the license allowlist:");
  for (const violation of violations) {
    console.error(`  - ${violation}`);
  }
  process.exit(1);
}

console.log(`all ${packageCount} packages satisfy the license allowlist`);
