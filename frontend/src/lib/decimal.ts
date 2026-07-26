/**
 * 有界十进制字符串求和(沿承旧 app.js 的 sumDecimalValues)。
 *
 * 后端 Money 序列化为规范十进制字符串;前端聚合时用 BigInt 精确求和,
 * 永不经过 JS 浮点。无法解析的值按 0 计并由调用方决定是否另行呈现。
 */

interface ParsedDecimal {
  coefficient: bigint;
  scale: number;
}

function parseDecimalForSum(value: string): ParsedDecimal {
  const match = /^(-?)(\d+)(?:\.(\d+))?$/.exec(String(value));
  if (match === null) {
    return { coefficient: 0n, scale: 0 };
  }
  const fraction = match[3] ?? "";
  const coefficient = BigInt(`${match[1]}${match[2]}${fraction}`);
  return { coefficient, scale: fraction.length };
}

/** 精确求和一组十进制字符串,返回去除尾零的规范十进制字符串。 */
export function sumDecimalStrings(values: readonly string[]): string {
  const parsed = values.map(parseDecimalForSum);
  const scale = parsed.reduce((maximum, value) => Math.max(maximum, value.scale), 0);
  let total = 0n;
  for (const value of parsed) {
    total += value.coefficient * 10n ** BigInt(scale - value.scale);
  }
  const negative = total < 0n;
  const digits = (negative ? -total : total).toString().padStart(scale + 1, "0");
  if (scale === 0) {
    return `${negative ? "-" : ""}${digits}`;
  }
  const whole = digits.slice(0, -scale);
  const fraction = digits.slice(-scale).replace(/0+$/, "");
  return `${negative ? "-" : ""}${whole}${fraction === "" ? "" : `.${fraction}`}`;
}
