import { describe, expect, it } from "vitest";
import { sumDecimalStrings } from "./decimal";

describe("sumDecimalStrings", () => {
  it("按最大小数位精确求和,不经过浮点", () => {
    expect(sumDecimalStrings(["0.1", "0.2"])).toBe("0.3");
    expect(sumDecimalStrings(["100.05", "0.95", "1"])).toBe("102");
  });

  it("支持负数与借位", () => {
    expect(sumDecimalStrings(["1.5", "-2.25"])).toBe("-0.75");
  });

  it("空集合与全零归一为 0", () => {
    expect(sumDecimalStrings([])).toBe("0");
    expect(sumDecimalStrings(["0.00", "0"])).toBe("0");
  });

  it("无法解析的值按 0 计(不猜测)", () => {
    expect(sumDecimalStrings(["1.5", "abc"])).toBe("1.5");
  });
});
