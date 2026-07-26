// 首帧主题初始化,防止深/浅色闪烁。
// 独立静态文件(script-src 'self' 兼容),不做任何网络请求。
// storage 白名单:仅读取 "ct-theme"。
(function () {
  var theme = "dark";
  try {
    var stored = window.localStorage.getItem("ct-theme");
    if (stored === "light" || stored === "dark") {
      theme = stored;
    } else if (
      window.matchMedia &&
      window.matchMedia("(prefers-color-scheme: light)").matches
    ) {
      theme = "light";
    }
  } catch (error) {
    // localStorage 不可用时保持默认深色。
  }
  document.documentElement.classList.toggle("light", theme === "light");
})();
