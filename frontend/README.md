# LLM-TRADE 前端工作台

这是 `LLM-TRADE` 的 Vue + Vite 前端，用于展示观察仓、交易仓、研报、审计摘要、回测结果，并通过本地 API 触发同步、选股、回测、模拟调仓和指定股票分析。

## 常用命令

```bash
npm install
npm run dev
npm run build
```

开发模式下，`vite.config.js` 会把 `/api` 代理到 `http://127.0.0.1:8765`。先在项目根目录启动后端：

```bash
python main.py --dashboard
```

生产模式下，运行 `npm run build` 生成 `frontend/dist`，再由 `python main.py --dashboard` 直接托管构建产物。
