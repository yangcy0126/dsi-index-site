# WDSI 外交情绪观测台

这是一个适合 `GitHub Pages` 的静态网站原型，用来公开展示 WDSI 指数、方法说明和下载入口。

## 目录结构

- `index.html`：首页
- `styles.css`：样式
- `script.js`：前端交互逻辑
- `data/`：网页直接读取的 JSON / CSV
- `scripts/build_wdsi_data.py`：把本地 Excel 情绪结果表转换成网页数据
- `.github/workflows/deploy.yml`：GitHub Pages 部署工作流

## 数据来源

当前数据生成脚本默认读取上一级目录中的原始结果文件：

- `../data/情绪测度结果数据/中国外交部例行记者会情绪测度结果.xlsx`
- `../data/情绪测度结果数据/美国国务院新闻办公室发言稿情绪测度结果.xlsx`
- `../data/情绪测度结果数据/英国外交办公室新闻稿情绪测度结果.xlsx`
- `../data/情绪测度结果数据/日本外交部数据情绪测度结果.xlsx`
- `../data/情绪测度结果数据/韩国外交部新闻稿情绪测度结果.xlsx`

脚本当前把每个文件里的 `2` 列解释为战争相关外交情绪分数，并生成：

- 分国 `JSON`
- 分国 `CSV`
- 全量合并 `CSV`
- 站点摘要 `summary.json`

## 本地更新数据

```powershell
python scripts/build_wdsi_data.py
```

## 本地预览

不要直接双击 `index.html`，因为浏览器会阻止本地 `fetch` JSON。

请在仓库目录运行：

```powershell
python -m http.server 8000
```

然后打开：

```text
http://localhost:8000
```

## GitHub Pages 部署

这个仓库已经附带 GitHub Pages 工作流。推到 `main` 分支后，进入 GitHub：

1. 打开仓库 `Settings`
2. 进入 `Pages`
3. 选择 `GitHub Actions` 作为 Source

之后每次推送都会自动部署。
