# n2x site

纯静态官网，无构建步骤、无外部 CDN。直接打开 `index.html` 或用任意静态服务器预览：

```sh
python3 -m http.server 8787 --directory site
# http://127.0.0.1:8787
```

## 页面

| 文件 | 内容 |
| --- | --- |
| `index.html` | 首页：用途、适用/不适用、先运行 lab、示例入口 |
| `getting-started.html` | 开始：运行 lab、Manifest、端口、启动执行、报告与归档 |
| `examples.html` | 示例：单边迁移、逐步控制、故障恢复、网页后端流程 |
| `concepts.html` | 概念手册：Manifest、Step、Run、Intent、Workspace、事件 |
| `notes.html` | 注意事项：容量与保留、归档、写入端契约、单写者、计划冻结 |
| `quickstart.html` | 旧链接跳转到 `getting-started.html` |

`style.css` 与 `app.js` 是共享资源。主题、移动端菜单、代码复制、侧栏高亮都在
`app.js` 里，均为渐进增强；禁用 JS 时正文仍然可见。

## 部署

把 `site/` 目录原样发布到任意静态托管即可。页面之间使用相对路径，子路径部署
（例如 `/n2x/`）也能工作；文档链接指向 GitHub 上的 Markdown 文件。
