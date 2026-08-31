# tdrive-aliyunpan

把阿里云盘的文件按计划搬进 [tdrive](https://github.com/dibin666/tdrive)（也就是搬进 Telegram）的插件。

- **任务**：把一个云盘目录映射到一个 tdrive 目录，可按名称正则和文件大小过滤，可选上传后删除云端原件。
- **计划与限额**：每天允许运行的时间窗口、窗口内的重新扫描间隔、每日上传到 Telegram 的字节上限。
- **账号**：下载并安装 aliyunpan 命令行、扫码登录。

所有操作都在 tdrive 网页界面里完成：设置 → 插件 → 「阿里云盘同步」→ 打开。

## 安装

在 tdrive「设置 → 插件 → 安装插件」里粘贴：

```
https://github.com/dibin666/tdrive-aliyunpan/releases/latest/download/tdrive.plugin.json
```

这个地址始终指向最新版，要更新就再装一次。支持 `linux/amd64`、`linux/arm64`、`windows/amd64`、`windows/arm64`。

## 许可

MIT
