# tdrive-aliyunpan

把阿里云盘的文件按计划搬进 [tdrive](https://github.com/dibin666/tdrive)（也就是搬进 Telegram）的插件。

- **任务**：把一个云盘目录映射到一个 tdrive 目录，可按名称正则和文件大小过滤，可选上传后删除云端原件；每个任务可独立选择备份盘或资源库。
- **计划与限额**：每天允许运行的时间窗口、窗口内的重新扫描间隔、每日上传到 Telegram 的字节上限。
- **账号**：下载并安装 aliyunpan 命令行、扫码登录。

所有操作都在 tdrive 网页界面里完成：设置 → 插件 → 「阿里云盘同步」→ 打开。

插件不会依赖 aliyunpan 的全局当前网盘。保存任务时选择“备份盘”或“资源库”，插件会自动读取当前账号对应的 drive ID，并在扫描、下载和删除时显式传入，多个任务可以同时使用不同网盘。

aliyunpan 命令行及其登录配置由宿主固定保存在数据卷的 `plugin-data/aliyunpan` 目录中，不随插件可执行文件的位置变化；升级或重启插件不会再次下载。旧版本的 `plugins/aliyunpan-data` 会在首次启动时自动迁移。

## 安装

在 tdrive「设置 → 插件 → 安装插件」里粘贴：

```
https://github.com/dibin666/tdrive-aliyunpan/releases/latest/download/tdrive.plugin.json
```

这个地址始终指向最新版，要更新就再装一次。支持 `linux/amd64`、`linux/arm64`、`windows/amd64`、`windows/arm64`。

## 许可

MIT
