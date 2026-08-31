# tdrive-aliyunpan

把阿里云盘的文件按计划搬进 [tdrive](https://github.com/dibin/tdrive)（也就是搬进 Telegram）的插件。

所有操作都在 tdrive 网页界面里完成：设置 → 插件 → 「阿里云盘同步」→ 打开。页面在当前标签页内展开，不会跳出去。

## 它做什么

- **同步页**：复用 tdrive 传输页的呈现方式 —— 进度就是行的背景色、同一套状态文案、同样的密度切换和刷新节奏。所有传输同时也会出现在 tdrive 自己的传输页上，来源标记为「阿里云盘」。
- **任务**：一条任务把一个云盘目录映射到一个 tdrive 目录，支持按名称正则排除、按大小过滤、可选覆盖同名文件、可选在上传成功后删除云端原件（默认关闭）。
- **计划与限额**：每天允许运行的时间窗口（支持跨零点）、窗口内的重新扫描间隔、每日上传到 Telegram 的字节上限和归零时刻。配额用完后剩下的队列原样留到下一个窗口继续。
- **账号**：下载并安装 aliyunpan 命令行、扫码登录、查看当前账号、退出登录。

传输过程中的所有限制都跟随 tdrive 当前的运行参数（`设置 → 性能参数 / 存储与暂存`），插件不另设一套。

## 工作方式

插件不重新实现阿里云盘的 API，而是驱动上游的 [tickstep/aliyunpan](https://github.com/tickstep/aliyunpan) 命令行：OAuth、令牌刷新、设备注册和下载协议都由它负责，这些东西按阿里的节奏变化，不按 tdrive 的。

一个文件的旅程：

1. 扫描：对每个启用的任务用 `aliyunpan ll` 逐层列目录，和 tdrive 里对应目录的现有文件比对（同名同大小视为已同步），差集进队列。
   **tdrive 本身就是索引** —— 不另存一份「已同步」台账，所以在「文件」页删掉一个文件，下一轮就会把它重新同步回来。
2. 暂存：`aliyunpan download` 把文件下到暂存目录，进度取自 `.aliyunpan-downloading` 临时文件的大小。
3. 上传：`files.beginUpload` 开一个可续传任务，然后逐个分片 `files.putSegment`，最后 `files.completeUpload`。
   分片是**串行**发的 —— tdrive 内部已经用 `UploadThreads` / `UploadPartSize` / `RateLimit` 做了并发和限速，插件再叠一层只会打架。整文件的并发数取 tdrive 的 `UploadConcurrency`。
4. 收尾：删掉暂存副本；只有任务开启了「上传后删除云端原文件」，并且 tdrive 已经确认写入，才会去删云端。

**分片提交的错误对插件不可见**：宿主在等待分片落库之前就关掉了插件这一端的流，所以一个失败的分片从插件看来和成功的一模一样。唯一的判据是 `files.completeUpload` 的 `upload is still missing segments [...]`——插件解析出缺失的下标，退避重试几次（分片可能还在落库），仍然缺就只重发这几片，最多三轮，之后 `files.abortUpload` 把残留分片清掉。

## 配置

配置存在插件数据的 `settings` 键里，也就是 tdrive「设置 → 插件 → 配置」那个 JSON 文本框读写的同一个键。两边看到的永远是同一份文档。

```jsonc
{
  "binaryPath": "",              // 留空 = 用插件自己下载的那份
  "stageDir": "",                // 留空 = <插件数据目录>/aliyunpan/stage
  "stageLimitBytes": 0,          // 0 = 跟随 tdrive 的 CacheLimit
  "ownerUserId": "",             // 上传归属账号，留空 = 第一个启用的管理员
  "downloadRate": "",            // 传给 aliyunpan 的 max_download_rate，如 "2MB"
  "schedule": {
    "enabled": true,
    "windowStart": "01:00",      // 两个都留空 = 全天；结束早于开始 = 跨零点
    "windowEnd": "07:00",
    "intervalMinutes": 15
  },
  "quota": {
    "dailyBytes": 21474836480,   // 0 = 不限；只统计推送到 Telegram 的字节
    "resetAt": "00:00"
  },
  "jobs": [
    {
      "id": "a1b2c3d4",
      "name": "影视",
      "enabled": true,
      "remotePath": "/我的资源/影视",
      "targetPath": "/阿里云盘/影视",
      "excludeNames": ["^\\."],
      "minSizeBytes": 0,
      "maxSizeBytes": 0,
      "overwrite": false,
      "deleteAfterUpload": false
    }
  ]
}
```

几个刻意的取舍：

- **单个文件大于一整天的配额**时，会在当天配额一点没用的情况下单独放行。否则它会永远排在队首，把后面的都堵死。页面上会给出提示。
- **窗口结束时不砍正在跑的文件**，只是不再取新的。分片虽然可以续传，但半途放弃会浪费已经下好的暂存副本。
- **失败的项不会被下一轮扫描自动重排**。重试是一个明确的动作，否则一个永久性错误会让扫描无限循环。

## 安装

插件通过「设置 → 插件 → 源码安装」从 HTTPS 源码地址安装，由 tdrive 的构建边车编译。

仓库里提交了 `vendor/`：构建机上拿不到 `../tdrive`，vendor 模式下所有依赖都来自仓库内，不需要 `github.com/dibin/tdrive` 发布到公共 proxy。

本地开发用同级目录的 tdrive 源码：

```
Works/
├── tdrive/          # go.mod 里 replace 指向这里
└── tdrive-plugin/
```

```sh
go build ./...
go test ./...
go mod vendor        # 改过依赖之后要重新生成
```

## 依赖 tdrive 的两处改动

这个插件需要 tdrive 主仓的两处通用改动（都不含阿里云盘特有逻辑）：

1. `internal/plugin/http.go` 把插件路由挂在 `auth.RequireBrowserAuth` 而不是 `RequireAuth` 上。插件页面是页面导航，带的是 HttpOnly 的会话 Cookie 而不是内存里的 bearer 令牌。
2. `ui/src/routes/settings/PluginsPage.tsx` 的「打开」在 WebUI 内嵌全屏展示插件页面，而不是新开标签页。

## aliyunpan 命令行

默认装在 `<插件数据目录>/aliyunpan/bin/aliyunpan`，版本固定为 `v0.4.0`（输出格式是被解析的，不能随版本漂移）。它是静态链接的 Go 二进制，distroless 镜像里可以直接 exec。

所有调用都带 `ALIYUNPAN_CONFIG_DIR=<插件数据目录>/aliyunpan/config`，和宿主机上任何已有的 aliyunpan 配置互不干扰。

登录是交互式的：命令行打印一个授权链接，然后阻塞等一个回车。插件充当那个终端 —— 从子进程的标准输出里抓出链接给页面显示，管理员在浏览器完成授权和扫码后点「我已完成登录」，插件往子进程的标准输入写一个换行。

## 许可

MIT
