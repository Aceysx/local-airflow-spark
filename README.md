## 如何在本地启动运行 spark、airflow 环境

整个环境包含 `spark-master`, `spark-worker`, `airflow-webserver`,`airflow-scheduler` 和 `postgres`

1. 根据本机配置在 `docker-compose.yml` 中配置 `worker` 的内存和CPU（需要检查自己的 docker 本身所配置的内存和CPU大小是否足够！）
2. 本地启动 docker-compose
```bash
docker-compose up -d --build
```
3. 打开 `http://localhost:8080/home` 进入 airflow dashboard; `http://localhost:9090` spark dashboard
4. 配置 spark connection; 打开 airflow->Admin->Connections
```
ConnectionId: spark_default
ConnectionType: spark
Host: spark://HOST_IP/SPARK_SERVICE_NAME
Port: 7070
Deploy Mode: client
Spark binary: spark-submit
```
5. 在 airflow 主页 可以看到 `Backblaze_drive_data_stats` DAG

## 整体思路
根据作业要求需要
- 统计 Backblaze 每天 Drive count/Drive failures
- 统计 Backblaze 每年 Drive failures count by brand

### Extract：从 [Backblaze](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data) 下载zip接到到本地
将远程的 zip 文件下载解压到 `./data/extracted_drive_data` 目录下。

#### 不足的地方
考虑到本地的资源紧张😓，在 `spark_airflow.py` 的 `extract_task` 配置了 skip 参数，可以跳过这个阶段。。

避免数据量太大咯。并没有实现根据当前日期去 Backblaze 下载数据的逻辑。。。

所以需要自己手动的在 `extract_job.py` 中设置某个季度的下载链接

### Transform：数据分析（日度和年度的计算）
这块分为了两个 DAG spark job，分别是 `calculate_daily_summary_data` 和 `calculate_annual_summary_data`; 这俩可以进行并行进行处理。

将 `extracted_drive_data` 中的 csv 文件读取进行计算，最后得到日度和年度的csv结果，放在 `./data/transformed` 中

#### 不足的地方
同样的不应该每次全量的搞（特别是日度的），应该 by day 的增量计算。而年度的应该 by year 的进行 YTD 的计算。

### Load：将计算好的数据存到 DB 中
这块处理的比较粗糙，直接将 `calculate_daily_summary_data` 和 `calculate_annual_summary_data` csv 的内容复制到 `airflow` 的 `PostgreSQL` 库中（table: `drive_data_annual_summary` 和 `drive_data_daily_summary`

关于分布式存储：也是取巧，通过给每个 container 挂载 docker volume 实现。

虽然。。但是。。。这样可以在本地快速的跑起来并体验


