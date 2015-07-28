Tera流程总结文档


目 录

[Tera流程总结文档	1]()

[1 Master的设计	1]()

[1.1 Master主要流程	1]()

[1.1.1 启动流程	1]()

[1.1.2 关闭流程	1]()

[1.1.3 加载tablet流程	1]()

[1.1.4 卸载tablet流程	2]()

[1.1.5 Tablet的reassignment	2]()

[1.1.6 迁移tablet流程	2]()

[1.1.7 分裂tablet流程	2]()

[1.1.8 合并tablet流程	3]()

[1.1.9 Tablet表状态定时收集	3]()

[1.1.10 负载均衡决策	4]()

[1.1.11 表格创建流程	4]()

[1.2 Master的数据结构	4]()

[1.2.1 Meta表结构	4]()

[1.2.2 表的schema	5]()

[1.2.3 表格的key设计：	5]()

[1.2.4 Master的内存数据结构	6]()

[1.2.5 ZK数据结构	6]()

[1.2.6 DFS目录结构	7]()

# Master的设计

## Master主要流程

### 启动流程

1. 在zk中注册并获得master的锁；（保证集群单实例）
1. 扫描zk的TS目录，获得活跃TS列表；

注意：Master启动后，定期更新该表；master采用zk的watch机制观察TS是否存活；

1. For each TS in TS列表
1.1. QueryTablet()获得该TS的管理Tablet列表；
1. master从zk获得meta Tablet位置；
1.1. 若meta tablet所对应的文件在zk不存在，则master创建该文件。
1.1. 若meta Tablet未被TS加载，则从获得TS列表选择meta TS；
1.1.1. 将metaTS位置写入zk文件；（严格按照该序执行）
1.1.1. 执行load meta Tablet；

注意：初始时，meta Tablet为空表，meta 表的位置是默认的，集群参与者master或TS都在启动配置文件中指定。先改zk文件，再执行load命令，保证防止master崩溃造成的频繁多TS加载同一个meta tablet。

1. master从meta TS获得扫描meta table：
1.1. 若发现某些tablet（table+startkey为tablet的标示）的区间缺失（底层文件系统）或重叠（由于split或merge的崩溃）：
1.1.1. 若区间重复，则选择长区间，删除短区间（split相当于回滚，merge相当于完成）。
1.1.1. 若区间缺失，则相当于该区间的tablet数据丢失；填入空白项；（一个尝试修复操作是产生扫描该用户表的所有tablet，从孤儿tablet重试恢复用户数据）；
1.1. 所有table的tablet列表；
1. 根据步骤3）和步骤5），产生unload tablet list；
1.1. 若发现meta表记录tablet不属于TS，但TS包含该tablet，则master在unload时崩溃，该tablet需要被unload完成后，才能被重新load；
1.1. 若发现meta表记录tablet属于TS，但TS不包含该tablet，则master在load时崩溃（TS崩溃也有可能出现该情况，splittablet时master崩溃），该tablet需要被load完成。
1. for each tablet in unload tablet list
1.1. 选择TS作为tablet的候选；
1.1. 更新meta表中该tablet对应的项，加入TS信息；
1.1. 执行rpc.LoadTablet(TS, Tablet)，该TS加载Tablet；
1.1. 加载完成，更新TS的tablet列表；
1. master启动完成，后台线程定期更新活跃TS列表和Tablet列表；
### 关闭流程

1、正常关闭与异常关闭一致；直接进行进程的kill操作；

### 加载tablet流程

1. 根据活跃TS列表，选择一个后续TS，准备接管tablet；
1. 添加meta表的tablet项；
1. 执行rpc.LoadTablet(TS, Tablet)命令，等待直到加载成功；
1. 若TS失败，则重新执行步骤1）；
### 卸载tablet流程

1. 清空meta表的tablet项；（master崩溃重启时，通过检查出不一致，进行恢复）
1. 向TS发送rpc.UnloadTablet命令，卸载tablet；
1. 若TS失败，则重新执行步骤2），卸载成功前，不能被reload；

注意：master重启时，通过比较活跃TS管理tablet列表和meta table的已接管列表，发现不一致，master恢复。

### Tablet的reassignment

1. 每个TS上线时，往zk注册文件并获得互斥锁；
1. 当TS与zk的心跳网络断开后，TS自杀；
1. Master启动时，watch每个TS；
1. 当每个TS与zk连接故障后，zk通知master出现TS宕机；
1. Master获得该TS的锁；
1. Master删除该文件；
1. For each tablet in TS
1.1. 执行加载tablet接口
### 迁移tablet流程

1. 旧TS卸载tablet
1. 新TS加载tablet
### 分裂tablet流程

#### 方案1

1. 对tablet所在的TS发送rpc.SplitTablet命令；等待并获得splitkey，此时旧的tablet被unload；
1. meta table的tablet插入新区间；
1. meta table的tablet压缩旧区间；
1. LoadTablet新区间；
1. LoadTablet修改区间；
#### 方案2

1. Tablet执行SplitTablet命令；创建前半段tablet，更新旧tablet的manifest，返还splitkey；

2、插入meta表的新tablet项；

1. 加载新tablet；
### 合并tablet流程

#### 方案1

1. 对于两个待合并的tablet，若两者key不连续，则合并失败；
1. 若两tablet不在同一个TS上，则迁移tablet到同一个TS；
1. 调用mergeTablet：unload两个tablet，合并两个tablet；
1. meta table的tablet扩展旧区间；
1. meta table的tablet删除新区间；
1. 加载旧tablet；
1. 加载新tablet；
#### 方案2

1、合并tabletA和tabletB，若两者key不连续，则合并失败；

2、Unload前半段tablet；

3、调用mergeTablet：更新后半段tablet的manifest；

4、Meta table删除前半段tablet记录；

### Tablet表状态定时收集



从该流程可以获知活跃TS表和tablet内存表的作用。

### 负载均衡决策



### 表格创建流程

1. 若table名存在，则创表失败；
1. Meta表插入schema；
1. DFS创建table名对应的目录；
1. DFS创将tablet名对应的目录；
1. 选择一个TS，执行加载Tablet；
## Master的数据结构

### Meta表结构

#### 方案1的元数据表

Key

Value

Table Name

scheam

Table Name + start key

IP:port, endkey, path

#### 方案2的元数据表

Key

Value

Table Name

scheam

Table Name + endkey

IP:port, path

### 表的schema

Tablet：表属性，包括：分裂水位，合并水位，key的类型等

LG：物理与levelDB对应，block cache配合LG的局部性，进一步提升性能；levelDB的IO特性，包括：存储介质，块大小，压缩算法等

Local group

列簇：列的逻辑属性，包括：最大最小版本数，数据存活时间等。

列簇

列

列

每个table，进行子表tablet切分；每个tablet有若干LG组成，每个tablet的所有LG共享一个操作日志；每个LG包含若干CF；每个CF包含若干列。

### 表格的key设计：

变长

变长

变长

2Bytes

2Bytes

7Bytes

1Bytes

RowKey

列族名+\0

Column Qualifier

行名长度

列名长度

时间戳

类型

该key从levelDB的角度看，是user key。通过设置key的时间戳，配合schema中执行的版本数量，可以对数据进行版本控制；与此同时，表格的多种删除操作采用【类型】字段标识；对key的控制逻辑可以做在levelDB执行compact时对key的drop策略中。

目前支持的删除包括以下几种：

- 删除指定行、列的特定版本：采用levelDB的删除接口
- 删除指定行、列的所有版本：配置列删除类型
- 删除指定行、列，特定时间之前的所有版本：配置时间戳删除类型
- 删除指定行的整个记录：配置记录删除类型
- 删除一个cf的所有列：配置列删除类型
### Master的内存数据结构

#### 活跃TS表

TS表记录了每个正在服务的TabletServer，这里的“正在服务”是以zookeeper上有该TabletServer所创建的节点为准的，与其是否真的负责Tablet无关。当zookeeper上的TabletServer节点发生变化后，Master需要更新此表（采用watch机制）。Master根据TS表完成TabletServer服务状态的获取。该部分信息与zk的TS节点一一对应。缓存了zk上的TS信息，表示当前集群的所有活跃TS。作用是根据该表，定点向此类TS节点收集信息，更新tablet表。

#### Tablet内存表

每个Tablet项记录了两类信息，一类是【元数据】，包括：位置信息（由哪个TabletServer负责）、文件路径、endkey等）；另一类是【状态信息】，包括：数据量、平均和瞬时访问压力等。其中，【元数据】与Meta Table保持最终一致性关系。Master每次获取到最新的TabletServer服务状态后要更新表中的【状态信息】；执行修复或负载均衡操作后要更新表中的【元数据】。监控子系统客户端发起查询时，Master也是以此表为依据返回系统状态的。

### ZK数据结构

Master的锁文件，每个TS对应一个锁文件。

master启动时获得master的锁；当master与ZK断开连接时，master自杀；master锁避免master多实例；

TS启动时，创建TS锁文件；当TS与ZK断开连接时，TS自杀；master通过ZK的watch机制获知TS退出集群；master通过发现新的TS锁文件，获知TS加入集群；

### DFS目录结构

Tera底层采用分布式文件系统，通过共享存储架构，大大简化了tera的设计，使得tera可以专注与表格逻辑及数据排序结构的设计。

在底层文件系统，一个tera集群的目录结构如下所示：

Tera集群的前缀路径/xxx，master和TS的配置文件中指定。

该路径下包含的子目录：meta table，user table1，user table2，…，user tablen；

Meta table的子目录：.log，.ldb，.sst，manifest，current

User table的子目录：tablet1，tablet2，…，tabletn

Tablet的子目录：.log，LG1，LG2，…，LGn

LG的子目录：.ldb，.sst，manifest，current