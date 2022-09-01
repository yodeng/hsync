# Hsync数据传输及同步

`Hsync`是用于搭建基于`https`协议的数据远程传输和同步的软件，基于`C/S`架构，依赖服务端`hsyncd`守护进程，客户端通过hsync与服务端进行数据传输，默认端口为`10808`，支持异步分块，可中断的，随时恢复的数据传输方式。

### 1. 依赖

#### 1.1 运行环境

+ linux64
+ openssl
+ python >=3.7

#### 1.2 其他python模块依赖

+ Cython
+ requests
+ aiohttp
+ tqdm

### 2. 安装

> git仓库安装 (for recommend)

```shell
pip3 install git+https://github.com/yodeng/hsync.git
```

### 3. 使用

hsync包括客户端和服务端程序，传输连接采用强SSL认证，使用前须配置客户端证书和密钥，然后启动服务端hsyncd服务。

对于无证书的请求，拒绝访问，有证书的请求，会对证书进行验证，验证通过允许访问，验证不通过拒绝访问。

#### 3.1 创建证书及密钥

##### 3.1.1 服务端

服务端使用`hsync-keygen`命令生成根证书，服务端证书和授权的客户端证书

证书输出文件夹为：`$HSYNC_DIR/cert/`

一共包括6个文件：`ca.key`， `ca.pem`， `hsyncd.crt`， `hsyncd.key`，`hsync.crt`， `hsync.key`

将`ca.pem`，`hsync.crt`和`hsync.key`这3个文件拷贝到客户端`$HSYNC_DIR/cert/`目录即可完成客户端授权

不要将服务端其他文件拷贝到客户端

##### 3.1.2 客户端

将服务端`ca.pem`，`hsync.crt`和`hsync.key`证书文件拷贝到客户端`$HSYNC_DIR/cert/`文件夹

客户端有了证书文件，即完成授权，能正常连接服务端。

#### 3.2 服务端启动

安装完成后可通过`hsyncd`命令启动和管理服务端程序。

##### 3.2.1 hsyncd命令参数

```
$ hsyncd -h 
usage: hsyncd [-h] [-host <str>] [-p <int>] [-l <str>] [-d]

hsyncd server daemon process

optional arguments:
  -h, --help            show this help message and exit
  -host <str>, --host-ip <str>
                        hsyncd server host ip, 0.0.0.0 by default
  -p <int>, --port <int>
                        hsyncd port 10808 by default
  -l <str>, --log <str>
                        hsyncd logging file, /home/dengyong/.hsync/hsyncd.log by default
  -d, --daemon          daemon process
```

命令参数解释如下：

| 参数            | 描述                                                         |
| --------------- | ------------------------------------------------------------ |
| -h/--help       | 打印参数帮助并退出                                           |
| -host/--host-ip | 服务端hsyncd程序绑定的主机ip地址，默认`0.0.0.0`，代表绑定所有物理网卡ip |
| -p/--port       | hsyncd服务使用的端口号，默认`10808`，要求端口不能被防火墙阻止 |
| -l/--log        | hsyncd服务端日志文件输出，默认`$HOME/.hsync/hsyncd.log`      |
| -d/--daemon     | 表示启动后台守护进程                                         |

+ 服务通过TCP协议传输，要求服务端网卡端口不能被防火墙阻止，否则客户端无法连接

##### 3.2.2 hsyncd服务管理

###### (1) 服务启动

前台启动:  直接使用命令`hsyncd`加相关参数即可

后台启动：需添加`-d`参数，或使用`hsyncd start`加相关参数即可， 程序会在后台运行

###### (2) 服务重启

后台运行的`hsyncd`，可直接通过`hsyncd restart`命令重新启动

###### (3) 服务终止

后台运行的`hsyncd`，可直接通过`hsyncd stop`命令终止，或直接`kill`掉相关进程即可

#### 3.3 客户端

客户端通过`hscp`命令从服务端拉取数据，通过`hsync`命令从服务端同步数据，会跳过服务端的隐藏文件

##### 3.3.1 拉取数据

直接从服务端拉取数据，类似于scp功能，不同的是`hscp`通过`https`协议传输，是异步传输，支持断点续传，并发拉取，速度更快。

```
$ hscp -h 
usage: hscp [-h] -i <str> [-host <str>] [-p <int>] [-n <int>] [-o <str>]

hscp for remote file copy

optional arguments:
  -h, --help            show this help message and exit
  -i <str>, --input <str>
                        input remote path
  -host <str>, --host-ip <str>
                        connect host ip, localhost by default
  -p <int>, --port <int>
                        connect port, 10808 by default
  -n <int>, --num <int>
                        max file copy in parallely, 3 by default
  -o <str>, --output <str>
                        output path
```

命令参数解释如下：

| 参数         | 描述                                                         |
| ------------ | ------------------------------------------------------------ |
| -h/--help    | 打印参数帮助并退出                                           |
| -i/--input   | 远程待传输的路径（文件或文件夹）                             |
| -h/--host-ip | 连接的远程主机IP                                             |
| -p/--port    | 连接的远程主机https端口，10808默认                           |
| -n/--num     | 同事拉取的最大文件数，默认3个，采用多进程，每个进程均使用异步进程拉取 |
| -o/--output  | 保存到本地的路径（文件或文件夹），文件夹不存在会自动创建     |

##### 3.3.2 同步数据

同步数据，当远程数据大小改变（`size`），内容修改（`mtime`）时，会将更新同步到本地，远程数据被删除时，已同步的数据不会删除。

传输过程为增量传输，支持断点续传，传输完成后会对已传输的数据进行`MD5`校验。

同步进程会一直等待远程端产生或更新数据，直到进程被杀掉。

同步到本地的数据，只保证数据内容一样，不保证相关时间戳和文件元信息一致，文件所属组为命令使用的用户。

可提前启动同步命令，等待远程有数据产生时，会自动拉取到本地。

```
$ hsync -h 
usage: hsync [-h] -i <str> [-host <str>] [-p <int>] [-o <str>]

hsync for remote file synchronize

optional arguments:
  -h, --help            show this help message and exit
  -i <str>, --input <str>
                        input remote path
  -host <str>, --host-ip <str>
                        connect host ip, localhost by default
  -p <int>, --port <int>
                        connect port, 10808 by default
  -o <str>, --output <str>
                        output path
```

命令参数解释如下：

| 参数         | 描述                                                     |
| ------------ | -------------------------------------------------------- |
| -h/--help    | 打印参数帮助并退出                                       |
| -i/--input   | 远程待传输的路径（文件或文件夹）                         |
| -h/--host-ip | 同步的远程主机IP                                         |
| -p/--port    | 同步的远程主机https端口，10808默认                       |
| -o/--output  | 保存到本地的路径（文件或文件夹），文件夹不存在会自动创建 |

### 4. 配置管理

程序部分参数可通过配置文件管理。

#### 4.1 环境变量

程序会识别一个环境变量`HSYNC_DIR`， 当存在该环境变量时，相关配置可以从环境变量指定的目录中读取，不存在时，默认为`$HOME/.hsync`目录。

服务端程序的默认日志文件会存放到该目录之下，服务端进程管理的`pidfile`文件也会存放到该目录之下

如果目录下存在`hsync.ini`配置文件，也会优先加载此目录下的`hsync.ini`配置，

#### 4.2 配置文件说明

hsync会优先识别`$HSYNC_DIR`目录下的`hsync.ini`配置文件。

配置参数加载顺序为:    **参数选项 > `$HSYNC_DIR/hsync.ini` > `$install_dir/hsync.ini`**

该文件的相关配置描述如下：

```
[hsyncd]                           ## 服务端hsyncd相关配置
Host_ip =                          ## hsyncd服务绑定的主机ip, 不指定或值为*则默认为全部网卡，命令等同于-host/--host-ip参数
Port = 10808                       ## hsyncd服务绑定的主机ip, 不指定则默认为10808，端口不能被防火墙阻止，命令等同于-p/--port参数
Forbidden_file = *.fa, *.fq        ## 服务端禁止客户端传输的文件规则，多个规则使用空白或逗号分割。
Forbidden_dir = /etc/              ## 服务端禁止客户端传输的文件夹绝对路径，多个规则使用空白或逗号分割。
Allowed_host =                     ## 服务端允许连接的客户端ip，多个ip使用空白或逗号分割，非指定的ip则不允许连接，不指定表示默认所有ip可连接服务端，会不安全，建议限制ip, 填写时应注意网络状态，如果有负载均衡或proxy服务器，应填实际直接连接的ip
MD5_check_nproc = 8                ## 服务端数据校验后台进程数
Hsync_verify = yes                 ## 服务端开启证书验证，yes表示只允许带证书请求，并对证书进行验证，若不为yes, 代表允许无证书请求。

[hscp]                             ## hscp命令相关配置
Host_ip =                          ## hsync连接的hsyncd服务器ip，需网络可达，默认为localhost
Port = 10808                       ## hsync连接的hsyncd服务器端口，需网络可达且服务端端口无防火墙
Max_tcp_conn = 100                 ## hsync传输的最大TCP连接数，默认100
Max_part_num = 100                 ## hsync传输的最大分块传输数，默认100
Max_runing = 100                   ## hsync传输的同时传输的数据分块数，默认100
Data_timeout = 30                  ## hsync连接hsyncd服务的超时时间，默认30秒

[hsync]                            ## hsync命令相关配置
Host_ip =                          ## hsync连接的hsyncd服务器ip，需网络可达，默认为localhost          
Port = 10808                       ## hsync连接的hsyncd服务器端口，需网络可达且服务端端口无防火墙
Max_tcp_conn = 100                 ## hsync传输的最大TCP连接数，默认100
Max_runing = 20                    ## hsync同步时同时传输的最大文件数，当远程路径为文件夹时生效，会异步传输文件夹下的文件，默认20
Data_timeout = 30                  ## hsync连接hsyncd服务的超时时间，默认30秒
Max_timeout_retry = 5              ## hsync连接hsyncd服务的最大允许超时次数，默认5次，超过5次超时会终止进程
Sync_interval_sec = 2              ## hsync同步的时间间隔，默认2秒
```

+ 配置参数会被命令行选项参数覆盖，没有选项参数时使用配置文件中的参数值
+ 配置文件中的值，不需要加引号

#### 4.3 配置查询

使用命令`hsync-echo-config`可打印配置文件及其参数信息

```
$ hsync-echo-config 
Configuration files to search (order by order):
 - /home/user/.hsync/hsync.ini
 - /share/user/soft/miniconda3/lib/python3.9/site-packages/hsync/hsync.ini

Available Config:
[hsyncd]
 - Allowed_host : 
 - Forbidden_dir : /etc/
 - Forbidden_file :
 - Host_ip : 
 - Port : 10808
 - MD5_check_nproc : 8
 - Hsync_verify = yes
[hscp]
 - Data_timeout : 30
 - Host_ip : 127.0.0.1
 - Max_part_num : 100
 - Max_runing : 100
 - Max_tcp_conn : 100
 - Port : 10808
[hsync]
 - Data_timeout : 30
 - Host_ip : 127.0.0.1
 - Max_runing : 100
 - Max_tcp_conn : 100
 - Max_timeout_retry : 5
 - Port : 10808
```
