# ODtools
#### Python ODtools for Zkr Origin Data Department

+ 项目结构  
    ├── fdfs_client  
    ├── hbase_client    
    ├── scrapy_redis   
    ├── README.md  
    └── tools  
    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;├── fastdfs_tools.py  
    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;├── hbase_tools.py  
    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;├── redis_tools.py   
    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└── singleton_tools.py   
    
    | 文件名             |                                           |
    | ------------------ | ----------------------------------------- |
    | fdfs_client        | fastdfs客户端                             |
    | hbase_client       | hbase客户端(thirft2)                      |
    | scrapy_redis       | scrapy_redis(支持redis集群)               |
    | fastdfs_tools.py   | fastdfs工具类                             |
    | hbase_tools.py     | hbase工具类                               |
    | redis_tools.py     | redis工具类(支持单机版reids和集群版redis) |
    | singleton_tools.py | 单例模式(元类版)                          |
    |                    |                                           |
    |                    |                                           |
    |                    |                                           |
    

+ scrapy_redis

  ```python
  # 使用redis集群版scrapy 需要在scrapy项目中setting.py添加一下参数
  REDIS_HOST = '127.0.0.1'
  REDIS_PORT = 6379
  REDIS_CLUSTER = False  # 若为集群版redis 则为True
  ```

  

