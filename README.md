[![Build Status](https://travis-ci.org/prettyyjnic/redis-clean.svg?branch=master)](https://travis-ci.org/prettyyjnic/redis-clean)

### 批量安全无阻塞删除redis 的key

#### 安装依赖
```
go get -u "github.com/go-redis/redis"
go get -u "github.com/spf13/cobra"
```

#### 使用
```
go install github.com/prettyyjnic/redis-clean

redis-clean --addr 127.0.0.1:6379 --keys test*
```
