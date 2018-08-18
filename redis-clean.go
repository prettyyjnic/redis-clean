package main

import (
	"github.com/go-redis/redis"
	"time"
	"log"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

var (
	MAX_ROWS int64 = 500
)

var opts redis.Options
var readTimeout int
var writeTimeout int
var keyPrefix string

var rootCmd = &cobra.Command{
	Use:   "scavenger",
	Short: "Scavenger is a redis clean tool",
	Long: `Scavenger is a redis clean tool built with Go.
It use scan method to find keys and delete big keys, so it would not block the redis`,
	Args: func(cmd *cobra.Command, args []string) error {
		if keyPrefix == "" {
			return errors.New("keys can not be empty")
		}
		if keyPrefix == "*" {
			return errors.New("keys can not be '*', it is not safe!")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		// Do Stuff Here
		excute()
	},
}

func init(){
	rootCmd.PersistentFlags().StringVar(&keyPrefix, "keys", "", "the keys's prefix you want to del")
	rootCmd.PersistentFlags().StringVar(&opts.Addr, "addr", "127.0.0.1:6379", "the redis's address, example: 127.0.0.1:6379")
	rootCmd.PersistentFlags().StringVar(&opts.Password, "pass", "", "the redis's password")
	rootCmd.PersistentFlags().IntVar(&readTimeout, "readTimeout", 10, "read timeout(seconds)")
	rootCmd.PersistentFlags().IntVar(&writeTimeout, "writeTimeout", 10, "write timeout(seconds)")

	rootCmd.MarkFlagRequired("keys")
}

func excute(){
	opts.ReadTimeout = time.Second * time.Duration(readTimeout)
	opts.WriteTimeout = time.Second * time.Duration(writeTimeout)

	conn := redis.NewClient(&opts)
	defer conn.Close()
	if err := conn.Ping().Err(); err!=nil{
		fmt.Println("连接redis失败:", err.Error())
		os.Exit(1)
	}
	var now time.Time
	var startTime time.Time
	var counts int
	if keyPrefix[len(keyPrefix)-1] != '*' {// 不是* 结尾
		now = time.Now()
		counts = 1
		log.Printf("删除key：%s 开始 \n", keyPrefix)
		err := delKey(conn, keyPrefix)
		if err != nil {
			panic(err.Error())
		}
		log.Printf("删除key：%s 成功, 耗时：%.2f 秒 \n", keyPrefix, time.Now().Sub(now).Seconds())
	}else{
		// 批量删除
		var cursor uint64 = 0
		startTime = time.Now()
		for {
			result := conn.Scan(cursor, keyPrefix, MAX_ROWS)
			if result.Err() != nil {
				panic(result.Err())
			}
			var keys []string
			keys, cursor = result.Val()
			counts+=len(keys)
			for i := 0; i < len(keys); i++ {
				// 删除 key
				now = time.Now()
				log.Printf("删除key：%s 开始 \n", keys[i])
				err := delKey(conn, keys[i])
				if err != nil {
					panic(err.Error())
				}
				log.Printf("删除key：%s 成功, 耗时：%.2f 秒 \n", keys[i], time.Now().Sub(now).Seconds())
			}
			if cursor == 0 {
				break;
			}
		}
	}
	fmt.Printf("总删除key: %d 个 耗时: %.2f 秒 \n", counts, time.Now().Sub(startTime).Seconds())
}

func main() {

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func delKey(conn *redis.Client, key string) error {
	var err error
	var keyType string
	keyType, err = conn.Type(key).Result()
	if err != nil {
		goto END
	}
	switch keyType {
	case "string":
		err = conn.Del(key).Err()
	case "list":
		var sizes int64
		sizes, err = conn.LLen(key).Result()
		if err != nil {
			goto END
		}
		var i int64 = 0
		for ; i < sizes; i += MAX_ROWS {
			err := conn.LTrim(key, i, i+MAX_ROWS).Err()
			if err != nil {
				goto END
			}
		}
		err = conn.Del(key).Err()
	case "hash":
		var cursor uint64
		var fields []string
		for {
			fields, cursor, err = conn.HScan(key, cursor, "*", MAX_ROWS).Result()
			if err != nil {
				goto END
			}
			if len(fields) > 0 {
				_, err = conn.HDel(key, fields...).Result()
				if err != nil {
					goto END
				}
			}

			if cursor == 0 {
				break;
			}
		}
		err = conn.Del(key).Err()
	case "set":
		var cursor uint64
		var fields []string
		for {
			fields, cursor, err = conn.SScan(key, cursor, "*", MAX_ROWS).Result()
			if err != nil {
				goto END
			}
			if len(fields) > 0 {
				var members []interface{}
				members = make([]interface{}, len(fields))
				for i := 0; i < len(fields); i++ {
					members[i] = fields[i]
				}
				_, err = conn.SRem(key, members...).Result()
				if err != nil {
					goto END
				}
			}

			if cursor == 0 {
				break;
			}
		}
		err = conn.Del(key).Err()
	case "zset":
		var cursor uint64
		var fields []string
		for {
			fields, cursor, err = conn.ZScan(key, cursor, "*", MAX_ROWS).Result()
			if err != nil {
				goto END
			}
			if len(fields) > 0 {
				var members []interface{}
				members = make([]interface{}, len(fields))
				for i := 0; i < len(fields); i++ {
					members[i] = fields[i]
				}
				_, err = conn.ZRem(key, members...).Result()
				if err != nil {
					goto END
				}
			}

			if cursor == 0 {
				break;
			}
		}
		err = conn.Del(key).Err()
	case "none":
		// 已经删除了的
	default:
		return errors.New("未知的类型："+keyType)
	}
END:
	if err != nil {
		if err == redis.Nil { // 空key
			return nil
		}
		return err
	}
	return nil
}
