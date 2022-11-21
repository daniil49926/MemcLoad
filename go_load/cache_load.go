package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
)

type AppInstall struct {
	dev_type string
	dev_id   string
	lat      string
	lon      string
	apps     []string
}

var ctx = context.Background()

func init_env() {
	if err := godotenv.Load(); err != nil {
		log.Print("No .env file in project")
	}
}

func GetClientConn(host string, username string, pwd string) redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr: host, Username: username, Password: pwd,
	})
	return *rdb
}

func GetClient() map[string]redis.Client {
	rdb_host, exists_host := os.LookupEnv("REDIS_HOST")
	rdb_pwd, exists_pwd := os.LookupEnv("REDIS_PWD")
	rdb_username, exist_username := os.LookupEnv("REDIS_LOGIN")
	rdb_port_idfa, exists_idfa := os.LookupEnv("REDIS_idfa")
	rdb_port_gaid, exists_gaid := os.LookupEnv("REDIS_gaid")
	rdb_port_adid, exists_adid := os.LookupEnv("REDIS_adid")
	rdb_port_dvid, exists_dvid := os.LookupEnv("REDIS_dvid")

	redis_connects_host := map[string]string{
		"idfa": rdb_host + ":" + rdb_port_idfa,
		"gaid": rdb_host + ":" + rdb_port_gaid,
		"adid": rdb_host + ":" + rdb_port_adid,
		"dvid": rdb_host + ":" + rdb_port_dvid,
	}

	if !(exists_host || exists_pwd || exists_idfa || exists_gaid || exists_adid || exists_dvid || exist_username) {
		panic("No data for connecting to Redis")
	}

	redis_connects_dict := make(map[string]redis.Client)

	for key, value := range redis_connects_host {
		redis_connects_dict[key] = GetClientConn(value, rdb_username, rdb_pwd)
	}
	return redis_connects_dict
}

func SetInRedis(idx int, redis_conn_dict map[string]redis.Client, buff chan AppInstall, wg *sync.WaitGroup) {
	log.Println("Start worker", idx)
	defer wg.Done()
	defer log.Println("Stop worker", idx)
	for {
		app_install, ok := <-buff
		if ok {
			key := app_install.dev_type + ":" + app_install.dev_id
			value := app_install.lat + ":" + app_install.lon + "-" + fmt.Sprintf("%s", app_install.apps)
			redis_conn := redis_conn_dict[app_install.dev_type]
			err := redis_conn.Set(ctx, key, value, 0).Err()
			if err != nil {
				fmt.Println("Error in set!")
			}
		} else {
			return
		}
	}

}

func ReadGzipFile(filepath string) (chan []byte, chan error) {
	log.Println("Start read", filepath)
	raw_file, err := os.Open(filepath)
	if err != nil {
		return nil, nil
	}
	raw_contents, err := gzip.NewReader(raw_file)
	if err != nil {
		return nil, nil
	}

	contents := bufio.NewScanner(raw_contents)
	cbuff := make([]byte, 0, bufio.MaxScanTokenSize)
	contents.Buffer(cbuff, bufio.MaxScanTokenSize*50)
	lines := make(chan []byte)
	errors := make(chan error)
	go func(lines chan []byte, errors chan error, contents *bufio.Scanner) {
		defer func(lines chan []byte, errors chan error) {
			close(lines)
			close(errors)
			raw_file.Close()
		}(lines, errors)
		var err error
		for contents.Scan() {
			lines <- contents.Bytes()
		}
		if err = contents.Err(); err != nil {
			errors <- err
			return
		}
	}(lines, errors, contents)
	log.Println("End read", filepath)
	return lines, nil
}

func ParseLine(lines chan []byte, buff chan AppInstall) {
	defer close(buff)
	for line := range lines {
		app_to_ret := new(AppInstall)
		split_line := strings.Split(string(line), "\t")

		split_apps := strings.Split(split_line[4], ",")
		apps := []string{}
		for _, i_app := range split_apps {
			apps = append(apps, i_app)
		}
		app_to_ret.dev_type = split_line[0]
		app_to_ret.dev_id = split_line[1]
		app_to_ret.lat = split_line[2]
		app_to_ret.lon = split_line[3]
		app_to_ret.apps = apps
		buff <- *app_to_ret
	}
	log.Println("Finish parseline")
}

func DotRename(filepath string) {
	split_path := strings.Split(filepath, "\\")
	log.Println(split_path)
	split_path = append(split_path, split_path[len(split_path)-1])
	split_path[len(split_path)-2] = "."
	var new_path string
	temp_sign := "\\"
	for _, chank_path := range split_path {
		if chank_path == "." {
			temp_sign = ""
		}
		new_path += chank_path + temp_sign
	}
	err := os.Rename(filepath, new_path)
	if err != nil {
		log.Println(err)
		log.Printf("Couldn't rename %s to %s", filepath, new_path)
	}
}

func main() {
	var wg sync.WaitGroup
	const workersCount = 16
	var buffer = make(chan AppInstall, workersCount)
	start_tm := time.Now()
	init_env()
	connects := GetClient()

	fmt.Println("Start")
	files_path, err := filepath.Glob("data/appsinstalled/2*.tsv.gz")
	if err != nil || len(files_path) == 0 {
		log.Println("No matching files")
	}
	for _, path := range files_path {
		lines, errors := ReadGzipFile(path)
		if errors != nil {
			log.Println("An error occurred while reading")
			panic(errors)
		}
		go ParseLine(lines, buffer)

		for i := 0; i < workersCount; i++ {
			wg.Add(1)
			go SetInRedis(i, connects, buffer, &wg)
		}
		wg.Wait()

		DotRename(path)
	}
	log.Printf("Run time: %s", time.Since(start_tm))
}

// Processing two tsv.gz by 512mb on 16 cores = 5.29 min
