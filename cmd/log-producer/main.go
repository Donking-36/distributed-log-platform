package main

import (
	"fmt"
	"io"
	"os"
	"time"
)

// main 组装真实进程依赖，并把启动失败转换为非零退出码。
func main() {
	if err := execute(
		os.LookupEnv,
		os.Stdout,
		time.Now,
	); err != nil {
		fmt.Fprintf(os.Stderr, "log-producer: %v\n", err)
		os.Exit(1)
	}
}

// execute 连接配置加载与日志生成逻辑，并保留可测试的依赖注入边界。
func execute(
	lookupEnv func(string) (string, bool),
	output io.Writer,
	now func() time.Time,
) error {
	if output == nil {
		return fmt.Errorf("output must not be nil")
	}
	if now == nil {
		return fmt.Errorf("clock must not be nil")
	}

	cfg, err := loadConfig(lookupEnv)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	if err := writeEvents(output, cfg, now); err != nil {
		return fmt.Errorf("write events: %w", err)
	}

	return nil
}
