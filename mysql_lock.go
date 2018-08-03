// Reuse existing mysql server setup for distributed locking
// https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_get-lock
// https://www.xaprb.com/blog/2006/07/26/how-to-coordinate-distributed-work-with-mysqls-get_lock/

package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"

	_ "github.com/go-sql-driver/mysql"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type DbConfig struct {
	Host     string
	Port     int
	User     string
	Password string
}

type Config struct {
	Db DbConfig
}

func main() {

	var (
		cfg Config

		lockAquired bool

		cfgFile     = flag.StringP("config", "c", "/etc/mysql_lock.yaml", "configuration file")
		lockName    = flag.StringP("name", "n", "cron", "lock name")
		lockTimeout = flag.DurationP("timeout", "t", 0, "lock acquisition timeout")
	)

	viper.AutomaticEnv()

	flag.Usage = func() {
		fmt.Printf("  %s [flags] -- <command> [argument ..]\n\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	flag.Parse()

	if flag.NArg() < 1 {
		os.Exit(1)
	}

	viper.SetConfigType("yaml")
	viper.SetConfigFile(*cfgFile)

	cfg.Db.Host = "127.0.0.1"
	cfg.Db.Port = 3306

	err := viper.ReadInConfig()
	hasError(err)

	err = viper.Unmarshal(&cfg)
	hasError(err)

	conn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/", cfg.Db.User, cfg.Db.Password, cfg.Db.Host, cfg.Db.Port))
	hasError(err)
	defer conn.Close()

	stmtOut, err := conn.Prepare("SELECT COALESCE(GET_LOCK(?, ?), 0)")
	hasError(err)

	err = stmtOut.QueryRow(*lockName, lockTimeout.Seconds()).Scan(&lockAquired)
	hasError(err)

	cmd, args := flag.Args()[0], flag.Args()[1:]

	if lockAquired {
		cmd := exec.Command(cmd, args...)

		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		err = cmd.Run()
		if err != nil {
			// todo: exit with cmd exit code
			os.Exit(-5)
		}
	} else {
		os.Exit(-10)
	}
}

func hasError(e error) {
	if e != nil {
		log.Fatalln(e)
	}
}
