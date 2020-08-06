// Licensed to Preferred Networks, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Preferred Networks, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/go-playground/validator/v10"

	"github.com/creasty/defaults"
	homedir "github.com/mitchellh/go-homedir"
	backendconfig "github.com/pfnet-research/pftaskqueue/pkg/backend/config"
	backendfactory "github.com/pfnet-research/pftaskqueue/pkg/backend/factory"
	backend "github.com/pfnet-research/pftaskqueue/pkg/backend/iface"
	workerconfig "github.com/pfnet-research/pftaskqueue/pkg/worker/config"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	// flags which viper doesn't override
	cfgFile     string
	displayOpts bool

	// command context for signal handling
	cmdContext, cmdContextCancel = context.WithCancel(context.Background())

	// viper managed options
	cmdOpts        CmdOption
	defaultCmdOpts CmdOption // just for print-default-config command

	// backend instance which can be initialized by mustInitializeQueueBackend()
	queueBackend backend.Backend

	logger = log.Logger
)

type CmdOption struct {
	Backend string                    `json:"backend" yaml:"backend" default:"redis"`
	Redis   RedisCmdOption            `json:"redis" yaml:"redis"`
	Worker  workerconfig.WorkerConfig `json:"worker" yaml:"worker"`
	Log     LogOption                 `json:"log" yaml:"log"`
}

type LogOption struct {
	Level  string `json:"level" yaml:"level" default:"info"`
	Pretty bool   `json:"pretty" yaml:"pretty" default:"true"`
}

type RedisCmdOption struct {
	KeyPrefix string `json:"keyPrefix" yaml:"keyPrefix" default:"-" validate:"required,min=1,max=128"`

	backendconfig.RedisClientConfig `json:",inline" yaml:",inline" validate:"-" mapstructure:",squash"`

	Backoff backendconfig.BackoffConfig `json:"backoff" yaml:"backoff" validate:"-"`
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:     "pftaskqueue",
	Short:   "Lightweight Task Queue Tool",
	Long:    `Lightweight Task Queue Tool`,
	Version: "dummy", // Version string overridden in init()
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		cmdContextCancel()
	},
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// setup signal handler for graceful shutdown
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, os.Interrupt /* Windows */)
		go func() {
			sig := <-sigs
			logger.Info().Str("signal", sig.String()).Msg("Signal received. Stopping all the on-going tasks")
			cmdContextCancel()
		}()

		// logger initialization
		switch strings.ToLower(cmdOpts.Log.Level) {
		case "trace":
			logger = logger.Level(zerolog.TraceLevel)
		case "debug":
			logger = logger.Level(zerolog.DebugLevel)
		case "info":
			logger = logger.Level(zerolog.InfoLevel)
		case "warn":
			logger = logger.Level(zerolog.WarnLevel)
		case "error":
			logger = logger.Level(zerolog.ErrorLevel)
		default:
			logger.Fatal().
				Err(errors.Errorf("%s is invalid log level", cmdOpts.Log.Level)).
				Msg("Can't initialize logger")
		}
		if cmdOpts.Log.Pretty {
			logger = logger.Output(zerolog.ConsoleWriter{
				Out: os.Stderr,
			})
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	if err := defaults.Set(&defaultCmdOpts); err != nil {
		panic(err)
	}
	if err := defaults.Set(&cmdOpts); err != nil {
		panic(err)
	}

	rootCmd.SetVersionTemplate(VersionString())
	flag := rootCmd.PersistentFlags()
	flag.StringVar(&cfgFile, "config", "", "config file path. [default = $PFTQCONFIG or $HOME/.pftaskqueue.yaml]")
	flag.BoolVar(&displayOpts, "display-options", false, "display loaded config values at startup")

	// Log setting
	flag.String("log-level", cmdOpts.Log.Level, `set loglevel (one of "error, warn, info, debug, trace)`)
	viperBindPFlag("Log.Level", cmdOpts.Log.Level, flag.Lookup("log-level"))

	flag.Bool("log-pretty", cmdOpts.Log.Pretty, `set pretty logging(human-friendly & colorized output), json logging if false`)
	viperBindPFlag("Log.Pretty", strconv.FormatBool(cmdOpts.Log.Pretty), flag.Lookup("log-pretty"))

	// Backend Type
	flag.String("backend", cmdOpts.Backend, "set taskqueue backend (currently supports only 'redis')")
	viperBindPFlag("Backend", cmdOpts.Backend, flag.Lookup("backend"))

	// Redis.KeyPrefix
	flag.String("redis-keyprefix", cmdOpts.Redis.KeyPrefix, "key prefix for all the keys stored to redis.  This works as pseudo namespace when you shared redis DB among others")
	viperBindPFlag("Redis.KeyPrefix", cmdOpts.Redis.KeyPrefix, flag.Lookup("redis-keyprefix"))

	// Redis.ClientConfig
	flag.String("redis-addr", cmdOpts.Redis.Addr, "address of redis server.")
	viperBindPFlag("Redis.Addr", cmdOpts.Redis.Addr, flag.Lookup("redis-addr"))

	flag.String("redis-password", cmdOpts.Redis.Password, "password of redis server.")
	viperBindPFlag("Redis.Password", cmdOpts.Redis.Password, flag.Lookup("redis-password"))

	flag.Int("redis-db", cmdOpts.Redis.DB, "DB of redis server")
	viperBindPFlag("Redis.DB", strconv.Itoa(cmdOpts.Redis.DB), flag.Lookup("redis-db"))

	flag.Duration("redis-dial-timeout", cmdOpts.Redis.DialTimeout, "dial timeout of redis client")
	viperBindPFlag("Redis.DialTimeout", cmdOpts.Redis.DialTimeout.String(), flag.Lookup("redis-dial-timeout"))

	flag.Duration("redis-read-timeout", cmdOpts.Redis.ReadTimeout, "read timeout of redis client")
	viperBindPFlag("Redis.ReadTimeout", cmdOpts.Redis.ReadTimeout.String(), flag.Lookup("redis-read-timeout"))

	flag.Duration("redis-write-timeout", cmdOpts.Redis.WriteTimeout, "write timeout of redis client")
	viperBindPFlag("Redis.WriteTimeout", cmdOpts.Redis.WriteTimeout.String(), flag.Lookup("redis-write-timeout"))

	flag.Int("redis-pool-size", cmdOpts.Redis.PoolSize, "connection pool size of redis client. if set 0, (10 * runtime.NumCPU) will be used")
	viperBindPFlag("Redis.PoolSize", strconv.Itoa(cmdOpts.Redis.PoolSize), flag.Lookup("redis-pool-size"))

	flag.Int("redis-min-idle-conns", cmdOpts.Redis.MinIdleConns, "minimum number of idle connections of redis connection pool")
	viperBindPFlag("Redis.MinIdleConns", strconv.Itoa(cmdOpts.Redis.MinIdleConns), flag.Lookup("redis-min-idle-conns"))

	flag.Duration("redis-max-conn-age", cmdOpts.Redis.MaxConnAge, "connection age at which client retires (closes) the connection in redis connection pool")
	viperBindPFlag("Redis.MaxConnAge", cmdOpts.Redis.MaxConnAge.String(), flag.Lookup("redis-max-conn-age"))

	flag.Duration("redis-pool-timeout", cmdOpts.Redis.PoolTimeout,
		"amount of time redis client waits for connection if all connections are busy before returning an error",
	)
	viperBindPFlag("Redis.PoolTimeout", cmdOpts.Redis.PoolTimeout.String(), flag.Lookup("redis-pool-timeout"))

	flag.Duration("redis-idle-timeout", cmdOpts.Redis.IdleTimeout,
		"amount of time after which client closes idle connections. should be less than server's timeout. -1 disables ide timeout check",
	)
	viperBindPFlag("Redis.IdleTimeout", cmdOpts.Redis.IdleTimeout.String(), flag.Lookup("redis-idle-timeout"))

	flag.Duration("redis-idle-check-frequency", cmdOpts.Redis.IdleCheckFrequency,
		"frequency of idle checks made by idle connections reaper. -1 disables idle connections reaper, but idle connections are still discarded by the client if IdleTimeout is set",
	)
	viperBindPFlag("Redis.IdleCheckFrequency", cmdOpts.Redis.IdleCheckFrequency.String(), flag.Lookup("redis-idle-check-frequency"))

	// BackoffConfig for redis
	flag.Duration("redis-backoff-initial-interval", cmdOpts.Redis.Backoff.InitialInterval, "initial interval of exponential backoff used in redis operation")
	viperBindPFlag("Redis.Backoff.InitialInterval", cmdOpts.Redis.Backoff.InitialInterval.String(), flag.Lookup("redis-backoff-initial-interval"))

	flag.Float64("redis-backoff-randomization-factor", cmdOpts.Redis.Backoff.RandomizationFactor, "randomization factor of exponential backoff used in redis operation")
	viperBindPFlag("Redis.RandomizationFactor",
		strconv.FormatFloat(cmdOpts.Redis.Backoff.RandomizationFactor, 'g', -1, 64),
		flag.Lookup("redis-backoff-randomization-factor"),
	)

	flag.Float64("redis-backoff-multiplier", cmdOpts.Redis.Backoff.Multiplier, "multiplier of exponential backoff used in redis operation")
	viperBindPFlag("Redis.Backoff.Multiplier",
		strconv.FormatFloat(cmdOpts.Redis.Backoff.Multiplier, 'g', -1, 64),
		flag.Lookup("redis-backoff-multiplier"),
	)

	flag.Duration("redis-backoff-max-interval", cmdOpts.Redis.Backoff.MaxInterval, "maximum interval of exponential backoff used in redis operation.")
	viperBindPFlag("Redis.MaxInterval", cmdOpts.Redis.Backoff.MaxInterval.String(), flag.Lookup("redis-backoff-max-interval"))

	flag.Duration("redis-backoff-max-elapsed-time", cmdOpts.Redis.Backoff.MaxElapsedTime, "maximum elapsed time of exponential backoff used in redis operation. 0 never stops backoff")
	viperBindPFlag("Redis.Backoff.MaxElapsedTime", cmdOpts.Redis.Backoff.MaxElapsedTime.String(), flag.Lookup("redis-backoff-max-elapsed-time"))

	flag.Int64("redis-backoff-max-retry", cmdOpts.Redis.Backoff.MaxRetry, "maximum retry limit of exponential backoff used in redis operation. this value take precedence over max-elapsed-time")
	viperBindPFlag("Redis.Backoff.MaxRetry",
		strconv.FormatFloat(float64(cmdOpts.Redis.Backoff.MaxRetry), 'g', -1, 64),
		flag.Lookup("redis-backoff-max-retry"),
	)
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else if env, ok := os.LookupEnv("PFTQCONFIG"); ok {
		// Use config file from env var.
		viper.SetConfigFile(env)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".pftaskqueue" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".pftaskqueue")
	}

	viper.SetEnvPrefix("pftq")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv() // read in environment variables that match

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			panic(err)
		}
	}

	// Custom mutatation from pflag values to viper values only when the value is differ from the default
	// viper.BindPFlag mutates default flag value to all the viper value
	// see: https://github.com/spf13/viper/issues/671
	mergePFlagToViper()

	// load config values from configfile(if found) and autoenv(if found)
	if err := viper.Unmarshal(&cmdOpts); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func mustInitializeQueueBackend() {
	switch strings.ToLower(cmdOpts.Backend) {
	case "redis":
		v := validator.New()
		if err := v.Struct(&cmdOpts.Redis); err != nil {
			logger.Fatal().Err(err).Msg("Redis Validation error")
		}

		redisClient := cmdOpts.Redis.NewClient()
		if err := redisClient.Ping().Err(); err != nil {
			logger.Fatal().Err(err).Msg("Can't connect to redis server")
		}

		var err error
		queueBackend, err = backendfactory.NewBackend(logger, backendconfig.Config{
			BackendType: cmdOpts.Backend,
			Redis: &backendconfig.RedisConfig{
				KeyPrefix: cmdOpts.Redis.KeyPrefix,
				Client:    cmdOpts.Redis.NewClient(),
				Backoff:   cmdOpts.Redis.Backoff,
			},
		})

		if err != nil {
			logger.Fatal().Err(err).Msg("Can't initialize backend")
		}
	default:
		logger.Fatal().
			Err(errors.Errorf("%s is invalid backend", cmdOpts.Backend)).
			Msg("Can't initialize backend")
	}
}

func displayCmdOptsIfEnabled() {
	if displayOpts {
		bytes, _ := json.Marshal(&cmdOpts)
		logger.Info().RawJSON("options", bytes).Msg("loaded options")
	}
}
