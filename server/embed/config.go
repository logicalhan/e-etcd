// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package embed

import (
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/logutil"
	"go.etcd.io/etcd/client/pkg/v3/srv"
	"go.etcd.io/etcd/client/pkg/v3/tlsutil"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/etcd/client/pkg/v3/types"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/pkg/v3/flags"
	"go.etcd.io/etcd/pkg/v3/netutil"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3compactor"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3discovery"
	"go.etcd.io/etcd/server/v3/storage/backend"

	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"sigs.k8s.io/yaml"

	bolt "go.etcd.io/bbolt"
)

const (
	ClusterStateFlagNew      = "new"
	ClusterStateFlagExisting = "existing"

	DefaultName                        = "default"
	DefaultMaxSnapshots                = 5
	DefaultMaxWALs                     = 5
	DefaultMaxTxnOps                   = uint(128)
	DefaultWarningApplyDuration        = 100 * time.Millisecond
	DefaultWarningUnaryRequestDuration = 300 * time.Millisecond
	DefaultMaxRequestBytes             = 1.5 * 1024 * 1024
	DefaultMaxConcurrentStreams        = math.MaxUint32
	DefaultGRPCKeepAliveMinTime        = 5 * time.Second
	DefaultGRPCKeepAliveInterval       = 2 * time.Hour
	DefaultGRPCKeepAliveTimeout        = 20 * time.Second
	DefaultDowngradeCheckTime          = 5 * time.Second
	DefaultWaitClusterReadyTimeout     = 5 * time.Second
	DefaultAutoCompactionMode          = "periodic"

	DefaultDiscoveryDialTimeout      = 2 * time.Second
	DefaultDiscoveryRequestTimeOut   = 5 * time.Second
	DefaultDiscoveryKeepAliveTime    = 2 * time.Second
	DefaultDiscoveryKeepAliveTimeOut = 6 * time.Second

	DefaultListenPeerURLs   = "http://localhost:2380"
	DefaultListenClientURLs = "http://localhost:2379"

	DefaultLogOutput = "default"
	JournalLogOutput = "systemd/journal"
	StdErrLogOutput  = "stderr"
	StdOutLogOutput  = "stdout"

	// DefaultLogRotationConfig is the default configuration used for log rotation.
	// Log rotation is disabled by default.
	// MaxSize    = 100 // MB
	// MaxAge     = 0 // days (no limit)
	// MaxBackups = 0 // no limit
	// LocalTime  = false // use computers local time, UTC by default
	// Compress   = false // compress the rotated log in gzip format
	DefaultLogRotationConfig = `{"maxsize": 100, "maxage": 0, "maxbackups": 0, "localtime": false, "compress": false}`

	// ExperimentalDistributedTracingAddress is the default collector address.
	ExperimentalDistributedTracingAddress = "localhost:4317"
	// ExperimentalDistributedTracingServiceName is the default etcd service name.
	ExperimentalDistributedTracingServiceName = "etcd"

	// DefaultStrictReconfigCheck is the default value for "--strict-reconfig-check" flag.
	// It's enabled by default.
	DefaultStrictReconfigCheck = true

	// maxElectionMs specifies the maximum value of election timeout.
	// More details are listed on etcd.io/docs > version > tuning/#time-parameters
	maxElectionMs = 50000
	// backend freelist map type
	freelistArrayType = "array"
)

var (
	ErrConflictBootstrapFlags = fmt.Errorf("multiple discovery or bootstrap flags are set. " +
		"Choose one of \"initial-cluster\", \"discovery\", \"discovery-endpoints\" or \"discovery-srv\"")
	ErrUnsetAdvertiseClientURLsFlag = fmt.Errorf("--advertise-client-urls is required when --listen-client-urls is set explicitly")
	ErrLogRotationInvalidLogOutput  = fmt.Errorf("--log-outputs requires a single file path when --log-rotate-config-json is defined")

	DefaultInitialAdvertisePeerURLs = "http://localhost:2380"
	DefaultAdvertiseClientURLs      = "http://localhost:2379"

	defaultHostname   string
	defaultHostStatus error

	// indirection for testing
	getCluster = srv.GetCluster
)

var (
	// CompactorModePeriodic is periodic compaction mode
	// for "Config.AutoCompactionMode" field.
	// If "AutoCompactionMode" is CompactorModePeriodic and
	// "AutoCompactionRetention" is "1h", it automatically compacts
	// compacts storage every hour.
	CompactorModePeriodic = v3compactor.ModePeriodic

	// CompactorModeRevision is revision-based compaction mode
	// for "Config.AutoCompactionMode" field.
	// If "AutoCompactionMode" is CompactorModeRevision and
	// "AutoCompactionRetention" is "1000", it compacts log on
	// revision 5000 when the current revision is 6000.
	// This runs every 5-minute if enough of logs have proceeded.
	CompactorModeRevision = v3compactor.ModeRevision
)

func init() {
	defaultHostname, defaultHostStatus = netutil.GetDefaultHost()
}

// Config holds the arguments for configuring an etcd server.
type Config struct {
	Name   string `json:"name"`
	Dir    string `json:"data-dir"`
	WalDir string `json:"wal-dir"`
	DBType string `json:"db-type"`

	SnapshotCount uint64 `json:"snapshot-count"`

	// SnapshotCatchUpEntries is the number of entries for a slow follower
	// to catch-up after compacting the raft storage entries.
	// We expect the follower has a millisecond level latency with the leader.
	// The max throughput is around 10K. Keep a 5K entries is enough for helping
	// follower to catch up.
	SnapshotCatchUpEntries uint64 `json:"experimental-snapshot-catch-up-entries"`

	MaxSnapFiles uint `json:"max-snapshots"`
	MaxWalFiles  uint `json:"max-wals"`

	// TickMs is the number of milliseconds between heartbeat ticks.
	// TODO: decouple tickMs and heartbeat tick (current heartbeat tick = 1).
	// make ticks a cluster wide configuration.
	TickMs     uint `json:"heartbeat-interval"`
	ElectionMs uint `json:"election-timeout"`

	// InitialElectionTickAdvance is true, then local member fast-forwards
	// election ticks to speed up "initial" leader election trigger. This
	// benefits the case of larger election ticks. For instance, cross
	// datacenter deployment may require longer election timeout of 10-second.
	// If true, local node does not need wait up to 10-second. Instead,
	// forwards its election ticks to 8-second, and have only 2-second left
	// before leader election.
	//
	// Major assumptions are that:
	//  - cluster has no active leader thus advancing ticks enables faster
	//    leader election, or
	//  - cluster already has an established leader, and rejoining follower
	//    is likely to receive heartbeats from the leader after tick advance
	//    and before election timeout.
	//
	// However, when network from leader to rejoining follower is congested,
	// and the follower does not receive leader heartbeat within left election
	// ticks, disruptive election has to happen thus affecting cluster
	// availabilities.
	//
	// Disabling this would slow down initial bootstrap process for cross
	// datacenter deployments. Make your own tradeoffs by configuring
	// --initial-election-tick-advance at the cost of slow initial bootstrap.
	//
	// If single-node, it advances ticks regardless.
	//
	// See https://github.com/etcd-io/etcd/issues/9333 for more detail.
	InitialElectionTickAdvance bool `json:"initial-election-tick-advance"`

	// BackendBatchInterval is the maximum time before commit the backend transaction.
	BackendBatchInterval time.Duration `json:"backend-batch-interval"`
	// BackendBatchLimit is the maximum operations before commit the backend transaction.
	BackendBatchLimit int `json:"backend-batch-limit"`
	// BackendFreelistType specifies the type of freelist that boltdb backend uses (array and map are supported types).
	BackendFreelistType string `json:"backend-bbolt-freelist-type"`
	QuotaBackendBytes   int64  `json:"quota-backend-bytes"`
	MaxTxnOps           uint   `json:"max-txn-ops"`
	MaxRequestBytes     uint   `json:"max-request-bytes"`

	// MaxConcurrentStreams specifies the maximum number of concurrent
	// streams that each client can open at a time.
	MaxConcurrentStreams uint32 `json:"max-concurrent-streams"`

	ListenPeerUrls, ListenClientUrls, ListenClientHttpUrls []url.URL
	AdvertisePeerUrls, AdvertiseClientUrls                 []url.URL
	ClientTLSInfo                                          transport.TLSInfo
	ClientAutoTLS                                          bool
	PeerTLSInfo                                            transport.TLSInfo
	PeerAutoTLS                                            bool
	// SelfSignedCertValidity specifies the validity period of the client and peer certificates
	// that are automatically generated by etcd when you specify ClientAutoTLS and PeerAutoTLS,
	// the unit is year, and the default is 1
	SelfSignedCertValidity uint `json:"self-signed-cert-validity"`

	// CipherSuites is a list of supported TLS cipher suites between
	// client/server and peers. If empty, Go auto-populates the list.
	// Note that cipher suites are prioritized in the given order.
	CipherSuites []string `json:"cipher-suites"`

	// TlsMinVersion is the minimum accepted TLS version between client/server and peers.
	TlsMinVersion string `json:"tls-min-version"`
	// TlsMaxVersion is the maximum accepted TLS version between client/server and peers.
	TlsMaxVersion string `json:"tls-max-version"`

	ClusterState          string `json:"initial-cluster-state"`
	DNSCluster            string `json:"discovery-srv"`
	DNSClusterServiceName string `json:"discovery-srv-name"`
	Dproxy                string `json:"discovery-proxy"`

	Durl         string                      `json:"discovery"`
	DiscoveryCfg v3discovery.DiscoveryConfig `json:"discovery-config"`

	InitialCluster                      string        `json:"initial-cluster"`
	InitialClusterToken                 string        `json:"initial-cluster-token"`
	StrictReconfigCheck                 bool          `json:"strict-reconfig-check"`
	ExperimentalWaitClusterReadyTimeout time.Duration `json:"wait-cluster-ready-timeout"`

	// AutoCompactionMode is either 'periodic' or 'revision'.
	AutoCompactionMode string `json:"auto-compaction-mode"`
	// AutoCompactionRetention is either duration string with time unit
	// (e.g. '5m' for 5-minute), or revision unit (e.g. '5000').
	// If no time unit is provided and compaction mode is 'periodic',
	// the unit defaults to hour. For example, '5' translates into 5-hour.
	AutoCompactionRetention string `json:"auto-compaction-retention"`

	// GRPCKeepAliveMinTime is the minimum interval that a client should
	// wait before pinging server. When client pings "too fast", server
	// sends goaway and closes the connection (errors: too_many_pings,
	// http2.ErrCodeEnhanceYourCalm). When too slow, nothing happens.
	// Server expects client pings only when there is any active streams
	// (PermitWithoutStream is set false).
	GRPCKeepAliveMinTime time.Duration `json:"grpc-keepalive-min-time"`
	// GRPCKeepAliveInterval is the frequency of server-to-client ping
	// to check if a connection is alive. Close a non-responsive connection
	// after an additional duration of Timeout. 0 to disable.
	GRPCKeepAliveInterval time.Duration `json:"grpc-keepalive-interval"`
	// GRPCKeepAliveTimeout is the additional duration of wait
	// before closing a non-responsive connection. 0 to disable.
	GRPCKeepAliveTimeout time.Duration `json:"grpc-keepalive-timeout"`

	// SocketOpts are socket options passed to listener config.
	SocketOpts transport.SocketOpts `json:"socket-options"`

	// PreVote is true to enable Raft Pre-Vote.
	// If enabled, Raft runs an additional election phase
	// to check whether it would get enough votes to win
	// an election, thus minimizing disruptions.
	PreVote bool `json:"pre-vote"`

	CORS map[string]struct{}

	// HostWhitelist lists acceptable hostnames from HTTP client requests.
	// Client origin policy protects against "DNS Rebinding" attacks
	// to insecure etcd servers. That is, any website can simply create
	// an authorized DNS name, and direct DNS to "localhost" (or any
	// other address). Then, all HTTP endpoints of etcd server listening
	// on "localhost" becomes accessible, thus vulnerable to DNS rebinding
	// attacks. See "CVE-2018-5702" for more detail.
	//
	// 1. If client connection is secure via HTTPS, allow any hostnames.
	// 2. If client connection is not secure and "HostWhitelist" is not empty,
	//    only allow HTTP requests whose Host field is listed in whitelist.
	//
	// Note that the client origin policy is enforced whether authentication
	// is enabled or not, for tighter controls.
	//
	// By default, "HostWhitelist" is "*", which allows any hostnames.
	// Note that when specifying hostnames, loopback addresses are not added
	// automatically. To allow loopback interfaces, leave it empty or set it "*",
	// or add them to whitelist manually (e.g. "localhost", "127.0.0.1", etc.).
	//
	// CVE-2018-5702 reference:
	// - https://bugs.chromium.org/p/project-zero/issues/detail?id=1447#c2
	// - https://github.com/transmission/transmission/pull/468
	// - https://github.com/etcd-io/etcd/issues/9353
	HostWhitelist map[string]struct{}

	// UserHandlers is for registering users handlers and only used for
	// embedding etcd into other applications.
	// The map key is the route path for the handler, and
	// you must ensure it can't be conflicted with etcd's.
	UserHandlers map[string]http.Handler `json:"-"`
	// ServiceRegister is for registering users' gRPC services. A simple usage example:
	//	cfg := embed.NewConfig()
	//	cfg.ServerRegister = func(s *grpc.Server) {
	//		pb.RegisterFooServer(s, &fooServer{})
	//		pb.RegisterBarServer(s, &barServer{})
	//	}
	//	embed.StartEtcd(cfg)
	ServiceRegister func(*grpc.Server) `json:"-"`

	AuthToken  string `json:"auth-token"`
	BcryptCost uint   `json:"bcrypt-cost"`

	// AuthTokenTTL in seconds of the simple token
	AuthTokenTTL uint `json:"auth-token-ttl"`

	ExperimentalInitialCorruptCheck     bool          `json:"experimental-initial-corrupt-check"`
	ExperimentalCorruptCheckTime        time.Duration `json:"experimental-corrupt-check-time"`
	ExperimentalCompactHashCheckEnabled bool          `json:"experimental-compact-hash-check-enabled"`
	ExperimentalCompactHashCheckTime    time.Duration `json:"experimental-compact-hash-check-time"`

	// ExperimentalEnableLeaseCheckpoint enables leader to send regular checkpoints to other members to prevent reset of remaining TTL on leader change.
	ExperimentalEnableLeaseCheckpoint bool `json:"experimental-enable-lease-checkpoint"`
	// ExperimentalEnableLeaseCheckpointPersist enables persisting remainingTTL to prevent indefinite auto-renewal of long lived leases. Always enabled in v3.6. Should be used to ensure smooth upgrade from v3.5 clusters with this feature enabled.
	// Requires experimental-enable-lease-checkpoint to be enabled.
	// Deprecated in v3.6.
	// TODO: Delete in v3.7
	ExperimentalEnableLeaseCheckpointPersist bool `json:"experimental-enable-lease-checkpoint-persist"`
	ExperimentalCompactionBatchLimit         int  `json:"experimental-compaction-batch-limit"`
	// ExperimentalCompactionSleepInterval is the sleep interval between every etcd compaction loop.
	ExperimentalCompactionSleepInterval     time.Duration `json:"experimental-compaction-sleep-interval"`
	ExperimentalWatchProgressNotifyInterval time.Duration `json:"experimental-watch-progress-notify-interval"`
	// ExperimentalWarningApplyDuration is the time duration after which a warning is generated if applying request
	// takes more time than this value.
	ExperimentalWarningApplyDuration time.Duration `json:"experimental-warning-apply-duration"`
	// ExperimentalBootstrapDefragThresholdMegabytes is the minimum number of megabytes needed to be freed for etcd server to
	// consider running defrag during bootstrap. Needs to be set to non-zero value to take effect.
	ExperimentalBootstrapDefragThresholdMegabytes uint `json:"experimental-bootstrap-defrag-threshold-megabytes"`
	// WarningUnaryRequestDuration is the time duration after which a warning is generated if applying
	// unary request takes more time than this value.
	WarningUnaryRequestDuration time.Duration `json:"warning-unary-request-duration"`
	// ExperimentalWarningUnaryRequestDuration is deprecated, please use WarningUnaryRequestDuration instead.
	ExperimentalWarningUnaryRequestDuration time.Duration `json:"experimental-warning-unary-request-duration"`
	// ExperimentalMaxLearners sets a limit to the number of learner members that can exist in the cluster membership.
	ExperimentalMaxLearners int `json:"experimental-max-learners"`

	// ForceNewCluster starts a new cluster even if previously started; unsafe.
	ForceNewCluster bool `json:"force-new-cluster"`

	EnablePprof           bool   `json:"enable-pprof"`
	Metrics               string `json:"metrics"`
	ListenMetricsUrls     []url.URL
	ListenMetricsUrlsJSON string `json:"listen-metrics-urls"`

	// ExperimentalEnableDistributedTracing indicates if experimental tracing using OpenTelemetry is enabled.
	ExperimentalEnableDistributedTracing bool `json:"experimental-enable-distributed-tracing"`
	// ExperimentalDistributedTracingAddress is the address of the OpenTelemetry Collector.
	// Can only be set if ExperimentalEnableDistributedTracing is true.
	ExperimentalDistributedTracingAddress string `json:"experimental-distributed-tracing-address"`
	// ExperimentalDistributedTracingServiceName is the name of the service.
	// Can only be used if ExperimentalEnableDistributedTracing is true.
	ExperimentalDistributedTracingServiceName string `json:"experimental-distributed-tracing-service-name"`
	// ExperimentalDistributedTracingServiceInstanceID is the ID key of the service.
	// This ID must be unique, as helps to distinguish instances of the same service
	// that exist at the same time.
	// Can only be used if ExperimentalEnableDistributedTracing is true.
	ExperimentalDistributedTracingServiceInstanceID string `json:"experimental-distributed-tracing-instance-id"`
	// ExperimentalDistributedTracingSamplingRatePerMillion is the number of samples to collect per million spans.
	// Defaults to 0.
	ExperimentalDistributedTracingSamplingRatePerMillion int `json:"experimental-distributed-tracing-sampling-rate"`

	// Logger is logger options: currently only supports "zap".
	// "capnslog" is removed in v3.5.
	Logger string `json:"logger"`
	// LogLevel configures log level. Only supports debug, info, warn, error, panic, or fatal. Default 'info'.
	LogLevel string `json:"log-level"`
	// LogFormat set log encoding. Only supports json, console. Default is 'json'.
	LogFormat string `json:"log-format"`
	// LogOutputs is either:
	//  - "default" as os.Stderr,
	//  - "stderr" as os.Stderr,
	//  - "stdout" as os.Stdout,
	//  - file path to append server logs to.
	// It can be multiple when "Logger" is zap.
	LogOutputs []string `json:"log-outputs"`
	// EnableLogRotation enables log rotation of a single LogOutputs file target.
	EnableLogRotation bool `json:"enable-log-rotation"`
	// LogRotationConfigJSON is a passthrough allowing a log rotation JSON config to be passed directly.
	LogRotationConfigJSON string `json:"log-rotation-config-json"`
	// ZapLoggerBuilder is used to build the zap logger.
	ZapLoggerBuilder func(*Config) error

	// logger logs server-side operations. The default is nil,
	// and "setupLogging" must be called before starting server.
	// Do not set logger directly.
	loggerMu *sync.RWMutex
	logger   *zap.Logger
	// EnableGRPCGateway enables grpc gateway.
	// The gateway translates a RESTful HTTP API into gRPC.
	EnableGRPCGateway bool `json:"enable-grpc-gateway"`

	// UnsafeNoFsync disables all uses of fsync.
	// Setting this is unsafe and will cause data loss.
	UnsafeNoFsync bool `json:"unsafe-no-fsync"`

	ExperimentalDowngradeCheckTime time.Duration `json:"experimental-downgrade-check-time"`

	// ExperimentalMemoryMlock enables mlocking of etcd owned memory pages.
	// The setting improves etcd tail latency in environments were:
	//   - memory pressure might lead to swapping pages to disk
	//   - disk latency might be unstable
	// Currently all etcd memory gets mlocked, but in future the flag can
	// be refined to mlock in-use area of bbolt only.
	ExperimentalMemoryMlock bool `json:"experimental-memory-mlock"`

	// ExperimentalTxnModeWriteWithSharedBuffer enables write transaction to use a shared buffer in its readonly check operations.
	ExperimentalTxnModeWriteWithSharedBuffer bool `json:"experimental-txn-mode-write-with-shared-buffer"`

	// V2Deprecation describes phase of API & Storage V2 support
	V2Deprecation config.V2DeprecationEnum `json:"v2-deprecation"`
}

// configYAML holds the config suitable for yaml parsing
type configYAML struct {
	Config
	configJSON
}

// configJSON has file options that are translated into Config options
type configJSON struct {
	ListenPeerUrls       string `json:"listen-peer-urls"`
	ListenClientUrls     string `json:"listen-client-urls"`
	ListenClientHttpUrls string `json:"listen-client-http-urls"`
	AdvertisePeerUrls    string `json:"initial-advertise-peer-urls"`
	AdvertiseClientUrls  string `json:"advertise-client-urls"`

	CORSJSON          string `json:"cors"`
	HostWhitelistJSON string `json:"host-whitelist"`

	ClientSecurityJSON securityConfig `json:"client-transport-security"`
	PeerSecurityJSON   securityConfig `json:"peer-transport-security"`
}

type securityConfig struct {
	CertFile       string `json:"cert-file"`
	KeyFile        string `json:"key-file"`
	ClientCertFile string `json:"client-cert-file"`
	ClientKeyFile  string `json:"client-key-file"`
	CertAuth       bool   `json:"client-cert-auth"`
	TrustedCAFile  string `json:"trusted-ca-file"`
	AutoTLS        bool   `json:"auto-tls"`
}

func NewBadgerConfig() *Config {
	c := NewConfig()
	c.DBType = string(backend.BadgerDB)
	return c
}
func NewBoltConfig() *Config {
	c := NewConfig()
	c.DBType = string(backend.BoltDB)
	return c
}

// NewConfig creates a new Config populated with default values.
func NewConfig() *Config {
	lpurl, _ := url.Parse(DefaultListenPeerURLs)
	apurl, _ := url.Parse(DefaultInitialAdvertisePeerURLs)
	lcurl, _ := url.Parse(DefaultListenClientURLs)
	acurl, _ := url.Parse(DefaultAdvertiseClientURLs)
	cfg := &Config{
		MaxSnapFiles: DefaultMaxSnapshots,
		MaxWalFiles:  DefaultMaxWALs,
		DBType:       string(backend.SQLite),

		Name: DefaultName,

		SnapshotCount:          etcdserver.DefaultSnapshotCount,
		SnapshotCatchUpEntries: etcdserver.DefaultSnapshotCatchUpEntries,

		MaxTxnOps:                        DefaultMaxTxnOps,
		MaxRequestBytes:                  DefaultMaxRequestBytes,
		MaxConcurrentStreams:             DefaultMaxConcurrentStreams,
		ExperimentalWarningApplyDuration: DefaultWarningApplyDuration,

		GRPCKeepAliveMinTime:  DefaultGRPCKeepAliveMinTime,
		GRPCKeepAliveInterval: DefaultGRPCKeepAliveInterval,
		GRPCKeepAliveTimeout:  DefaultGRPCKeepAliveTimeout,

		SocketOpts: transport.SocketOpts{
			ReusePort:    false,
			ReuseAddress: false,
		},

		TickMs:                     100,
		ElectionMs:                 1000,
		InitialElectionTickAdvance: true,

		ListenPeerUrls:      []url.URL{*lpurl},
		ListenClientUrls:    []url.URL{*lcurl},
		AdvertisePeerUrls:   []url.URL{*apurl},
		AdvertiseClientUrls: []url.URL{*acurl},

		ClusterState:                        ClusterStateFlagNew,
		InitialClusterToken:                 "etcd-cluster",
		ExperimentalWaitClusterReadyTimeout: DefaultWaitClusterReadyTimeout,

		StrictReconfigCheck: DefaultStrictReconfigCheck,
		Metrics:             "basic",

		CORS:          map[string]struct{}{"*": {}},
		HostWhitelist: map[string]struct{}{"*": {}},

		AuthToken:    "simple",
		BcryptCost:   uint(bcrypt.DefaultCost),
		AuthTokenTTL: 300,

		PreVote: true,

		loggerMu:              new(sync.RWMutex),
		logger:                nil,
		Logger:                "zap",
		LogOutputs:            []string{DefaultLogOutput},
		LogLevel:              logutil.DefaultLogLevel,
		EnableLogRotation:     false,
		LogRotationConfigJSON: DefaultLogRotationConfig,
		EnableGRPCGateway:     true,

		ExperimentalDowngradeCheckTime:           DefaultDowngradeCheckTime,
		ExperimentalMemoryMlock:                  false,
		ExperimentalTxnModeWriteWithSharedBuffer: true,
		ExperimentalMaxLearners:                  membership.DefaultMaxLearners,

		ExperimentalCompactHashCheckEnabled: false,
		ExperimentalCompactHashCheckTime:    time.Minute,

		V2Deprecation: config.V2_DEPR_DEFAULT,

		DiscoveryCfg: v3discovery.DiscoveryConfig{
			ConfigSpec: clientv3.ConfigSpec{
				DialTimeout:      DefaultDiscoveryDialTimeout,
				RequestTimeout:   DefaultDiscoveryRequestTimeOut,
				KeepAliveTime:    DefaultDiscoveryKeepAliveTime,
				KeepAliveTimeout: DefaultDiscoveryKeepAliveTimeOut,

				Secure: &clientv3.SecureConfig{},
				Auth:   &clientv3.AuthConfig{},
			},
		},

		AutoCompactionMode: DefaultAutoCompactionMode,
	}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	return cfg
}

func ConfigFromFile(path string) (*Config, error) {
	cfg := &configYAML{Config: *NewConfig()}
	if err := cfg.configFromFile(path); err != nil {
		return nil, err
	}
	return &cfg.Config, nil
}

func (cfg *configYAML) configFromFile(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	defaultInitialCluster := cfg.InitialCluster

	err = yaml.Unmarshal(b, cfg)
	if err != nil {
		return err
	}

	if cfg.configJSON.ListenPeerUrls != "" {
		u, err := types.NewURLs(strings.Split(cfg.configJSON.ListenPeerUrls, ","))
		if err != nil {
			fmt.Fprintf(os.Stderr, "unexpected error setting up listen-peer-urls: %v\n", err)
			os.Exit(1)
		}
		cfg.Config.ListenPeerUrls = u
	}

	if cfg.configJSON.ListenClientUrls != "" {
		u, err := types.NewURLs(strings.Split(cfg.configJSON.ListenClientUrls, ","))
		if err != nil {
			fmt.Fprintf(os.Stderr, "unexpected error setting up listen-client-urls: %v\n", err)
			os.Exit(1)
		}
		cfg.Config.ListenClientUrls = u
	}

	if cfg.configJSON.ListenClientHttpUrls != "" {
		u, err := types.NewURLs(strings.Split(cfg.configJSON.ListenClientHttpUrls, ","))
		if err != nil {
			fmt.Fprintf(os.Stderr, "unexpected error setting up listen-client-http-urls: %v\n", err)
			os.Exit(1)
		}
		cfg.Config.ListenClientHttpUrls = u
	}

	if cfg.configJSON.AdvertisePeerUrls != "" {
		u, err := types.NewURLs(strings.Split(cfg.configJSON.AdvertisePeerUrls, ","))
		if err != nil {
			fmt.Fprintf(os.Stderr, "unexpected error setting up initial-advertise-peer-urls: %v\n", err)
			os.Exit(1)
		}
		cfg.Config.AdvertisePeerUrls = u
	}

	if cfg.configJSON.AdvertiseClientUrls != "" {
		u, err := types.NewURLs(strings.Split(cfg.configJSON.AdvertiseClientUrls, ","))
		if err != nil {
			fmt.Fprintf(os.Stderr, "unexpected error setting up advertise-peer-urls: %v\n", err)
			os.Exit(1)
		}
		cfg.Config.AdvertiseClientUrls = u
	}

	if cfg.ListenMetricsUrlsJSON != "" {
		u, err := types.NewURLs(strings.Split(cfg.ListenMetricsUrlsJSON, ","))
		if err != nil {
			fmt.Fprintf(os.Stderr, "unexpected error setting up listen-metrics-urls: %v\n", err)
			os.Exit(1)
		}
		cfg.ListenMetricsUrls = u
	}

	if cfg.CORSJSON != "" {
		uv := flags.NewUniqueURLsWithExceptions(cfg.CORSJSON, "*")
		cfg.CORS = uv.Values
	}

	if cfg.HostWhitelistJSON != "" {
		uv := flags.NewUniqueStringsValue(cfg.HostWhitelistJSON)
		cfg.HostWhitelist = uv.Values
	}

	// If a discovery or discovery-endpoints flag is set, clear default initial cluster set by InitialClusterFromName
	if (cfg.Durl != "" || cfg.DNSCluster != "" || len(cfg.DiscoveryCfg.Endpoints) > 0) && cfg.InitialCluster == defaultInitialCluster {
		cfg.InitialCluster = ""
	}
	if cfg.ClusterState == "" {
		cfg.ClusterState = ClusterStateFlagNew
	}

	copySecurityDetails := func(tls *transport.TLSInfo, ysc *securityConfig) {
		tls.CertFile = ysc.CertFile
		tls.KeyFile = ysc.KeyFile
		tls.ClientCertFile = ysc.ClientCertFile
		tls.ClientKeyFile = ysc.ClientKeyFile
		tls.ClientCertAuth = ysc.CertAuth
		tls.TrustedCAFile = ysc.TrustedCAFile
	}
	copySecurityDetails(&cfg.ClientTLSInfo, &cfg.ClientSecurityJSON)
	copySecurityDetails(&cfg.PeerTLSInfo, &cfg.PeerSecurityJSON)
	cfg.ClientAutoTLS = cfg.ClientSecurityJSON.AutoTLS
	cfg.PeerAutoTLS = cfg.PeerSecurityJSON.AutoTLS
	if cfg.SelfSignedCertValidity == 0 {
		cfg.SelfSignedCertValidity = 1
	}
	return cfg.Validate()
}

func updateCipherSuites(tls *transport.TLSInfo, ss []string) error {
	if len(tls.CipherSuites) > 0 && len(ss) > 0 {
		return fmt.Errorf("TLSInfo.CipherSuites is already specified (given %v)", ss)
	}
	if len(ss) > 0 {
		cs, err := tlsutil.GetCipherSuites(ss)
		if err != nil {
			return err
		}
		tls.CipherSuites = cs
	}
	return nil
}

func updateMinMaxVersions(info *transport.TLSInfo, min, max string) {
	// Validate() has been called to check the user input, so it should never fail.
	var err error
	if info.MinVersion, err = tlsutil.GetTLSVersion(min); err != nil {
		panic(err)
	}
	if info.MaxVersion, err = tlsutil.GetTLSVersion(max); err != nil {
		panic(err)
	}
}

// Validate ensures that '*embed.Config' fields are properly configured.
func (cfg *Config) Validate() error {
	if err := cfg.setupLogging(); err != nil {
		return err
	}
	if err := checkBindURLs(cfg.ListenPeerUrls); err != nil {
		return err
	}
	if err := checkBindURLs(cfg.ListenClientUrls); err != nil {
		return err
	}
	if err := checkBindURLs(cfg.ListenClientHttpUrls); err != nil {
		return err
	}
	if len(cfg.ListenClientHttpUrls) == 0 {
		cfg.logger.Warn("Running http and grpc server on single port. This is not recommended for production.")
	}
	if err := checkBindURLs(cfg.ListenMetricsUrls); err != nil {
		return err
	}
	if err := checkHostURLs(cfg.AdvertisePeerUrls); err != nil {
		addrs := cfg.getAdvertisePeerUrls()
		return fmt.Errorf(`--initial-advertise-peer-urls %q must be "host:port" (%v)`, strings.Join(addrs, ","), err)
	}
	if err := checkHostURLs(cfg.AdvertiseClientUrls); err != nil {
		addrs := cfg.getAdvertiseClientUrls()
		return fmt.Errorf(`--advertise-client-urls %q must be "host:port" (%v)`, strings.Join(addrs, ","), err)
	}
	// Check if conflicting flags are passed.
	nSet := 0
	for _, v := range []bool{cfg.Durl != "", cfg.InitialCluster != "", cfg.DNSCluster != "", len(cfg.DiscoveryCfg.Endpoints) > 0} {
		if v {
			nSet++
		}
	}

	if cfg.ClusterState != ClusterStateFlagNew && cfg.ClusterState != ClusterStateFlagExisting {
		return fmt.Errorf("unexpected clusterState %q", cfg.ClusterState)
	}

	if nSet > 1 {
		return ErrConflictBootstrapFlags
	}

	// Check if both v2 discovery and v3 discovery flags are passed.
	v2discoveryFlagsExist := cfg.Dproxy != ""
	v3discoveryFlagsExist := len(cfg.DiscoveryCfg.Endpoints) > 0 ||
		cfg.DiscoveryCfg.Token != "" ||
		cfg.DiscoveryCfg.Secure.Cert != "" ||
		cfg.DiscoveryCfg.Secure.Key != "" ||
		cfg.DiscoveryCfg.Secure.Cacert != "" ||
		cfg.DiscoveryCfg.Auth.Username != "" ||
		cfg.DiscoveryCfg.Auth.Password != ""

	if v2discoveryFlagsExist && v3discoveryFlagsExist {
		return errors.New("both v2 discovery settings (discovery, discovery-proxy) " +
			"and v3 discovery settings (discovery-token, discovery-endpoints, discovery-cert, " +
			"discovery-key, discovery-cacert, discovery-user, discovery-password) are set")
	}

	// If one of `discovery-token` and `discovery-endpoints` is provided,
	// then the other one must be provided as well.
	if (cfg.DiscoveryCfg.Token != "") != (len(cfg.DiscoveryCfg.Endpoints) > 0) {
		return errors.New("both --discovery-token and --discovery-endpoints must be set")
	}

	if cfg.TickMs == 0 {
		return fmt.Errorf("--heartbeat-interval must be >0 (set to %dms)", cfg.TickMs)
	}
	if cfg.ElectionMs == 0 {
		return fmt.Errorf("--election-timeout must be >0 (set to %dms)", cfg.ElectionMs)
	}
	if 5*cfg.TickMs > cfg.ElectionMs {
		return fmt.Errorf("--election-timeout[%vms] should be at least as 5 times as --heartbeat-interval[%vms]", cfg.ElectionMs, cfg.TickMs)
	}
	if cfg.ElectionMs > maxElectionMs {
		return fmt.Errorf("--election-timeout[%vms] is too long, and should be set less than %vms", cfg.ElectionMs, maxElectionMs)
	}

	// check this last since proxying in etcdmain may make this OK
	if cfg.ListenClientUrls != nil && cfg.AdvertiseClientUrls == nil {
		return ErrUnsetAdvertiseClientURLsFlag
	}

	switch cfg.AutoCompactionMode {
	case CompactorModeRevision, CompactorModePeriodic:
	case "":
		return errors.New("undefined auto-compaction-mode")
	default:
		return fmt.Errorf("unknown auto-compaction-mode %q", cfg.AutoCompactionMode)
	}

	// Validate distributed tracing configuration but only if enabled.
	if cfg.ExperimentalEnableDistributedTracing {
		if err := validateTracingConfig(cfg.ExperimentalDistributedTracingSamplingRatePerMillion); err != nil {
			return fmt.Errorf("distributed tracing configurition is not valid: (%v)", err)
		}
	}

	if !cfg.ExperimentalEnableLeaseCheckpointPersist && cfg.ExperimentalEnableLeaseCheckpoint {
		cfg.logger.Warn("Detected that checkpointing is enabled without persistence. Consider enabling experimental-enable-lease-checkpoint-persist")
	}

	if cfg.ExperimentalEnableLeaseCheckpointPersist && !cfg.ExperimentalEnableLeaseCheckpoint {
		return fmt.Errorf("setting experimental-enable-lease-checkpoint-persist requires experimental-enable-lease-checkpoint")
	}

	if cfg.ExperimentalCompactHashCheckTime <= 0 {
		return fmt.Errorf("--experimental-compact-hash-check-time must be >0 (set to %v)", cfg.ExperimentalCompactHashCheckTime)
	}

	// If `--name` isn't configured, then multiple members may have the same "default" name.
	// When adding a new member with the "default" name as well, etcd may regards its peerURL
	// as one additional peerURL of the existing member which has the same "default" name,
	// because each member can have multiple client or peer URLs.
	// Please refer to https://github.com/etcd-io/etcd/issues/13757
	if cfg.Name == DefaultName {
		cfg.logger.Warn(
			"it isn't recommended to use default name, please set a value for --name. "+
				"Note that etcd might run into issue when multiple members have the same default name",
			zap.String("name", cfg.Name))
	}

	minVersion, err := tlsutil.GetTLSVersion(cfg.TlsMinVersion)
	if err != nil {
		return err
	}
	maxVersion, err := tlsutil.GetTLSVersion(cfg.TlsMaxVersion)
	if err != nil {
		return err
	}

	// maxVersion == 0 means that Go selects the highest available version.
	if maxVersion != 0 && minVersion > maxVersion {
		return fmt.Errorf("min version (%s) is greater than max version (%s)", cfg.TlsMinVersion, cfg.TlsMaxVersion)
	}

	// Check if user attempted to configure ciphers for TLS1.3 only: Go does not support that currently.
	if minVersion == tls.VersionTLS13 && len(cfg.CipherSuites) > 0 {
		return fmt.Errorf("cipher suites cannot be configured when only TLS1.3 is enabled")
	}

	return nil
}

// PeerURLsMapAndToken sets up an initial peer URLsMap and cluster token for bootstrap or discovery.
func (cfg *Config) PeerURLsMapAndToken(which string) (urlsmap types.URLsMap, token string, err error) {
	token = cfg.InitialClusterToken
	switch {
	case cfg.Durl != "":
		urlsmap = types.URLsMap{}
		// If using v2 discovery, generate a temporary cluster based on
		// self's advertised peer URLs
		urlsmap[cfg.Name] = cfg.AdvertisePeerUrls
		token = cfg.Durl

	case len(cfg.DiscoveryCfg.Endpoints) > 0:
		urlsmap = types.URLsMap{}
		// If using v3 discovery, generate a temporary cluster based on
		// self's advertised peer URLs
		urlsmap[cfg.Name] = cfg.AdvertisePeerUrls
		token = cfg.DiscoveryCfg.Token

	case cfg.DNSCluster != "":
		clusterStrs, cerr := cfg.GetDNSClusterNames()
		lg := cfg.logger
		if cerr != nil {
			lg.Warn("failed to resolve during SRV discovery", zap.Error(cerr))
		}
		if len(clusterStrs) == 0 {
			return nil, "", cerr
		}
		for _, s := range clusterStrs {
			lg.Info("got bootstrap from DNS for etcd-server", zap.String("node", s))
		}
		clusterStr := strings.Join(clusterStrs, ",")
		if strings.Contains(clusterStr, "https://") && cfg.PeerTLSInfo.TrustedCAFile == "" {
			cfg.PeerTLSInfo.ServerName = cfg.DNSCluster
		}
		urlsmap, err = types.NewURLsMap(clusterStr)
		// only etcd member must belong to the discovered cluster.
		// proxy does not need to belong to the discovered cluster.
		if which == "etcd" {
			if _, ok := urlsmap[cfg.Name]; !ok {
				return nil, "", fmt.Errorf("cannot find local etcd member %q in SRV records", cfg.Name)
			}
		}

	default:
		// We're statically configured, and cluster has appropriately been set.
		urlsmap, err = types.NewURLsMap(cfg.InitialCluster)
	}
	return urlsmap, token, err
}

// GetDNSClusterNames uses DNS SRV records to get a list of initial nodes for cluster bootstrapping.
// This function will return a list of one or more nodes, as well as any errors encountered while
// performing service discovery.
// Note: Because this checks multiple sets of SRV records, discovery should only be considered to have
// failed if the returned node list is empty.
func (cfg *Config) GetDNSClusterNames() ([]string, error) {
	var (
		clusterStrs       []string
		cerr              error
		serviceNameSuffix string
	)
	if cfg.DNSClusterServiceName != "" {
		serviceNameSuffix = "-" + cfg.DNSClusterServiceName
	}

	lg := cfg.GetLogger()

	// Use both etcd-server-ssl and etcd-server for discovery.
	// Combine the results if both are available.
	clusterStrs, cerr = getCluster("https", "etcd-server-ssl"+serviceNameSuffix, cfg.Name, cfg.DNSCluster, cfg.AdvertisePeerUrls)
	if cerr != nil {
		clusterStrs = make([]string, 0)
	}
	lg.Info(
		"get cluster for etcd-server-ssl SRV",
		zap.String("service-scheme", "https"),
		zap.String("service-name", "etcd-server-ssl"+serviceNameSuffix),
		zap.String("server-name", cfg.Name),
		zap.String("discovery-srv", cfg.DNSCluster),
		zap.Strings("advertise-peer-urls", cfg.getAdvertisePeerUrls()),
		zap.Strings("found-cluster", clusterStrs),
		zap.Error(cerr),
	)

	defaultHTTPClusterStrs, httpCerr := getCluster("http", "etcd-server"+serviceNameSuffix, cfg.Name, cfg.DNSCluster, cfg.AdvertisePeerUrls)
	if httpCerr == nil {
		clusterStrs = append(clusterStrs, defaultHTTPClusterStrs...)
	}
	lg.Info(
		"get cluster for etcd-server SRV",
		zap.String("service-scheme", "http"),
		zap.String("service-name", "etcd-server"+serviceNameSuffix),
		zap.String("server-name", cfg.Name),
		zap.String("discovery-srv", cfg.DNSCluster),
		zap.Strings("advertise-peer-urls", cfg.getAdvertisePeerUrls()),
		zap.Strings("found-cluster", clusterStrs),
		zap.Error(httpCerr),
	)

	return clusterStrs, multierr.Combine(cerr, httpCerr)
}

func (cfg Config) InitialClusterFromName(name string) (ret string) {
	if len(cfg.AdvertisePeerUrls) == 0 {
		return ""
	}
	n := name
	if name == "" {
		n = DefaultName
	}
	for i := range cfg.AdvertisePeerUrls {
		ret = ret + "," + n + "=" + cfg.AdvertisePeerUrls[i].String()
	}
	return ret[1:]
}

func (cfg Config) IsNewCluster() bool { return cfg.ClusterState == ClusterStateFlagNew }
func (cfg Config) ElectionTicks() int { return int(cfg.ElectionMs / cfg.TickMs) }

func (cfg Config) V2DeprecationEffective() config.V2DeprecationEnum {
	if cfg.V2Deprecation == "" {
		return config.V2_DEPR_DEFAULT
	}
	return cfg.V2Deprecation
}

func (cfg Config) defaultPeerHost() bool {
	return len(cfg.AdvertisePeerUrls) == 1 && cfg.AdvertisePeerUrls[0].String() == DefaultInitialAdvertisePeerURLs
}

func (cfg Config) defaultClientHost() bool {
	return len(cfg.AdvertiseClientUrls) == 1 && cfg.AdvertiseClientUrls[0].String() == DefaultAdvertiseClientURLs
}

func (cfg *Config) ClientSelfCert() (err error) {
	if !cfg.ClientAutoTLS {
		return nil
	}
	if !cfg.ClientTLSInfo.Empty() {
		cfg.logger.Warn("ignoring client auto TLS since certs given")
		return nil
	}
	chosts := make([]string, 0, len(cfg.ListenClientUrls)+len(cfg.ListenClientHttpUrls))
	for _, u := range cfg.ListenClientUrls {
		chosts = append(chosts, u.Host)
	}
	for _, u := range cfg.ListenClientHttpUrls {
		chosts = append(chosts, u.Host)
	}
	cfg.ClientTLSInfo, err = transport.SelfCert(cfg.logger, filepath.Join(cfg.Dir, "fixtures", "client"), chosts, cfg.SelfSignedCertValidity)
	if err != nil {
		return err
	}
	return updateCipherSuites(&cfg.ClientTLSInfo, cfg.CipherSuites)
}

func (cfg *Config) PeerSelfCert() (err error) {
	if !cfg.PeerAutoTLS {
		return nil
	}
	if !cfg.PeerTLSInfo.Empty() {
		cfg.logger.Warn("ignoring peer auto TLS since certs given")
		return nil
	}
	phosts := make([]string, len(cfg.ListenPeerUrls))
	for i, u := range cfg.ListenPeerUrls {
		phosts[i] = u.Host
	}
	cfg.PeerTLSInfo, err = transport.SelfCert(cfg.logger, filepath.Join(cfg.Dir, "fixtures", "peer"), phosts, cfg.SelfSignedCertValidity)
	if err != nil {
		return err
	}
	return updateCipherSuites(&cfg.PeerTLSInfo, cfg.CipherSuites)
}

// UpdateDefaultClusterFromName updates cluster advertise URLs with, if available, default host,
// if advertise URLs are default values(localhost:2379,2380) AND if listen URL is 0.0.0.0.
// e.g. advertise peer URL localhost:2380 or listen peer URL 0.0.0.0:2380
// then the advertise peer host would be updated with machine's default host,
// while keeping the listen URL's port.
// User can work around this by explicitly setting URL with 127.0.0.1.
// It returns the default hostname, if used, and the error, if any, from getting the machine's default host.
// TODO: check whether fields are set instead of whether fields have default value
func (cfg *Config) UpdateDefaultClusterFromName(defaultInitialCluster string) (string, error) {
	if defaultHostname == "" || defaultHostStatus != nil {
		// update 'initial-cluster' when only the name is specified (e.g. 'etcd --name=abc')
		if cfg.Name != DefaultName && cfg.InitialCluster == defaultInitialCluster {
			cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
		}
		return "", defaultHostStatus
	}

	used := false
	pip, pport := cfg.ListenPeerUrls[0].Hostname(), cfg.ListenPeerUrls[0].Port()
	if cfg.defaultPeerHost() && pip == "0.0.0.0" {
		cfg.AdvertisePeerUrls[0] = url.URL{Scheme: cfg.AdvertisePeerUrls[0].Scheme, Host: fmt.Sprintf("%s:%s", defaultHostname, pport)}
		used = true
	}
	// update 'initial-cluster' when only the name is specified (e.g. 'etcd --name=abc')
	if cfg.Name != DefaultName && cfg.InitialCluster == defaultInitialCluster {
		cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	}

	cip, cport := cfg.ListenClientUrls[0].Hostname(), cfg.ListenClientUrls[0].Port()
	if cfg.defaultClientHost() && cip == "0.0.0.0" {
		cfg.AdvertiseClientUrls[0] = url.URL{Scheme: cfg.AdvertiseClientUrls[0].Scheme, Host: fmt.Sprintf("%s:%s", defaultHostname, cport)}
		used = true
	}
	dhost := defaultHostname
	if !used {
		dhost = ""
	}
	return dhost, defaultHostStatus
}

// checkBindURLs returns an error if any URL uses a domain name.
func checkBindURLs(urls []url.URL) error {
	for _, url := range urls {
		if url.Scheme == "unix" || url.Scheme == "unixs" {
			continue
		}
		host, _, err := net.SplitHostPort(url.Host)
		if err != nil {
			return err
		}
		if host == "localhost" {
			// special case for local address
			// TODO: support /etc/hosts ?
			continue
		}
		if net.ParseIP(host) == nil {
			return fmt.Errorf("expected IP in URL for binding (%s)", url.String())
		}
	}
	return nil
}

func checkHostURLs(urls []url.URL) error {
	for _, url := range urls {
		host, _, err := net.SplitHostPort(url.Host)
		if err != nil {
			return err
		}
		if host == "" {
			return fmt.Errorf("unexpected empty host (%s)", url.String())
		}
	}
	return nil
}

func (cfg *Config) getAdvertisePeerUrls() (ss []string) {
	ss = make([]string, len(cfg.AdvertisePeerUrls))
	for i := range cfg.AdvertisePeerUrls {
		ss[i] = cfg.AdvertisePeerUrls[i].String()
	}
	return ss
}

func (cfg *Config) getListenPeerUrls() (ss []string) {
	ss = make([]string, len(cfg.ListenPeerUrls))
	for i := range cfg.ListenPeerUrls {
		ss[i] = cfg.ListenPeerUrls[i].String()
	}
	return ss
}

func (cfg *Config) getAdvertiseClientUrls() (ss []string) {
	ss = make([]string, len(cfg.AdvertiseClientUrls))
	for i := range cfg.AdvertiseClientUrls {
		ss[i] = cfg.AdvertiseClientUrls[i].String()
	}
	return ss
}

func (cfg *Config) getListenClientUrls() (ss []string) {
	ss = make([]string, len(cfg.ListenClientUrls))
	for i := range cfg.ListenClientUrls {
		ss[i] = cfg.ListenClientUrls[i].String()
	}
	return ss
}

func (cfg *Config) getListenClientHttpUrls() (ss []string) {
	ss = make([]string, len(cfg.ListenClientHttpUrls))
	for i := range cfg.ListenClientHttpUrls {
		ss[i] = cfg.ListenClientHttpUrls[i].String()
	}
	return ss
}

func (cfg *Config) getMetricsURLs() (ss []string) {
	ss = make([]string, len(cfg.ListenMetricsUrls))
	for i := range cfg.ListenMetricsUrls {
		ss[i] = cfg.ListenMetricsUrls[i].String()
	}
	return ss
}

func parseBackendFreelistType(freelistType string) bolt.FreelistType {
	if freelistType == freelistArrayType {
		return bolt.FreelistArrayType
	}

	return bolt.FreelistMapType
}
