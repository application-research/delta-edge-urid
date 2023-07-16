package config

import (
	"github.com/caarlos0/env/v6"
	logging "github.com/ipfs/go-log/v2"
	"github.com/joho/godotenv"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

var (
	log                       = logging.Logger("config")
	defaultTestBootstrapPeers []multiaddr.Multiaddr
)

type EdgeConfig struct {
	Node struct {
		Name                  string `env:"NODE_NAME" envDefault:"edge-urid"`
		Description           string `env:"NODE_DESCRIPTION"`
		Type                  string `env:"NODE_TYPE"`
		DbDsn                 string `env:"DB_DSN" envDefault:"edge-urid.db"`
		Repo                  string `env:"REPO" envDefault:"./whypfs"`
		DsRepo                string `env:"DS_REPO" envDefault:"./whypfs-ds"`
		Port                  int    `env:"PORT" envDefault:"1414"`
		AdminApiKey           string `env:"ADMIN_API_KEY" envDefault:"admin"`
		DefaultCollectionName string `env:"DEFAULT_COLLECTION_NAME" envDefault:"default"`
	}

	Common struct {
		BucketAggregateSize        int64 `env:"BUCKET_AGGREGATE_SIZE" envDefault:"4544576000"`
		MaxSizeToSplit             int64 `env:"MAX_SIZE_TO_SPLIT" envDefault:"32000000000"`
		SplitSize                  int64 `env:"SPLIT_SIZE" envDefault:"5048576000"`
		DealCheck                  int   `env:"DEAL_CHECK" envDefault:"600"`
		ReplicationFactor          int   `env:"REPLICATION_FACTOR" envDefault:"0"`
		CapacityLimitPerKeyInBytes int64 `env:"CAPACITY_LIMIT_PER_KEY_IN_BYTES" envDefault:"0"`
	}

	ExternalApi struct {
		AuthSvcUrl  string `env:"AUTH_SVC_API" envDefault:"https://auth.estuary.tech"`
		DeltaSvcUrl string `env:"DELTA_SVC_API" envDefault:"https://delta.estuary.tech"`
	}
}

func InitConfig() EdgeConfig {
	godotenv.Load() // load from environment OR .env file if it exists
	var cfg EdgeConfig

	if err := env.Parse(&cfg); err != nil {
		log.Fatal("error parsing config: %+v\n", err)
	}

	log.Debug("config parsed successfully")

	return cfg
}

// BootstrapEstuaryPeers Creating a list of multiaddresses that are used to bootstrap the network.
func BootstrapEstuaryPeers() []peer.AddrInfo {

	for _, s := range []string{
		"/ip4/145.40.90.135/tcp/6746/p2p/12D3KooWNTiHg8eQsTRx8XV7TiJbq3379EgwG6Mo3V3MdwAfThsx",
		"/ip4/139.178.68.217/tcp/6744/p2p/12D3KooWCVXs8P7iq6ao4XhfAmKWrEeuKFWCJgqe9jGDMTqHYBjw",
		"/ip4/147.75.49.71/tcp/6745/p2p/12D3KooWGBWx9gyUFTVQcKMTenQMSyE2ad9m7c9fpjS4NMjoDien",
		"/ip4/147.75.86.255/tcp/6745/p2p/12D3KooWFrnuj5o3tx4fGD2ZVJRyDqTdzGnU3XYXmBbWbc8Hs8Nd",
		"/ip4/3.134.223.177/tcp/6745/p2p/12D3KooWN8vAoGd6eurUSidcpLYguQiGZwt4eVgDvbgaS7kiGTup",
		"/ip4/35.74.45.12/udp/6746/quic/p2p/12D3KooWLV128pddyvoG6NBvoZw7sSrgpMTPtjnpu3mSmENqhtL7",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
	} {
		ma, err := multiaddr.NewMultiaddr(s)
		if err != nil {
			panic(err)
		}
		defaultTestBootstrapPeers = append(defaultTestBootstrapPeers, ma)
	}

	peers, _ := peer.AddrInfosFromP2pAddrs(defaultTestBootstrapPeers...)
	return peers
}
