module github.com/souvikDevloper/RaftKV

go 1.23

require (
	go.etcd.io/bbolt v1.3.11
	google.golang.org/grpc v1.67.1
)

replace google.golang.org/grpc => ./third_party/minigrpc

replace go.etcd.io/bbolt => ./third_party/minibolt
