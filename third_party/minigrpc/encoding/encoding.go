package encoding

type Codec interface {
	Name() string
	Marshal(any) ([]byte, error)
	Unmarshal([]byte, any) error
}

func RegisterCodec(Codec) {}
