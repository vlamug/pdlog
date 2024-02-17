package discovery

type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}
