package ingest

type Module int

func (m Module) String() string {
	switch m {
	case Kafka:
		return "kafka"
	case UxSock:
		return "uxsock"
	case Redis:
		return "redis"
	case NamedPipe:
		return "fifo"
	case Tail:
		return "tail"
	default:
		return "unsupported"
	}
}

func (m Module) Explain() string {
	switch m {
	case Kafka:
		return `Kafka consumer. 
		A clustered message broker for multiple producer and multiple consumer environment. 
		Tracks offsets for for each group of consumers, to avoid message loss when consumer stops.`
	case UxSock:
		return `Unix domain socket.
		A data communications endpoint for exchanging data between processes executing on the same host operating system. 
		Standard component of POSIX operating systems.`
	case Redis:
		return `Redis is an open-source in-memory database project implementing a distributed, in-memory key-value store with optional durability.`
	case NamedPipe:
		return `Named pipe is an extension to the traditional pipe concept on Unix and Unix-like systems, and is one of the methods of inter-process communication.`
	case Tail:
		return `Consume log file with optional offset tracking.`
	default:
		return "unsupported"
	}
}

const (
	Unsupported Module = iota
	Kafka
	UxSock
	Redis
	NamedPipe
	Tail
)

var Modules = []Module{
	Kafka,
}
