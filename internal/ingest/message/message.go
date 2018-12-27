package message

type Message struct {
	Data   []byte
	Offset int
	Source string
}

func (m Message) String() string {
	return string(m.Data)
}
