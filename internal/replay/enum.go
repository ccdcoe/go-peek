package replay

type Partial int

const (
	CompletelyInRange Partial = iota
	HeadInRange
	TailInRange
	MiddleSectionInRange
)

func (p Partial) string() string {
	switch p {
	case HeadInRange:
		return "handle head is in specified range"
	case TailInRange:
		return "handle tail is in specified range"
	case MiddleSectionInRange:
		return "handle middle section is in specified range"
	default:
		return "handle is fully or not at all in range"
	}
}

type Format int

const (
	JSON Format = iota
	Gob
)

func (f Format) ext() string {
	switch f {
	case JSON:
		return ".json"
	case Gob:
		return ".gob"
	default:
		return ".dump"
	}
}
