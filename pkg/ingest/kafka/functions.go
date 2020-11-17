package kafka

type OffsetMode int

const (
	OffsetLastCommit OffsetMode = iota
	OffsetEarliest
	OffsetLatest
)

func TranslateOffsetMode(offset string) OffsetMode {
	switch offset {
	case "beginning":
		return OffsetEarliest
	case "latest":
		return OffsetLatest
	default:
		return OffsetLastCommit
	}
}
