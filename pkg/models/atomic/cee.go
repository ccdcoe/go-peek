package atomic

import "strings"

func isCommonEventExpression(msg string) bool {
	if strings.HasPrefix(msg, "@cee: ") || strings.HasPrefix(msg, " @cee: ") {
		return true
	}
	return false
}

func getCommonEventExpressionPayload(raw string) []byte {
	return []byte(strings.TrimPrefix(strings.TrimLeft(raw, " "), "@cee: "))
}
