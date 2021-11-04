package process

import jsoniter "github.com/json-iterator/go"

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func IsSuricataAlert(raw []byte) bool {
	val := json.Get(raw, "event_type")
	if val == nil {
		return false
	}
	return val.ToString() == "alert"
}
