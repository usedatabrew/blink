package helper

import "strings"

func NormalizeStreamName(stream string) string {
	var normalizedName = stream
	if len(strings.Split(stream, ".")) == 2 {
		normalizedName = strings.Split(stream, ".")[1]
	}

	return normalizedName
}
