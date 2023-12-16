package sqlproc

import "strconv"

func guessType(str string) interface{} {
	if i, err := strconv.ParseInt(str, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(str, 64); err == nil {
		return f
	}
	if b, err := strconv.ParseBool(str); err == nil {
		return b
	}
	return str
}
