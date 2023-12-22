package sqlproc

// Yes, I know this is a bad idea;
// I'll think about it a bit later
func compareValues(a, b interface{}, op string) bool {
	switch v := a.(type) {
	case int:
		switch op {
		case ">":
			return v > b.(int)
		case "<":
			return v < b.(int)
		case ">=":
			return v >= b.(int)
		case "<=":
			return v <= b.(int)
		}
	case int8:
		switch op {
		case ">":
			return v > b.(int8)
		case "<":
			return v < b.(int8)
		case ">=":
			return v >= b.(int8)
		case "<=":
			return v <= b.(int8)
		}
	case int16:
		switch op {
		case ">":
			return v > b.(int16)
		case "<":
			return v < b.(int16)
		case ">=":
			return v >= b.(int16)
		case "<=":
			return v <= b.(int16)
		}
	case int32:
		switch op {
		case ">":
			return v > b.(int32)
		case "<":
			return v < b.(int32)
		case ">=":
			return v >= b.(int32)
		case "<=":
			return v <= b.(int32)
		}
	case int64:
		switch op {
		case ">":
			return v > b.(int64)
		case "<":
			return v < b.(int64)
		case ">=":
			return v >= b.(int64)
		case "<=":
			return v <= b.(int64)
		}
	case uint:
		switch op {
		case ">":
			return v > b.(uint)
		case "<":
			return v < b.(uint)
		case ">=":
			return v >= b.(uint)
		case "<=":
			return v <= b.(uint)
		}
	case uint8:
		switch op {
		case ">":
			return v > b.(uint8)
		case "<":
			return v < b.(uint8)
		case ">=":
			return v >= b.(uint8)
		case "<=":
			return v <= b.(uint8)
		}
	case uint16:
		switch op {
		case ">":
			return v > b.(uint16)
		case "<":
			return v < b.(uint16)
		case ">=":
			return v >= b.(uint16)
		case "<=":
			return v <= b.(uint16)
		}
	case uint32:
		switch op {
		case ">":
			return v > b.(uint32)
		case "<":
			return v < b.(uint32)
		case ">=":
			return v >= b.(uint32)
		case "<=":
			return v <= b.(uint32)
		}
	case uint64:
		switch op {
		case ">":
			return v > b.(uint64)
		case "<":
			return v < b.(uint64)
		case ">=":
			return v >= b.(uint64)
		case "<=":
			return v <= b.(uint64)
		}
	case float32:
		switch op {
		case ">":
			return v > b.(float32)
		case "<":
			return v < b.(float32)
		case ">=":
			return v >= b.(float32)
		case "<=":
			return v <= b.(float32)
		}
	case float64:
		switch op {
		case ">":
			return v > b.(float64)
		case "<":
			return v < b.(float64)
		case ">=":
			return v >= b.(float64)
		case "<=":
			return v <= b.(float64)
		}
	case string:
		switch op {
		case "=":
			return v != b.(string)
		case "!=":
			return v == b.(string)
		}
	default:
		return false
	}

	return false
}
