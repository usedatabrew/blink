package schema

import "testing"

func Test_inferSchemaFromJson(t *testing.T) {
	inferSchemaFromJson([]byte(""))
}
