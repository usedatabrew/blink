package offset_storage

import "fmt"

func BuildKey(pipelineId int64, stream string) string {
	return fmt.Sprintf("pipeline_%d_stream_%s", pipelineId, stream)
}
