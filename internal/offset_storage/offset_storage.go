package offset_storage

// OffsetStorage is used to store checkpoints/offsets for source connectors
// which use incremental sync by primary keys
type OffsetStorage interface {
	SetOffsetForPipeline(key string, offset int64) error
	GetOffsetByPipelineStream(key string) (int64, error)
}
