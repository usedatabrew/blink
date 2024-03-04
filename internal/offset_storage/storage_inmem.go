package offset_storage

type StorageInMem struct {
	offsets map[string]int64
}

func NewStorageInMem() *StorageInMem {
	return &StorageInMem{
		offsets: map[string]int64{},
	}
}

func (o *StorageInMem) GetOffsetByPipelineStream(key string) (int64, error) {
	if off, exist := o.offsets[key]; exist {
		return off, nil
	}

	return 0, nil
}

func (o *StorageInMem) SetOffsetForPipeline(key string, offset int64) error {
	o.offsets[key] = offset
	return nil
}
