package service_registry

type ServiceState string

const (
	Starting ServiceState = "starting"
	Loaded   ServiceState = "loaded"
	Started  ServiceState = "started"
	Failing  ServiceState = "failing"
)
