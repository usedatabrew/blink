package message

type Message struct {
	ID   int               `json:"id"`
	Data string            `json:"data"`
	Meta map[string]string `json:"meta"`
}
