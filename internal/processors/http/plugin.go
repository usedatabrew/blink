package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/usedatabrew/blink/internal/message"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/stream_context"
	"io"
	"net/http"
)

type Plugin struct {
	config Config
	ctx    *stream_context.Context
	model  string
	prompt string
}

func NewHttpPlugin(appctx *stream_context.Context, config Config) (*Plugin, error) {
	return &Plugin{config: config, ctx: appctx}, nil
}

func (p *Plugin) Process(context context.Context, msg *message.Message) (*message.Message, error) {
	if msg.GetStream() != p.config.StreamName {
		return msg, nil
	}

	var requestPayload []byte
	// means we have to pack all the message and send it over http
	if p.config.Source == "*" {
		requestPayload, _ = msg.Data.MarshalJSON()
	} else {
		sourceFieldValue := msg.GetValue(p.config.Source)
		if marshaled, err := json.Marshal(&sourceFieldValue); err != nil {
			return nil, err
		} else {
			requestPayload = marshaled
		}
	}

	req, err := http.NewRequest(p.config.Method, p.config.Endpoint, bytes.NewReader(requestPayload))
	if err != nil {
		return nil, err
	}

	if p.config.Auth.BasicAuthUser != "" && p.config.Auth.BasicAuthPassword != "" {
		req.SetBasicAuth(p.config.Auth.BasicAuthUser, p.config.Auth.BasicAuthPassword)
	}

	for key, value := range p.config.Auth.Headers {
		req.Header.Set(key, value)
	}

	client := http.Client{}
	var resp *http.Response
	if resp, err = client.Do(req); err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode > 400 {
		return nil, errors.New("response code from endpoint is higher than 400")
	}

	if p.config.TargetField == "" {
		return msg, err
	}

	var bodyResult []byte
	if bodyResult, err = io.ReadAll(resp.Body); err != nil {
		return nil, err
	}

	msg.SetNewField(p.config.TargetField, string(bodyResult), arrow.BinaryTypes.String)

	return msg, nil
}

// EvolveSchema will add a string column to the schema in order to store OpenAI response
func (p *Plugin) EvolveSchema(streamSchema *schema.StreamSchemaObj) error {
	if p.config.TargetField != "" {
		streamSchema.AddField(p.config.StreamName, p.config.TargetField, arrow.BinaryTypes.String, message.ArrowToPg10(arrow.BinaryTypes.String))
	} else {
		streamSchema.FakeEvolve()
	}

	return nil
}
