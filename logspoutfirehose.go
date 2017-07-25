package logspoutfirehose

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/gliderlabs/logspout/router"
)

type Adapter struct {
	route              *router.Route
	svc                *firehose.Firehose
	deliveryStreamName string
}

type Record struct {
	Timestamp     time.Time              `json:"@timestamp"`
	ContainerId   string                 `json:"container_id"`
	ContainerName string                 `json:"container_name"`
	Message       string                 `json:"message"`
	Hostname      string                 `json:"hostname"`
	Source        string                 `json:"source"` // stdout, stderr
	Args          map[string]interface{} `json:"args,omitempty"`
}

func init() {
	router.AdapterFactories.Register(NewRawAdapter, "firehose")
}

func NewRawAdapter(route *router.Route) (router.LogAdapter, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	deliveryStreamName := route.Address
	if deliveryStreamName == "" {
		return nil, errors.New("delivery stream name required in format firehose://<stream-name>")
	}

	svc := firehose.New(sess)

	return &Adapter{
		route:              route,
		svc:                svc,
		deliveryStreamName: deliveryStreamName,
	}, nil
}

func (adapter *Adapter) Stream(logstream chan *router.Message) {
	streamName := aws.String(adapter.deliveryStreamName)

	for message := range logstream {

		var jsonMsg map[string]interface{}
		err := json.Unmarshal([]byte(message.Data), &jsonMsg)
		// error is ignored because we don't care if we could not unmarshal
		// we will just not have additional args
		if err != nil {
		}

		info := Record{
			Timestamp:     message.Time,
			ContainerId:   message.Container.ID,
			Message:       message.Data,
			ContainerName: message.Container.Name,
			Hostname:      message.Container.Config.Hostname,
			Source:        message.Source,
			Args:          jsonMsg,
		}

		bytes, err := json.Marshal(info)
		if err != nil {
			log.Println("firehose: ", err)
		}

		params := &firehose.PutRecordInput{
			DeliveryStreamName: streamName,
			Record: &firehose.Record{
				Data: append(bytes, "\n"...),
			},
		}

		_, err = adapter.svc.PutRecord(params)
		if err != nil {
			log.Println("firehose: ", err)
		}
	}
}
