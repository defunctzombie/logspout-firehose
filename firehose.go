package raw

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	docker "github.com/fsouza/go-dockerclient"
	"github.com/gliderlabs/logspout/router"
)

var (
	bufferSizeEnv      = "FIREHOSE_BUFFERSIZE"
	flushTimeoutEnv    = "FIREHOSE_FLUSH_TIMEOUT"
	requestLimitEnv    = "FIREHOSE_REQUEST_LIMIT"
	overhead           = 1024
	firehoseSizeLimit  = (4*1024 - overhead) * 1000
	firehoseBatchLimit = 500
)

type Adapter struct {
	route                *router.Route
	svc                  *firehose.Firehose
	deliveryStreamName   *string
	deliver              chan *Record
	bufferSize           int
	flushTimeout         time.Duration
	firehoseRequestLimit int
	debugLog             bool
}

type Container struct {
	Name      string `json:"name"`
	Fullpod   string `json:"full-pod,omitempty"`
	Podprefix string `json:"pod,omitempty"`
	Namespace string `json:"ns,omitempty"`
}

type Record map[string]interface{}

// type Record struct {
// 	Timestamp time.Time              `json:"@timestamp"`
// 	Container *Container             `json:"container,omitempty"`
// 	Message   string                 `json:"message,omitempty"`
// 	Hostname  string                 `json:"hostname"`
// 	Source    string                 `json:"source"` // stdout, stderr
// 	Args      map[string]interface{} `json:"args,omitempty"`
// }

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

	bufferSize := firehoseBatchLimit
	if os.Getenv(bufferSizeEnv) != "" {
		bufferSize, err = strconv.Atoi(os.Getenv(bufferSizeEnv))
		if err != nil {
			return nil, fmt.Errorf("Invalid %s env var: %v", bufferSizeEnv, os.Getenv(bufferSizeEnv))
		}
	}

	firehoseRequestLimit := firehoseSizeLimit
	if os.Getenv(requestLimitEnv) != "" {
		firehoseRequestLimit, err = strconv.Atoi(os.Getenv(requestLimitEnv))
		if err != nil {
			return nil, fmt.Errorf("Invalid %s env var: %v", requestLimitEnv, os.Getenv(requestLimitEnv))
		}
	}

	flushTimeout := time.Duration(10) * time.Second
	if os.Getenv(flushTimeoutEnv) != "" {
		duration, err := strconv.Atoi(os.Getenv(flushTimeoutEnv))
		if err != nil {
			return nil, fmt.Errorf("Invalid %s env var: %v", flushTimeoutEnv, os.Getenv(flushTimeoutEnv))
		}
		flushTimeout = time.Duration(duration) * time.Second
	}

	debug := false
	if os.Getenv("DEBUG") != "" {
		log.Println("firehose: activating debug mode")
		debug = true
	}

	svc := firehose.New(sess)

	streamName := aws.String(deliveryStreamName)
	adapter := &Adapter{
		route:                route,
		svc:                  svc,
		deliveryStreamName:   streamName,
		deliver:              make(chan *Record),
		bufferSize:           bufferSize,
		flushTimeout:         flushTimeout,
		firehoseRequestLimit: firehoseRequestLimit,
		debugLog:             debug,
	}

	go adapter.batchPutToFirehose()

	return adapter, nil
}

func (adapter *Adapter) Stream(logstream chan *router.Message) {

	for message := range logstream {

		var data Record
		err := json.Unmarshal([]byte(message.Data), &data)
		if err != nil {
			// not json
			data = make(map[string]interface{})
			data["message"] = message.Data
		}

		//
		container := extractKubernetesInfo(message.Container)

		data["host"] = message.Container.Config.Hostname
		data["container"] = container
		data["source"] = message.Source

		if _, exist := data["@timestamp"]; !exist {
			data["@timestamp"] = message.Time
		}
		if _, exist := data["@version"]; !exist {
			data["@version"] = 1
		}

		// rewrite format V0 into format V1
		if fields, exist := data["@fields"]; exist {
			if fieldMap, err := fields.(map[string]interface{}); err {
				for k, v := range fieldMap {
					data[strings.TrimLeft(k, "@")] = v
				}
				delete(data, "@fields")
			}
			// convert other fields?
		}

		adapter.deliver <- &data
	}
}

func (adapter *Adapter) batchPutToFirehose() {
	buffer := adapter.newBuffer()

	timeout := time.NewTimer(adapter.flushTimeout)
	bufferSize := 0

	for {
		select {
		case record := <-adapter.deliver:
			{
				//adapter.logD("batchPutToFirehose: got a record: %v \n", record)

				// buffer, and optionally flush
				bytes, err := json.Marshal(record)
				if err != nil {
					log.Println("batchPutToFirehose: json marshalling error - ", err)
				}
				frecord := &firehose.Record{
					Data: append(bytes, "\n"...),
				}

				adapter.logD("batchPutToFirehose: bufferSize: %d, len: %d < %d \n", bufferSize, len(buffer), cap(buffer))

				if len(buffer) == cap(buffer) || bufferSize+len(frecord.Data) >= adapter.firehoseRequestLimit {
					timeout.Reset(adapter.flushTimeout)
					go adapter.flushBuffer(buffer)
					buffer = adapter.newBuffer()
					bufferSize = 0
				}

				bufferSize = bufferSize + len(frecord.Data)
				buffer = append(buffer, frecord)
			}
		case <-timeout.C:
			{
				// flush
				adapter.logD("batchPutToFirehose: timeout: %d, len: %d < %d \n", bufferSize, len(buffer), cap(buffer))
				if len(buffer) > 0 {
					go adapter.flushBuffer(buffer)
					buffer = adapter.newBuffer()
					bufferSize = 0
				}
				timeout.Reset(adapter.flushTimeout)
			}
		}
	}
}

func (adapter *Adapter) newBuffer() []*firehose.Record {
	return make([]*firehose.Record, 0, adapter.bufferSize)
}

func (adapter *Adapter) flushBuffer(buffer []*firehose.Record) error {
	// flush
	params := &firehose.PutRecordBatchInput{
		DeliveryStreamName: adapter.deliveryStreamName,
		Records:            buffer,
	}

	err := retry(3, 2*time.Second, func() error {
		adapter.logD("flushBuffer (retry): sending %d records\n", len(buffer))
		response, err := adapter.svc.PutRecordBatch(params)
		if err != nil {
			adapter.logD("flushBuffer (retry): result %v\n", err)
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case firehose.ErrCodeServiceUnavailableException:
					adapter.logD("firehose: service unavailable: %s - retrying\n", err)
					return err
				default:
					return stop{err}
				}
			}
		}
		if response != nil && *response.FailedPutCount > 0 {
			adapter.logD("firehose: %d records failed - retrying\n", *response.FailedPutCount)
			// re-arrange buffer to keep only failed records
			buffer := make([]*firehose.Record, 0, *response.FailedPutCount)
			for i, r := range response.RequestResponses {
				if r.ErrorCode != nil {
					buffer = append(buffer, params.Records[i])
				}
			}
			params.SetRecords(buffer)
			return fmt.Errorf("retrying %d failed records", *response.FailedPutCount)
		}
		adapter.logD("flushBuffer (retry): everything has been sent\n")
		return nil
	})

	adapter.logD("flushBuffer (abort/ok): %v\n", err)
	if err != nil {
		log.Println("firehose: batch error", err.Error())
	}
	return err
}

func retry(attempts int, sleep time.Duration, fn func() error) error {
	if err := fn(); err != nil {
		if s, ok := err.(stop); ok {
			// Return the original error for later checking
			return s.error
		}

		if attempts--; attempts > 0 {
			time.Sleep(sleep)
			return retry(attempts, 2*sleep, fn)
		}
		return err
	}
	return nil
}

type stop struct {
	error
}

func (adapter *Adapter) logD(format string, args ...interface{}) {
	if adapter.debugLog {
		log.Printf("firehose: "+format, args...)
	}
}

func extractKubernetesInfo(container *docker.Container) *Container {
	if val, exist := container.Config.Labels["io.kubernetes.container.name"]; exist {
		fullPod := container.Config.Labels["io.kubernetes.pod.name"]
		pod := strings.Split(container.Config.Labels["io.kubernetes.pod.name"], "-")
		podPrefix := fullPod
		if len(pod) > 0 {
			podPrefix = pod[0]
		}
		return &Container{
			Name:      val,
			Fullpod:   container.Config.Labels["io.kubernetes.pod.name"],
			Podprefix: podPrefix,
			Namespace: container.Config.Labels["io.kubernetes.pod.namespace"],
		}
	} else {
		return &Container{
			Name: container.Name,
		}
	}
}
