package raw

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
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
	deliver              chan Record
	bufferSize           int
	flushTimeout         time.Duration
	firehoseRequestLimit int
	debugLog             bool
	printContent         bool
	debugContainers      map[string]bool
}

type ContainerInfo struct {
	Name      string `json:"name"`
	Fullpod   string `json:"full-pod,omitempty"`
	Podprefix string `json:"pod,omitempty"`
	Namespace string `json:"ns,omitempty"`
	ID        string `json:"id,omitempty"`
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

	print := false
	if os.Getenv("LOG_CONTENT") != "" {
		log.Println("firehose: activating print mode")
		print = true
	}

	debugContainers := make(map[string]bool)
	if os.Getenv("DEBUG_CONTAINERS") != "" {
		log.Println("firehose: activating debug containers list")
		for _, id := range strings.Split(os.Getenv("DEBUG_CONTAINERS"), ",") {
			container := normalID(strings.TrimSpace(id))
			log.Println("debugging container: ", container)
			debugContainers[container] = true
		}
	}

	svc := firehose.New(sess)

	streamName := aws.String(deliveryStreamName)
	adapter := &Adapter{
		route:                route,
		svc:                  svc,
		deliveryStreamName:   streamName,
		deliver:              make(chan Record),
		bufferSize:           bufferSize,
		flushTimeout:         flushTimeout,
		firehoseRequestLimit: firehoseRequestLimit,
		debugLog:             debug,
		printContent:         print,
		debugContainers:      debugContainers,
	}

	go adapter.batchPutToFirehose()

	return adapter, nil
}

var levelToValue = map[string]int{
	"TRACE":    000,
	"trace":    000,
	"DEBUG":    100,
	"debug":    100,
	"INFO":     200,
	"info":     200,
	"NOTICE":   250,
	"notice":   250,
	"WARN":     300,
	"warn":     300,
	"ERROR":    400,
	"error":    400,
	"FATAL":    500,
	"fatal":    500,
	"CRITICAL": 500,
	"critical": 500,
}

// Stream decode message and send it to the firehose queue
func (adapter *Adapter) Stream(logstream chan *router.Message) {

	for message := range logstream {

		container := extractKubernetesInfo(message.Container)
		dec := json.NewDecoder(strings.NewReader(message.Data))
		for {
			var data Record

			err := dec.Decode(&data)
			if err == io.EOF {
				// all done with this message, check next one
				continue
			}
			if err != nil {
				// not json
				data = make(map[string]interface{})
				data["message"] = message.Data
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

			// make sure level is a number
			// and populate log_level as string
			if v, exist := data["level"]; exist {
				if nb, ok := v.(string); ok {
					if _, err := strconv.Atoi(nb); err != nil {
						data["log_level"] = nb
						if level, exist := levelToValue[nb]; exist {
							data["level"] = level
						} else {
							delete(data, "level")
						}
					}
				}
			}

			data["host"] = message.Container.Config.Hostname
			data["container"] = container
			data["source"] = message.Source

			if _, exist := data["@timestamp"]; !exist {
				data["@timestamp"] = message.Time
			}
			if _, exist := data["@version"]; !exist {
				data["@version"] = 1
			}

			if adapter.printContent && adapter.debugContainers[container.ID] {
				adapter.logD("stream: %s enqueing: %v \n", container.ID, data)
			}

			adapter.deliver <- data
		}
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

				// buffer, and optionally flush
				bytes, err := json.Marshal(record)
				if err != nil {
					log.Println("batch: json marshalling error - ", err)
				}
				frecord := &firehose.Record{
					Data: append(bytes, "\n"...),
				}

				adapter.logD("batch: bufferSize: %d, len: %d < %d \n", bufferSize, len(buffer), cap(buffer))

				if len(buffer) == cap(buffer) || bufferSize+len(frecord.Data) >= adapter.firehoseRequestLimit {
					timeout.Reset(adapter.flushTimeout)
					go adapter.flushBuffer(buffer)
					buffer = adapter.newBuffer()
					bufferSize = 0
				}

				if adapter.printContent {
					if v, ok := record["container"].(*ContainerInfo); ok {
						if adapter.debugContainers[v.ID] {
							adapter.logD("batch: %s batching record: %v", v.ID, string(frecord.Data))
						}
					}
				}

				bufferSize = bufferSize + len(frecord.Data)
				buffer = append(buffer, frecord)
			}
		case <-timeout.C:
			{
				// flush
				adapter.logD("batch: timeout: %d, len: %d < %d \n", bufferSize, len(buffer), cap(buffer))
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

func extractKubernetesInfo(container *docker.Container) *ContainerInfo {
	if val, exist := container.Config.Labels["io.kubernetes.container.name"]; exist {
		fullPod := container.Config.Labels["io.kubernetes.pod.name"]
		pod := strings.Split(container.Config.Labels["io.kubernetes.pod.name"], "-")
		podPrefix := fullPod
		// blahblah-rs_id-pod_id
		if len(pod) > 2 {
			// by default remove the 2 last components (rs_id and pod_id)
			joinSegments := 2
			lastPart := pod[len(pod)-1]
			if len(lastPart) < 5 && isInt(lastPart) {
				// we have a statefulset, so we only remove the last component
				// which is the statefulset index
				joinSegments = 1
			}
			podPrefix = strings.Join(pod[:len(pod)-joinSegments], "-")
		} else if len(pod) > 0 {
			podPrefix = pod[0]
		}
		return &ContainerInfo{
			Name:      val,
			Fullpod:   container.Config.Labels["io.kubernetes.pod.name"],
			Podprefix: podPrefix,
			Namespace: container.Config.Labels["io.kubernetes.pod.namespace"],
			ID:        normalID(container.ID),
		}
	} else {
		return &ContainerInfo{
			Name: container.Name,
			ID:   normalID(container.ID),
		}
	}
}

func normalID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

var allDigits = regexp.MustCompile(`^[0-9]+$`)

func isInt(v string) bool {
	switch v {
	case "0":
		return true
	case "1":
		return true
	case "2":
		return true
	case "3":
		return true
	case "4":
		return true
	case "5":
		return true
	default:
		return allDigits.MatchString(v)
	}
}
