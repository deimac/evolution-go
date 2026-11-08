package webhook_producer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	producer_interfaces "github.com/evolution-foundation/evolution-go/pkg/events/interfaces"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
)

// Webhooks go through one bounded queue served by a few workers.
//
// Each event used to be sent from its own goroutine, with no limit and a fixed
// 30s retry. When an instance reconnected with a large offline queue (thousands
// of events), all of them hit the receiver at once and every retry round hit it
// again, taking the receiver down and dropping the events after 5 attempts.
//
// Now at most `workers` requests run at the same time, retries back off
// exponentially, and a full queue makes Produce wait: the whatsmeow event
// handler slows down and WhatsApp delivers the offline queue at the pace the
// receiver can take, instead of events being dropped.
const (
	defaultWorkers     = 4
	defaultQueueSize   = 10000
	defaultMaxAttempts = 8
	defaultTimeout     = 120 * time.Second
	baseBackoff        = 2 * time.Second
	maxBackoff         = 5 * time.Minute
)

type webhookJob struct {
	url    string
	body   []byte
	userID string
}

type webhookProducer struct {
	url           string
	loggerWrapper *logger_wrapper.LoggerManager
	jobs          chan webhookJob
	client        *http.Client
	maxAttempts   int
	backoff       func(attempt int) time.Duration
	sleep         func(time.Duration)
}

func NewWebhookProducer(
	url string,
	loggerWrapper *logger_wrapper.LoggerManager,
) producer_interfaces.Producer {
	return newWebhookProducer(url, loggerWrapper,
		envInt("WEBHOOK_MAX_CONCURRENCY", defaultWorkers),
		envInt("WEBHOOK_QUEUE_SIZE", defaultQueueSize),
	)
}

func newWebhookProducer(url string, loggerWrapper *logger_wrapper.LoggerManager, workers, queueSize int) *webhookProducer {
	if workers < 1 {
		workers = 1
	}
	if queueSize < 1 {
		queueSize = 1
	}
	p := &webhookProducer{
		url:           url,
		loggerWrapper: loggerWrapper,
		jobs:          make(chan webhookJob, queueSize),
		client:        &http.Client{Timeout: defaultTimeout},
		maxAttempts:   defaultMaxAttempts,
		backoff:       expBackoff,
		sleep:         time.Sleep,
	}
	for i := 0; i < workers; i++ {
		go p.worker()
	}
	return p
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(strings.TrimSpace(os.Getenv(name))); err == nil && v > 0 {
		return v
	}
	return def
}

// expBackoff: 2s, 4s, 8s ... up to 5min, plus up to 20% jitter so the retries
// of a burst do not reach the receiver at the same instant.
func expBackoff(attempt int) time.Duration {
	d := maxBackoff
	if attempt >= 1 && attempt < 20 {
		if b := baseBackoff << uint(attempt-1); b < maxBackoff {
			d = b
		}
	}
	return d + time.Duration(rand.Int63n(int64(d)/5+1))
}

func (p *webhookProducer) Produce(
	queueName string,
	payload []byte,
	webhookUrl string,
	userID string,
) error {
	splitQueue := strings.Split(queueName, ".")

	if len(splitQueue) < 2 {
		return nil
	}

	if p.url != "" {
		p.jobs <- webhookJob{url: p.url, body: payload, userID: userID}
	}
	if webhookUrl != "" {
		p.jobs <- webhookJob{url: webhookUrl, body: payload, userID: userID}
	}

	return nil
}

func (p *webhookProducer) worker() {
	for job := range p.jobs {
		p.sendWebhookWithRetry(job.url, job.body, job.userID)
	}
}

func (p *webhookProducer) sendWebhookWithRetry(url string, body []byte, userID string) {
	log := p.loggerWrapper.GetLogger(userID)
	for attempt := 1; attempt <= p.maxAttempts; attempt++ {
		err, responseBody, statusCode := p.sendWebhook(url, body)
		if err == nil {
			log.LogInfo("[%s] webhook sent successfully - url: %s, status: %d, response: %s", userID, url, statusCode, string(responseBody))
			return
		}
		if !retryable(statusCode) {
			log.LogError("[%s] webhook rejected, not retrying - url: %s, status: %d, error: %v", userID, url, statusCode, err)
			return
		}
		if attempt == p.maxAttempts {
			break
		}
		wait := p.backoff(attempt)
		log.LogWarn("[%s] webhook failed - url: %s, attempt: %d, retry in %s, error: %v", userID, url, attempt, wait.Round(time.Second), err)
		p.sleep(wait)
	}
	log.LogError("[%s] webhook failed after maximum retries - url: %s", userID, url)
}

// retryable: network errors (status 0), timeout, rate limit and 5xx. Any other
// 4xx fails the same way on every attempt.
func retryable(statusCode int) bool {
	return statusCode == 0 || statusCode == http.StatusRequestTimeout || statusCode == http.StatusTooManyRequests || statusCode >= 500
}

func (p *webhookProducer) sendWebhook(url string, body []byte) (error, []byte, int) {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return err, nil, 0
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return err, nil, 0
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("erro ao ler resposta: %v", err), nil, 0
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.New("received non-2xx response: " + resp.Status), responseBody, resp.StatusCode
	}

	return nil, responseBody, resp.StatusCode
}

// CreateGlobalQueues não faz nada para webhook producer
func (p *webhookProducer) CreateGlobalQueues() error {
	return nil
}
