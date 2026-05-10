package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"flag"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

const (
	defaultLogLevel = log.InfoLevel
)

type metricsCacheHandler struct {
	mutex             sync.Mutex
	handler           http.Handler
	refreshCond       *sync.Cond
	refreshing        bool
	backgroundRefresh bool
	lastScrapeStarted time.Time
	cachedStatus      int
	cachedHeader      http.Header
	cachedBody        []byte
}

type metricsResponse struct {
	status             int
	header             http.Header
	body               []byte
	lastScrapeStarted  time.Time
	servedFromCache    bool
	servedStale        bool
	cacheWaitStartedAt time.Time
}

type captureResponseWriter struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func newMetricsCacheHandler(handler http.Handler) *metricsCacheHandler {
	h := &metricsCacheHandler{handler: handler}
	h.refreshCond = sync.NewCond(&h.mutex)
	return h
}

func (w *captureResponseWriter) Header() http.Header {
	return w.header
}

func (w *captureResponseWriter) WriteHeader(status int) {
	w.status = status
}

func (w *captureResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(p)
}

func (h *metricsCacheHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.WithFields(log.Fields{
		"remote_addr": r.RemoteAddr,
		"method":      r.Method,
		"user_agent":  r.UserAgent(),
	}).Info("Metrics endpoint requested")

	response := h.response(r)
	h.writeResponse(w, r, response)
	if response.servedFromCache {
		fields := log.Fields{
			"cache_age":       time.Since(response.lastScrapeStarted),
			"duration":        time.Since(response.cacheWaitStartedAt),
			"scrape_interval": time.Duration(config.ScrapeInterval) * time.Second,
		}
		if response.servedStale {
			fields["stale"] = true
		}
		log.WithFields(fields).Info("Metrics served from cache")
	}
}

func (h *metricsCacheHandler) response(r *http.Request) metricsResponse {
	waitStartedAt := time.Now()

	h.mutex.Lock()
	if len(h.cachedBody) > 0 {
		response := h.cachedResponseLocked(false, waitStartedAt)
		h.mutex.Unlock()
		return response
	}
	if h.refreshing || h.backgroundRefresh {
		for len(h.cachedBody) == 0 {
			h.refreshCond.Wait()
		}
		response := h.cachedResponseLocked(false, waitStartedAt)
		h.mutex.Unlock()
		return response
	}
	h.refreshing = true
	h.mutex.Unlock()

	response := h.refresh(r)
	return response
}

func (h *metricsCacheHandler) startBackgroundRefresh(ctx context.Context) {
	if config.ScrapeInterval <= 0 {
		return
	}
	h.mutex.Lock()
	h.backgroundRefresh = true
	h.mutex.Unlock()
	go func() {
		for {
			started := time.Now()
			h.refresh(nil)
			wait := time.Duration(config.ScrapeInterval)*time.Second - time.Since(started)
			if wait < 0 {
				wait = 0
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
		}
	}()
}

func (h *metricsCacheHandler) refresh(r *http.Request) metricsResponse {
	started := time.Now()
	log.WithField("scrape_interval", time.Duration(config.ScrapeInterval)*time.Second).Info("Background RabbitMQ metrics refresh started")
	if r == nil {
		r = newBackgroundMetricsRequest()
	}
	capture := &captureResponseWriter{header: make(http.Header)}
	h.handler.ServeHTTP(capture, r)
	if capture.status == 0 {
		capture.status = http.StatusOK
	}

	response := metricsResponse{
		status:            capture.status,
		header:            capture.header.Clone(),
		body:              append([]byte(nil), capture.body.Bytes()...),
		lastScrapeStarted: started,
	}

	h.mutex.Lock()
	if h.shouldReplaceCacheLocked(response.body) {
		h.cachedStatus = response.status
		h.cachedHeader = response.header.Clone()
		h.cachedBody = append(h.cachedBody[:0], response.body...)
		h.lastScrapeStarted = started
	} else {
		log.WithFields(log.Fields{
			"old_body_bytes": len(h.cachedBody),
			"new_body_bytes": len(response.body),
		}).Warn("Discarding suspiciously small metrics payload")
		response = h.cachedResponseLocked(false, started)
	}
	h.refreshing = false
	h.refreshCond.Broadcast()
	h.mutex.Unlock()

	log.WithFields(log.Fields{
		"body_bytes":      len(response.body),
		"duration":        time.Since(started),
		"scrape_interval": time.Duration(config.ScrapeInterval) * time.Second,
	}).Info("Background RabbitMQ metrics refreshed")

	return response
}

func (h *metricsCacheHandler) shouldReplaceCacheLocked(newBody []byte) bool {
	if len(h.cachedBody) == 0 {
		return true
	}
	return len(newBody) >= len(h.cachedBody)*8/10
}

func newBackgroundMetricsRequest() *http.Request {
	req, _ := http.NewRequest("GET", "/metrics", nil)
	return req
}

func (h *metricsCacheHandler) cachedResponseLocked(stale bool, waitStartedAt time.Time) metricsResponse {
	return metricsResponse{
		status:             h.cachedStatus,
		header:             h.cachedHeader.Clone(),
		body:               append([]byte(nil), h.cachedBody...),
		lastScrapeStarted:  h.lastScrapeStarted,
		servedFromCache:    true,
		servedStale:        stale,
		cacheWaitStartedAt: waitStartedAt,
	}
}

func (h *metricsCacheHandler) writeResponse(w http.ResponseWriter, r *http.Request, response metricsResponse) {
	copyHeader(w.Header(), response.header)
	if r != nil && acceptsGzip(r) {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(response.status)
		gz := gzip.NewWriter(w)
		_, _ = gz.Write(response.body)
		_ = gz.Close()
		return
	}
	w.Header().Del("Content-Encoding")
	w.WriteHeader(response.status)
	_, _ = w.Write(response.body)
}

func acceptsGzip(r *http.Request) bool {
	for _, part := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		if strings.TrimSpace(strings.Split(part, ";")[0]) == "gzip" {
			return true
		}
	}
	return false
}

func copyHeader(dst, src http.Header) {
	for key, values := range src {
		dst.Del(key)
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func initLogger() {
	log.SetLevel(getLogLevel())
	if strings.ToUpper(config.OutputFormat) == "JSON" {
		log.SetFormatter(&log.JSONFormatter{})
	} else {
		// The TextFormatter is default, you don't actually have to do this.
		log.SetFormatter(&log.TextFormatter{})
	}
}

func main() {
	var checkURL = flag.String("check-url", "", "Curl url and return exit code (http: 200 => 0, otherwise 1)")
	var configFile = flag.String("config-file", "conf/rabbitmq.conf", "path to json config")
	flag.Parse()

	if *checkURL != "" { // do a single http get request. Used in docker healthckecks as curl is not inside the image
		curl(*checkURL)
		return
	}

	err := initConfigFromFile(*configFile)                  //Try parsing config file
	if _, isPathError := err.(*os.PathError); isPathError { // No file => use environment variables
		initConfig()
	} else if err != nil {
		panic(err)
	}

	initLogger()
	initClient()
	exporter := newExporter()
	prometheus.MustRegister(exporter)

	log.WithFields(log.Fields{
		"VERSION":    Version,
		"REVISION":   Revision,
		"BRANCH":     Branch,
		"BUILD_DATE": BuildDate,
		//		"RABBIT_PASSWORD": config.RABBIT_PASSWORD,
	}).Info("Starting RabbitMQ exporter")

	log.WithFields(log.Fields{
		"PUBLISH_ADDR":           config.PublishAddr,
		"PUBLISH_PORT":           config.PublishPort,
		"RABBIT_URL":             config.RabbitURL,
		"RABBIT_USER":            config.RabbitUsername,
		"RABBIT_CONNECTION":      config.RabbitConnection,
		"OUTPUT_FORMAT":          config.OutputFormat,
		"RABBIT_CAPABILITIES":    formatCapabilities(config.RabbitCapabilities),
		"RABBIT_EXPORTERS":       config.EnabledExporters,
		"CAFILE":                 config.CAFile,
		"CERTFILE":               config.CertFile,
		"KEYFILE":                config.KeyFile,
		"SKIPVERIFY":             config.InsecureSkipVerify,
		"EXCLUDE_METRICS":        config.ExcludeMetrics,
		"SKIP_EXCHANGES":         config.SkipExchanges.String(),
		"INCLUDE_EXCHANGES":      config.IncludeExchanges.String(),
		"SKIP_QUEUES":            config.SkipQueues.String(),
		"INCLUDE_QUEUES":         config.IncludeQueues.String(),
		"SKIP_VHOST":             config.SkipVHost.String(),
		"INCLUDE_VHOST":          config.IncludeVHost.String(),
		"RABBIT_TIMEOUT":         config.Timeout,
		"MAX_QUEUES":             config.MaxQueues,
		"RABBIT_SCRAPE_INTERVAL": config.ScrapeInterval,
		//		"RABBIT_PASSWORD": config.RABBIT_PASSWORD,
	}).Info("Active Configuration")

	if config.ScrapeInterval > 0 && config.ScrapeInterval < config.Timeout {
		log.WithFields(log.Fields{
			"RABBIT_SCRAPE_INTERVAL": config.ScrapeInterval,
			"RABBIT_TIMEOUT":         config.Timeout,
		}).Warn("RABBIT_SCRAPE_INTERVAL is lower than RABBIT_TIMEOUT; RabbitMQ scrapes may run longer than the configured minimum interval")
	}

	serverCtx, serverCancel := context.WithCancel(context.Background())
	defer serverCancel()

	handler := http.NewServeMux()
	metricsHandler := promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{DisableCompression: true})
	metricsCache := newMetricsCacheHandler(metricsHandler)
	metricsCache.startBackgroundRefresh(serverCtx)
	handler.Handle("/metrics", metricsCache)
	handler.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<html>
             <head><title>RabbitMQ Exporter</title></head>
             <body>
             <h1>RabbitMQ Exporter</h1>
             <p><a href='/metrics'>Metrics</a></p>
             </body>
             </html>`))
	})
	handler.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if exporter.LastScrapeOK() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusGatewayTimeout)
		}
	})

	server := &http.Server{Addr: config.PublishAddr + ":" + config.PublishPort, Handler: handler}

	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	<-runService()
	log.Info("Shutting down")
	serverCancel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := server.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
	cancel()
}

func getLogLevel() log.Level {
	lvl := strings.ToLower(os.Getenv("LOG_LEVEL"))
	level, err := log.ParseLevel(lvl)
	if err != nil {
		level = defaultLogLevel
	}
	return level
}

func formatCapabilities(caps rabbitCapabilitySet) string {
	var buffer bytes.Buffer
	first := true
	for k := range caps {
		if !first {
			buffer.WriteString(",")
		}
		first = false
		buffer.WriteString(string(k))
	}
	return buffer.String()
}
