// Package rcserver implements the HTTP endpoint to serve the remote control
package rcserver

import (
	"context"
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/fs/rc/jobs"
	"github.com/rclone/rclone/fs/resume"
	libhttp "github.com/rclone/rclone/lib/http"
)

const path = "/metrics"

var promHandlerFunc http.HandlerFunc

func init() {
	rcloneCollector := accounting.NewRcloneCollector(context.Background())
	prometheus.MustRegister(rcloneCollector)
	prometheus.MustRegister(newResumeCollector())

	m := fshttp.NewMetrics("rclone")
	for _, c := range m.Collectors() {
		prometheus.MustRegister(c)
	}
	fshttp.DefaultMetrics = m

	promHandlerFunc = promhttp.Handler().ServeHTTP
}

type resumeCollector struct {
	saveScanTotal                   *prometheus.Desc
	saveScanSecondsTotal            *prometheus.Desc
	commitSuccessBatchTotal         *prometheus.Desc
	commitSuccessBatchItemsTotal    *prometheus.Desc
	commitSuccessBatchSecondsTotal  *prometheus.Desc
	objectFrontierCommitTotal       *prometheus.Desc
	objectFrontierCommittedSegments *prometheus.Desc
	objectFrontierBlockedTotal      *prometheus.Desc
	objectFrontierInflight          *prometheus.Desc
	objectFrontierPending           *prometheus.Desc
	objectSegmentsCreatedTotal      *prometheus.Desc
	objectSegmentObjectsTotal       *prometheus.Desc
	objectSegmentBytesTotal         *prometheus.Desc
	fileFrontierCommitTotal         *prometheus.Desc
	fileFrontierCommittedTasks      *prometheus.Desc
	fileFrontierBlockedTotal        *prometheus.Desc
	fileFrontierInflight            *prometheus.Desc
	fileFrontierPending             *prometheus.Desc
	fileScanPagesTotal              *prometheus.Desc
	fileScanDirectoriesTotal        *prometheus.Desc
	fileTasksCreatedTotal           *prometheus.Desc
	fileTaskFilesTotal              *prometheus.Desc
	fileTaskBytesTotal              *prometheus.Desc
}

func newResumeCollector() *resumeCollector {
	return &resumeCollector{
		saveScanTotal: prometheus.NewDesc("rclone_resume_save_scan_total",
			"Number of persisted resume scan cursor writes", nil, nil),
		saveScanSecondsTotal: prometheus.NewDesc("rclone_resume_save_scan_seconds_total",
			"Cumulative time spent persisting resume scan cursor state", nil, nil),
		commitSuccessBatchTotal: prometheus.NewDesc("rclone_resume_commit_success_batch_total",
			"Number of successful resume batch commits", nil, nil),
		commitSuccessBatchItemsTotal: prometheus.NewDesc("rclone_resume_commit_success_batch_items_total",
			"Number of completed items persisted via resume batch commits", nil, nil),
		commitSuccessBatchSecondsTotal: prometheus.NewDesc("rclone_resume_commit_success_batch_seconds_total",
			"Cumulative time spent in resume batch success commits", nil, nil),
		objectFrontierCommitTotal: prometheus.NewDesc("rclone_resume_object_frontier_commit_total",
			"Number of object frontier commit operations that advanced at least one segment", nil, nil),
		objectFrontierCommittedSegments: prometheus.NewDesc("rclone_resume_object_frontier_committed_segments_total",
			"Number of object resume segments committed through the frontier", nil, nil),
		objectFrontierBlockedTotal: prometheus.NewDesc("rclone_resume_object_frontier_blocked_total",
			"Number of times object frontier commit processing was blocked by a failed segment", nil, nil),
		objectFrontierInflight: prometheus.NewDesc("rclone_resume_object_frontier_inflight",
			"Current number of in-flight object resume segments tracked by the frontier", nil, nil),
		objectFrontierPending: prometheus.NewDesc("rclone_resume_object_frontier_pending",
			"Current number of pending completed object resume segments waiting on the frontier", nil, nil),
		objectSegmentsCreatedTotal: prometheus.NewDesc("rclone_resume_object_segments_created_total",
			"Number of object resume segments created", nil, nil),
		objectSegmentObjectsTotal: prometheus.NewDesc("rclone_resume_object_segment_objects_total",
			"Number of objects assigned to created object resume segments", nil, nil),
		objectSegmentBytesTotal: prometheus.NewDesc("rclone_resume_object_segment_bytes_total",
			"Number of bytes assigned to created object resume segments", nil, nil),
		fileFrontierCommitTotal: prometheus.NewDesc("rclone_resume_file_frontier_commit_total",
			"Number of file frontier commit operations that advanced at least one task", nil, nil),
		fileFrontierCommittedTasks: prometheus.NewDesc("rclone_resume_file_frontier_committed_tasks_total",
			"Number of file resume tasks committed through the frontier", nil, nil),
		fileFrontierBlockedTotal: prometheus.NewDesc("rclone_resume_file_frontier_blocked_total",
			"Number of times file frontier commit processing was blocked by a failed task", nil, nil),
		fileFrontierInflight: prometheus.NewDesc("rclone_resume_file_frontier_inflight",
			"Current number of in-flight file resume tasks tracked by the frontier", nil, nil),
		fileFrontierPending: prometheus.NewDesc("rclone_resume_file_frontier_pending",
			"Current number of pending completed file resume tasks waiting on the frontier", nil, nil),
		fileScanPagesTotal: prometheus.NewDesc("rclone_resume_file_scan_pages_total",
			"Number of file resume scan pages processed", nil, nil),
		fileScanDirectoriesTotal: prometheus.NewDesc("rclone_resume_file_scan_directories_total",
			"Number of file resume directory descents", nil, nil),
		fileTasksCreatedTotal: prometheus.NewDesc("rclone_resume_file_tasks_created_total",
			"Number of file resume tasks created", nil, nil),
		fileTaskFilesTotal: prometheus.NewDesc("rclone_resume_file_task_files_total",
			"Number of files assigned to created file resume tasks", nil, nil),
		fileTaskBytesTotal: prometheus.NewDesc("rclone_resume_file_task_bytes_total",
			"Number of bytes assigned to created file resume tasks", nil, nil),
	}
}

func (c *resumeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.saveScanTotal
	ch <- c.saveScanSecondsTotal
	ch <- c.commitSuccessBatchTotal
	ch <- c.commitSuccessBatchItemsTotal
	ch <- c.commitSuccessBatchSecondsTotal
	ch <- c.objectFrontierCommitTotal
	ch <- c.objectFrontierCommittedSegments
	ch <- c.objectFrontierBlockedTotal
	ch <- c.objectFrontierInflight
	ch <- c.objectFrontierPending
	ch <- c.objectSegmentsCreatedTotal
	ch <- c.objectSegmentObjectsTotal
	ch <- c.objectSegmentBytesTotal
	ch <- c.fileFrontierCommitTotal
	ch <- c.fileFrontierCommittedTasks
	ch <- c.fileFrontierBlockedTotal
	ch <- c.fileFrontierInflight
	ch <- c.fileFrontierPending
	ch <- c.fileScanPagesTotal
	ch <- c.fileScanDirectoriesTotal
	ch <- c.fileTasksCreatedTotal
	ch <- c.fileTaskFilesTotal
	ch <- c.fileTaskBytesTotal
}

func (c *resumeCollector) Collect(ch chan<- prometheus.Metric) {
	m := resume.Metrics()
	ch <- prometheus.MustNewConstMetric(c.saveScanTotal, prometheus.CounterValue, float64(m.SaveScanTotal))
	ch <- prometheus.MustNewConstMetric(c.saveScanSecondsTotal, prometheus.CounterValue, m.SaveScanSecondsTotal)
	ch <- prometheus.MustNewConstMetric(c.commitSuccessBatchTotal, prometheus.CounterValue, float64(m.CommitSuccessBatchTotal))
	ch <- prometheus.MustNewConstMetric(c.commitSuccessBatchItemsTotal, prometheus.CounterValue, float64(m.CommitSuccessBatchItemsTotal))
	ch <- prometheus.MustNewConstMetric(c.commitSuccessBatchSecondsTotal, prometheus.CounterValue, m.CommitSuccessBatchSecondsTotal)
	ch <- prometheus.MustNewConstMetric(c.objectFrontierCommitTotal, prometheus.CounterValue, float64(m.ObjectFrontierCommitTotal))
	ch <- prometheus.MustNewConstMetric(c.objectFrontierCommittedSegments, prometheus.CounterValue, float64(m.ObjectFrontierCommittedSegments))
	ch <- prometheus.MustNewConstMetric(c.objectFrontierBlockedTotal, prometheus.CounterValue, float64(m.ObjectFrontierBlockedTotal))
	ch <- prometheus.MustNewConstMetric(c.objectFrontierInflight, prometheus.GaugeValue, float64(m.ObjectFrontierInflight))
	ch <- prometheus.MustNewConstMetric(c.objectFrontierPending, prometheus.GaugeValue, float64(m.ObjectFrontierPending))
	ch <- prometheus.MustNewConstMetric(c.objectSegmentsCreatedTotal, prometheus.CounterValue, float64(m.ObjectSegmentsCreatedTotal))
	ch <- prometheus.MustNewConstMetric(c.objectSegmentObjectsTotal, prometheus.CounterValue, float64(m.ObjectSegmentObjectsTotal))
	ch <- prometheus.MustNewConstMetric(c.objectSegmentBytesTotal, prometheus.CounterValue, float64(m.ObjectSegmentBytesTotal))
	ch <- prometheus.MustNewConstMetric(c.fileFrontierCommitTotal, prometheus.CounterValue, float64(m.FileFrontierCommitTotal))
	ch <- prometheus.MustNewConstMetric(c.fileFrontierCommittedTasks, prometheus.CounterValue, float64(m.FileFrontierCommittedTasks))
	ch <- prometheus.MustNewConstMetric(c.fileFrontierBlockedTotal, prometheus.CounterValue, float64(m.FileFrontierBlockedTotal))
	ch <- prometheus.MustNewConstMetric(c.fileFrontierInflight, prometheus.GaugeValue, float64(m.FileFrontierInflight))
	ch <- prometheus.MustNewConstMetric(c.fileFrontierPending, prometheus.GaugeValue, float64(m.FileFrontierPending))
	ch <- prometheus.MustNewConstMetric(c.fileScanPagesTotal, prometheus.CounterValue, float64(m.FileScanPagesTotal))
	ch <- prometheus.MustNewConstMetric(c.fileScanDirectoriesTotal, prometheus.CounterValue, float64(m.FileScanDirectoriesTotal))
	ch <- prometheus.MustNewConstMetric(c.fileTasksCreatedTotal, prometheus.CounterValue, float64(m.FileTasksCreatedTotal))
	ch <- prometheus.MustNewConstMetric(c.fileTaskFilesTotal, prometheus.CounterValue, float64(m.FileTaskFilesTotal))
	ch <- prometheus.MustNewConstMetric(c.fileTaskBytesTotal, prometheus.CounterValue, float64(m.FileTaskBytesTotal))
}

// MetricsStart the remote control server if configured
//
// If the server wasn't configured the *Server returned may be nil
func MetricsStart(ctx context.Context, opt *rc.Options) (*MetricsServer, error) {
	jobs.SetOpt(opt) // set the defaults for jobs
	if len(opt.MetricsHTTP.ListenAddr) > 0 {
		// Serve on the DefaultServeMux so can have global registrations appear
		s, err := newMetricsServer(ctx, opt)
		if err != nil {
			return nil, err
		}
		return s, s.Serve()
	}
	return nil, nil
}

// MetricsServer contains everything to run the rc server
type MetricsServer struct {
	ctx             context.Context // for global config
	server          *libhttp.Server
	promHandlerFunc http.Handler
	opt             *rc.Options
}

func newMetricsServer(ctx context.Context, opt *rc.Options) (*MetricsServer, error) {
	s := &MetricsServer{
		ctx:             ctx,
		opt:             opt,
		promHandlerFunc: promHandlerFunc,
	}

	var err error
	s.server, err = libhttp.NewServer(ctx,
		libhttp.WithConfig(opt.MetricsHTTP),
		libhttp.WithAuth(opt.MetricsAuth),
		libhttp.WithTemplate(opt.MetricsTemplate),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to init server: %w", err)
	}

	router := s.server.Router()
	router.Get(path, promHandlerFunc)
	return s, nil
}

// Serve runs the http server in the background.
//
// Use s.Close() and s.Wait() to shutdown server
func (s *MetricsServer) Serve() error {
	s.server.Serve()
	return nil
}

// Wait blocks while the server is serving requests
func (s *MetricsServer) Wait() {
	s.server.Wait()
}

// Shutdown gracefully shuts down the server
func (s *MetricsServer) Shutdown() error {
	return s.server.Shutdown()
}
