// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"errors"
	"flag"
	"fmt"
	"slices"
	"time"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
)

// lane is an in-memory identifier of pending work logically enqueued together. Its value is
// exported as a metric label, so renaming one changes existing metrics.
type lane string

const (
	planLane           lane = "plan"
	compactionLane     lane = "compaction"
	fastCompactionLane lane = "fast-compaction"
	slowCompactionLane lane = "slow-compaction"
)

const (
	lanePolicySimple          = "simple"
	lanePolicyCompactionClass = "compaction-class"
)

type laneTransition struct {
	lane lane
	kind rotationTransition
}

// Defines how to map jobs and requests into lanes
type lanePolicy interface {
	// AllLanes returns every lane this policy defines. The scheduler tracks exactly these, and
	// serves them in this order to a worker that names no job type.
	AllLanes() []lane

	// CompactionLanes returns the lanes carrying compaction jobs.
	CompactionLanes() []lane

	// LaneForJob returns the job's lane. It must always return the same lane for a job for the
	// lifetime of the process, as different callers may re-derive it at different times.
	LaneForJob(TrackedJob) lane

	// LanesForRequest returns the lanes this worker requested.
	LanesForRequest(*compactorschedulerpb.LeaseJobRequest) ([]lane, error)
}

type CompactionClassConfig struct {
	FastMaxDuration     time.Duration `yaml:"fast_max_duration" category:"experimental"`
	OutOfOrderClassFast bool          `yaml:"out_of_order_class_fast" category:"experimental"`
}

func (cfg *CompactionClassConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.DurationVar(&cfg.FastMaxDuration, prefix+".fast-max-duration", 2*time.Hour, "Compaction jobs whose source blocks span at most this duration are classified as fast. Jobs spanning longer are classified as slow.")
	f.BoolVar(&cfg.OutOfOrderClassFast, prefix+".out-of-order-class-fast", true, "Classify out-of-order compaction jobs as fast regardless of the duration they span. Disable if out-of-order jobs for a tenant grow large enough to dominate the fast class.")
}

func (cfg *CompactionClassConfig) Validate(prefix string) error {
	if cfg.FastMaxDuration <= 0 {
		return errors.New(prefix + ".fast-max-duration must be positive")
	}
	return nil
}

// isFastCompaction treats out-of-order jobs as fast because they are recompacted repeatedly, and so
// keep spanning a wide time range while staying small. Such a job is only recognizable while it
// carries the "from-out-of-order" hint, which the compactor drops past the first level, or if the
// tenant has -blocks-storage.tsdb.out-of-order-blocks-external-label-enabled set.
func (cfg CompactionClassConfig) isFastCompaction(j *CompactionJob) bool {
	return (cfg.OutOfOrderClassFast && j.outOfOrder) || j.Duration() <= cfg.FastMaxDuration
}

type LanePolicyConfig struct {
	Policy          string                `yaml:"policy" category:"experimental"`
	CompactionClass CompactionClassConfig `yaml:"compaction_class"`
}

func (cfg *LanePolicyConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Policy, prefix+".policy", lanePolicySimple, "The lane policy the compactor scheduler should use. Valid values: "+lanePolicySimple+", "+lanePolicyCompactionClass)
	cfg.CompactionClass.RegisterFlagsWithPrefix(prefix+".compaction-class", f)
}

func (cfg *LanePolicyConfig) Validate(prefix string) error {
	if _, err := newLanePolicy(*cfg); err != nil {
		return err
	}
	return cfg.CompactionClass.Validate(prefix + ".compaction-class")
}

func newLanePolicy(cfg LanePolicyConfig) (lanePolicy, error) {
	switch cfg.Policy {
	case lanePolicySimple:
		return newSimpleLanePolicy(), nil
	case lanePolicyCompactionClass:
		return newCompactionClassLanePolicy(cfg.CompactionClass), nil
	default:
		return nil, fmt.Errorf("unrecognized lane policy: %s", cfg.Policy)
	}
}

// simpleLanePolicy assigns a lane per job type
type simpleLanePolicy struct {
	allLanes        []lane
	compactionLanes []lane
}

func newSimpleLanePolicy() lanePolicy {
	return &simpleLanePolicy{
		allLanes:        []lane{planLane, compactionLane},
		compactionLanes: []lane{compactionLane},
	}
}

func (slp *simpleLanePolicy) LaneForJob(j TrackedJob) lane {
	if j.ID() == planJobId {
		return planLane
	}
	return compactionLane
}

func (slp *simpleLanePolicy) AllLanes() []lane {
	return slp.allLanes
}

func (slp *simpleLanePolicy) CompactionLanes() []lane {
	return slp.compactionLanes
}

// requestedLanes maps a lease request to scheduler lanes
func (slp *simpleLanePolicy) LanesForRequest(req *compactorschedulerpb.LeaseJobRequest) ([]lane, error) {
	numLanes := len(req.LaneRequests)
	if numLanes == 0 {
		// No lanes supplied, provide a default
		return slp.AllLanes(), nil
	}
	if numLanes > len(slp.allLanes) {
		return nil, fmt.Errorf("at most %d lanes supported, provided %d", len(slp.allLanes), numLanes)
	}

	lanes := make([]lane, 0, numLanes)
	for _, ln := range req.LaneRequests {
		var l lane
		switch ln.JobType {
		case compactorschedulerpb.JOB_TYPE_PLANNING:
			l = planLane
		case compactorschedulerpb.JOB_TYPE_COMPACTION:
			l = compactionLane
		default:
			return nil, fmt.Errorf("unknown job type in lane request: %q", ln.JobType.String())
		}
		if slices.Contains(lanes, l) {
			return nil, fmt.Errorf("duplicate lane in request: %q", ln.JobType.String())
		}
		lanes = append(lanes, l)
	}
	return lanes, nil
}

// compactionClassLanePolicy gives each compaction class its own lane, so that workers can be
// dedicated to one class and sized independently of the other.
//
// Dedicate them: Rotator.LeaseJob walks lanes in the requested order and only round-robins tenants
// within a lane, so a worker asking for both classes drains fast work across every tenant before any
// tenant's slow work. Under simpleLanePolicy the tenant rotation is the outermost fairness rule, so
// this is a weaker guarantee: one tenant's fast backlog can starve another tenant's slow work.
type compactionClassLanePolicy struct {
	allLanes        []lane
	compactionLanes []lane
	classCfg        CompactionClassConfig
}

func newCompactionClassLanePolicy(classCfg CompactionClassConfig) lanePolicy {
	return &compactionClassLanePolicy{
		allLanes:        []lane{planLane, fastCompactionLane, slowCompactionLane},
		compactionLanes: []lane{fastCompactionLane, slowCompactionLane},
		classCfg:        classCfg,
	}
}

func (clp *compactionClassLanePolicy) LaneForJob(j TrackedJob) lane {
	if j.ID() == planJobId {
		return planLane
	}
	// Every non-plan job is a compaction job; anything else is a programming error.
	cj := j.(*TrackedCompactionJob)
	if clp.classCfg.isFastCompaction(cj.value) {
		return fastCompactionLane
	}
	return slowCompactionLane
}

func (clp *compactionClassLanePolicy) AllLanes() []lane {
	return clp.allLanes
}

func (clp *compactionClassLanePolicy) CompactionLanes() []lane {
	return clp.compactionLanes
}

// LanesForRequest serves both class lanes to a compaction request that names no class.
func (clp *compactionClassLanePolicy) LanesForRequest(req *compactorschedulerpb.LeaseJobRequest) ([]lane, error) {
	numLanes := len(req.LaneRequests)
	if numLanes == 0 {
		// No lanes supplied, provide a default
		return clp.AllLanes(), nil
	}
	if numLanes > len(clp.allLanes) {
		return nil, fmt.Errorf("at most %d lanes supported, provided %d", len(clp.allLanes), numLanes)
	}

	lanes := make([]lane, 0, len(clp.allLanes))
	for _, ln := range req.LaneRequests {
		var requested []lane
		switch ln.JobType {
		case compactorschedulerpb.JOB_TYPE_PLANNING:
			requested = []lane{planLane}
		case compactorschedulerpb.JOB_TYPE_COMPACTION:
			switch ln.CompactionClass {
			case compactorschedulerpb.COMPACTION_CLASS_FAST:
				requested = []lane{fastCompactionLane}
			case compactorschedulerpb.COMPACTION_CLASS_SLOW:
				requested = []lane{slowCompactionLane}
			default:
				requested = clp.compactionLanes
			}
		default:
			return nil, fmt.Errorf("unknown job type in lane request: %q", ln.JobType.String())
		}
		for _, l := range requested {
			if slices.Contains(lanes, l) {
				return nil, fmt.Errorf("duplicate lane in request: %q", l)
			}
			lanes = append(lanes, l)
		}
	}
	return lanes, nil
}
