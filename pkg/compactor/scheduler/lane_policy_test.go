// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
)

func testCompactionClassConfig() CompactionClassConfig {
	var cfg CompactionClassConfig
	cfg.RegisterFlagsWithPrefix("test", flag.NewFlagSet("test", flag.ContinueOnError))
	return cfg
}

func TestCompactionClassConfig_IsFastCompaction(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)

	tests := map[string]struct {
		cfg          CompactionClassConfig
		job          *CompactionJob
		expectedFast bool
	}{
		"in-order job within the fast duration": {
			cfg:          CompactionClassConfig{FastMaxDuration: 2 * time.Hour, OutOfOrderClassFast: true},
			job:          &CompactionJob{minTime: 0, maxTime: hour},
			expectedFast: true,
		},
		"in-order job exactly at the fast duration": {
			cfg:          CompactionClassConfig{FastMaxDuration: 2 * time.Hour, OutOfOrderClassFast: true},
			job:          &CompactionJob{minTime: 0, maxTime: 2 * hour},
			expectedFast: true,
		},
		"in-order job beyond the fast duration": {
			cfg:          CompactionClassConfig{FastMaxDuration: 2 * time.Hour, OutOfOrderClassFast: true},
			job:          &CompactionJob{minTime: 0, maxTime: 2*hour + 1},
			expectedFast: false,
		},
		"in-order 24h job": {
			cfg:          CompactionClassConfig{FastMaxDuration: 2 * time.Hour, OutOfOrderClassFast: true},
			job:          &CompactionJob{minTime: 0, maxTime: 24 * hour},
			expectedFast: false,
		},
		"out-of-order 24h job": {
			cfg:          CompactionClassConfig{FastMaxDuration: 2 * time.Hour, OutOfOrderClassFast: true},
			job:          &CompactionJob{minTime: 0, maxTime: 24 * hour, outOfOrder: true},
			expectedFast: true,
		},
		"out-of-order 24h job with out-of-order fast classification disabled": {
			cfg:          CompactionClassConfig{FastMaxDuration: 2 * time.Hour, OutOfOrderClassFast: false},
			job:          &CompactionJob{minTime: 0, maxTime: 24 * hour, outOfOrder: true},
			expectedFast: false,
		},
		"out-of-order short job with out-of-order fast classification disabled": {
			cfg:          CompactionClassConfig{FastMaxDuration: 2 * time.Hour, OutOfOrderClassFast: false},
			job:          &CompactionJob{minTime: 0, maxTime: hour, outOfOrder: true},
			expectedFast: true,
		},
		// Timestamps arrive from a worker, so an inverted range must not overflow into a huge duration.
		"inverted range is treated as spanning nothing": {
			cfg:          CompactionClassConfig{FastMaxDuration: 2 * time.Hour, OutOfOrderClassFast: false},
			job:          &CompactionJob{minTime: 24 * hour, maxTime: 0},
			expectedFast: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expectedFast, tc.cfg.isFastCompaction(tc.job))
		})
	}
}

func TestCompactionClassLanePolicy_LaneForJob(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	policy := newCompactionClassLanePolicy(testCompactionClassConfig())

	newJob := func(job *CompactionJob) TrackedJob {
		return NewTrackedCompactionJob("id", job, 1, time.Now())
	}

	require.Equal(t, planLane, policy.LaneForJob(NewTrackedPlanJob(time.Now())))
	require.Equal(t, fastCompactionLane, policy.LaneForJob(newJob(&CompactionJob{maxTime: hour})))
	require.Equal(t, slowCompactionLane, policy.LaneForJob(newJob(&CompactionJob{maxTime: 24 * hour})))
	require.Equal(t, fastCompactionLane, policy.LaneForJob(newJob(&CompactionJob{maxTime: 24 * hour, outOfOrder: true})))
}

func TestLanePolicy_AllLanesCoversEveryJobType(t *testing.T) {
	for name, policy := range map[string]lanePolicy{
		lanePolicySimple:          newSimpleLanePolicy(),
		lanePolicyCompactionClass: newCompactionClassLanePolicy(testCompactionClassConfig()),
	} {
		t.Run(name, func(t *testing.T) {
			// Rotator and JobTracker only track lanes reported by AllLanes, so any lane a lease
			// request can return must appear there.
			for _, jobType := range []compactorschedulerpb.JobType{
				compactorschedulerpb.JOB_TYPE_PLANNING,
				compactorschedulerpb.JOB_TYPE_COMPACTION,
			} {
				lanes, err := policy.LanesForRequest(&compactorschedulerpb.LeaseJobRequest{
					LaneRequests: []*compactorschedulerpb.LaneRequest{{JobType: jobType}},
				})
				require.NoError(t, err)
				for _, l := range lanes {
					require.Contains(t, policy.AllLanes(), l, "lane %q served for %s is not in AllLanes", l, jobType)
				}
			}
			// Plan jobs carry no bytes, so tracking the plan lane would only export zeroes.
			require.NotContains(t, policy.CompactionLanes(), planLane)
		})
	}
}

func TestLanesForRequest(t *testing.T) {
	laneRequests := func(types ...compactorschedulerpb.JobType) *compactorschedulerpb.LeaseJobRequest {
		req := &compactorschedulerpb.LeaseJobRequest{}
		for _, jt := range types {
			req.LaneRequests = append(req.LaneRequests, &compactorschedulerpb.LaneRequest{JobType: jt})
		}
		return req
	}
	compactionClass := func(class compactorschedulerpb.CompactionClass) *compactorschedulerpb.LeaseJobRequest {
		return &compactorschedulerpb.LeaseJobRequest{
			LaneRequests: []*compactorschedulerpb.LaneRequest{
				{JobType: compactorschedulerpb.JOB_TYPE_COMPACTION, CompactionClass: class},
			},
		}
	}

	tests := map[string]struct {
		policy      lanePolicy
		req         *compactorschedulerpb.LeaseJobRequest
		expected    []lane
		expectedErr string
	}{
		"simple: no lane requests is served every lane": {
			policy:   newSimpleLanePolicy(),
			req:      laneRequests(),
			expected: []lane{planLane, compactionLane},
		},
		"simple: compaction then planning preserves request order": {
			policy:   newSimpleLanePolicy(),
			req:      laneRequests(compactorschedulerpb.JOB_TYPE_COMPACTION, compactorschedulerpb.JOB_TYPE_PLANNING),
			expected: []lane{compactionLane, planLane},
		},
		"compaction class: no lane requests is served every lane": {
			policy:   newCompactionClassLanePolicy(testCompactionClassConfig()),
			req:      laneRequests(),
			expected: []lane{planLane, fastCompactionLane, slowCompactionLane},
		},
		"compaction class: compaction expands to both class lanes": {
			policy:   newCompactionClassLanePolicy(testCompactionClassConfig()),
			req:      laneRequests(compactorschedulerpb.JOB_TYPE_COMPACTION),
			expected: []lane{fastCompactionLane, slowCompactionLane},
		},
		"compaction class: planning only": {
			policy:   newCompactionClassLanePolicy(testCompactionClassConfig()),
			req:      laneRequests(compactorschedulerpb.JOB_TYPE_PLANNING),
			expected: []lane{planLane},
		},
		"compaction class: compaction then planning preserves request order": {
			policy:   newCompactionClassLanePolicy(testCompactionClassConfig()),
			req:      laneRequests(compactorschedulerpb.JOB_TYPE_COMPACTION, compactorschedulerpb.JOB_TYPE_PLANNING),
			expected: []lane{fastCompactionLane, slowCompactionLane, planLane},
		},
		"compaction class: fast only": {
			policy:   newCompactionClassLanePolicy(testCompactionClassConfig()),
			req:      compactionClass(compactorschedulerpb.COMPACTION_CLASS_FAST),
			expected: []lane{fastCompactionLane},
		},
		"compaction class: slow only": {
			policy:   newCompactionClassLanePolicy(testCompactionClassConfig()),
			req:      compactionClass(compactorschedulerpb.COMPACTION_CLASS_SLOW),
			expected: []lane{slowCompactionLane},
		},
		// The simple policy has one compaction lane, so a class request collapses onto it. This
		// keeps worker configuration valid whichever policy the scheduler runs.
		"simple: class request collapses to the single compaction lane": {
			policy:   newSimpleLanePolicy(),
			req:      compactionClass(compactorschedulerpb.COMPACTION_CLASS_SLOW),
			expected: []lane{compactionLane},
		},
		// A class is only meaningful for compaction, so naming one elsewhere is ignored.
		"compaction class: planning narrowed to a class is served the plan lane": {
			policy: newCompactionClassLanePolicy(testCompactionClassConfig()),
			req: &compactorschedulerpb.LeaseJobRequest{
				LaneRequests: []*compactorschedulerpb.LaneRequest{
					{JobType: compactorschedulerpb.JOB_TYPE_PLANNING, CompactionClass: compactorschedulerpb.COMPACTION_CLASS_FAST},
				},
			},
			expected: []lane{planLane},
		},
		"compaction class: a class already served by an unnarrowed request is rejected": {
			policy: newCompactionClassLanePolicy(testCompactionClassConfig()),
			req: &compactorschedulerpb.LeaseJobRequest{
				LaneRequests: []*compactorschedulerpb.LaneRequest{
					{JobType: compactorschedulerpb.JOB_TYPE_COMPACTION},
					{JobType: compactorschedulerpb.JOB_TYPE_COMPACTION, CompactionClass: compactorschedulerpb.COMPACTION_CLASS_SLOW},
				},
			},
			expectedErr: `duplicate lane in request: "slow-compaction"`,
		},
		"compaction class: unknown job type is rejected": {
			policy:      newCompactionClassLanePolicy(testCompactionClassConfig()),
			req:         laneRequests(compactorschedulerpb.JOB_TYPE_UNKNOWN),
			expectedErr: `unknown job type in lane request: "JOB_TYPE_UNKNOWN"`,
		},
		"compaction class: more requests than lanes is rejected": {
			policy: newCompactionClassLanePolicy(testCompactionClassConfig()),
			req: laneRequests(
				compactorschedulerpb.JOB_TYPE_PLANNING,
				compactorschedulerpb.JOB_TYPE_COMPACTION,
				compactorschedulerpb.JOB_TYPE_PLANNING,
				compactorschedulerpb.JOB_TYPE_COMPACTION,
			),
			expectedErr: "at most 3 lanes supported, provided 4",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			lanes, err := tc.policy.LanesForRequest(tc.req)
			if tc.expectedErr != "" {
				require.EqualError(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, lanes)
		})
	}
}

func TestNewLanePolicy(t *testing.T) {
	cfg := LanePolicyConfig{CompactionClass: testCompactionClassConfig()}

	cfg.Policy = lanePolicySimple
	policy, err := newLanePolicy(cfg)
	require.NoError(t, err)
	require.IsType(t, &simpleLanePolicy{}, policy)

	cfg.Policy = lanePolicyCompactionClass
	policy, err = newLanePolicy(cfg)
	require.NoError(t, err)
	require.IsType(t, &compactionClassLanePolicy{}, policy)

	cfg.Policy = "nope"
	_, err = newLanePolicy(cfg)
	require.EqualError(t, err, "unrecognized lane policy: nope")
}

func TestCompactionClassConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		duration time.Duration
		expected string
	}{
		"positive duration": {duration: 2 * time.Hour},
		"zero duration":     {duration: 0, expected: "test.fast-max-duration must be positive"},
		"negative duration": {duration: -time.Hour, expected: "test.fast-max-duration must be positive"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := CompactionClassConfig{FastMaxDuration: tc.duration}
			err := cfg.Validate("test")
			if tc.expected == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expected)
			}
		})
	}
}
