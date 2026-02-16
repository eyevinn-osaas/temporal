package advancetimepoint

import (
	"context"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"

	historyservice "go.temporal.io/server/api/historyservice/v1"
)

// timePointKind indicates what type of time point was found.
type timePointKind int

const (
	timePointKindNone timePointKind = iota
	timePointKindUserTimer
	timePointKindActivityTimeout
	timePointKindWorkflowRunTimeout
	timePointKindWorkflowExecutionTimeout
)

// timePoint represents the next schedulable time point.
type timePoint struct {
	kind       timePointKind
	fireTime   time.Time
	timerSeqID workflow.TimerSequenceID // for user timers and activity timeouts
}

func Invoke(
	ctx context.Context,
	request *historyservice.AdvanceWorkflowExecutionTimePointRequest,
	shardCtx historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (*historyservice.AdvanceWorkflowExecutionTimePointResponse, error) {
	advanceReq := request.GetAdvanceRequest()
	if _, err := api.GetActiveNamespace(shardCtx, namespace.ID(request.GetNamespaceId()), advanceReq.GetWorkflowExecution().GetWorkflowId()); err != nil {
		return nil, err
	}

	resp := &historyservice.AdvanceWorkflowExecutionTimePointResponse{}

	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			request.GetNamespaceId(),
			advanceReq.GetWorkflowExecution().GetWorkflowId(),
			advanceReq.GetWorkflowExecution().GetRunId(),
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			executionInfo := mutableState.GetExecutionInfo()

			// Verify time-skipping is enabled for this workflow.
			if executionInfo.GetTimeSkippingConfig() == nil {
				return nil, serviceerror.NewFailedPrecondition("time-skipping is not enabled for this workflow execution")
			}

			// Find the next time point.
			tp := findNextTimePoint(mutableState)
			if tp.kind == timePointKindNone {
				resp.NoUpcomingTimePoint = true
				return &api.UpdateWorkflowAction{Noop: true, CreateWorkflowTask: false}, nil
			}

			// Record the TIME_POINT_ADVANCED event before firing.
			if _, err := mutableState.AddWorkflowExecutionTimePointAdvancedEvent(
				tp.fireTime,
				advanceReq.GetIdentity(),
				advanceReq.GetRequestId(),
			); err != nil {
				return nil, err
			}

			// Fire the time point.
			if err := fireTimePoint(mutableState, tp); err != nil {
				return nil, err
			}

			resp.VirtualTimeOffset = executionInfo.GetVirtualTimeOffset()

			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: true,
			}, nil
		},
		nil,
		shardCtx,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// findNextTimePoint does an O(n) scan over user timers, activity timeouts
// (excluding heartbeat), and workflow timeouts to find the single earliest
// upcoming time point. It avoids sorting by just tracking the minimum.
//
// NOTE: Nexus operation timeouts and other CHASM/HSM state machine timers are
// not yet included; they will be added in a follow-up.
func findNextTimePoint(mutableState historyi.MutableState) timePoint {
	var best timePoint

	consider := func(kind timePointKind, t time.Time, seqID workflow.TimerSequenceID) {
		if t.IsZero() {
			return
		}
		if best.kind == timePointKindNone || t.Before(best.fireTime) {
			best = timePoint{kind: kind, fireTime: t, timerSeqID: seqID}
		}
	}

	// User timers: just scan for the earliest ExpiryTime.
	for _, timerInfo := range mutableState.GetPendingTimerInfos() {
		expiryTime := timestamp.TimeValue(timerInfo.ExpiryTime)
		consider(timePointKindUserTimer, expiryTime, workflow.TimerSequenceID{
			EventID:   timerInfo.GetStartedEventId(),
			Timestamp: expiryTime,
			TimerType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			Attempt:   1,
		})
	}

	// Activity timeouts: compute each timeout and track the minimum.
	// Excludes heartbeat timeouts (those are "task timeouts", not time points).
	for _, ai := range mutableState.GetPendingActivityInfos() {
		if ai.Paused {
			continue
		}
		considerActivityTimeouts(ai, consider)
	}

	// Workflow run timeout.
	execInfo := mutableState.GetExecutionInfo()
	consider(timePointKindWorkflowRunTimeout, timestamp.TimeValue(execInfo.WorkflowRunExpirationTime), workflow.TimerSequenceID{})

	// Workflow execution timeout.
	consider(timePointKindWorkflowExecutionTimeout, timestamp.TimeValue(execInfo.WorkflowExecutionExpirationTime), workflow.TimerSequenceID{})

	return best
}

// considerActivityTimeouts computes the non-heartbeat timeouts for a single
// activity and calls consider for each.
func considerActivityTimeouts(
	ai *persistencespb.ActivityInfo,
	consider func(timePointKind, time.Time, workflow.TimerSequenceID),
) {
	if ai.ScheduledEventId == common.EmptyEventID {
		return
	}

	makeSeqID := func(t time.Time, timerType enumspb.TimeoutType) workflow.TimerSequenceID {
		return workflow.TimerSequenceID{
			EventID:   ai.ScheduledEventId,
			Timestamp: t,
			TimerType: timerType,
			Attempt:   ai.Attempt,
		}
	}

	// Schedule-to-start: only if not yet started.
	if ai.StartedEventId == common.EmptyEventID {
		if d := timestamp.DurationValue(ai.ScheduleToStartTimeout); d > 0 {
			t := timestamp.TimeValue(ai.ScheduledTime).Add(d)
			consider(timePointKindActivityTimeout, t, makeSeqID(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START))
		}
	}

	// Schedule-to-close.
	if d := timestamp.DurationValue(ai.ScheduleToCloseTimeout); d > 0 {
		base := ai.FirstScheduledTime
		if base == nil {
			base = ai.ScheduledTime
		}
		t := timestamp.TimeValue(base).Add(d)
		consider(timePointKindActivityTimeout, t, makeSeqID(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE))
	}

	// Start-to-close: only if started.
	if ai.StartedEventId != common.EmptyEventID {
		if d := timestamp.DurationValue(ai.StartToCloseTimeout); d > 0 {
			t := timestamp.TimeValue(ai.StartedTime).Add(d)
			consider(timePointKindActivityTimeout, t, makeSeqID(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE))
		}
	}
}

// fireTimePoint fires the given time point by calling the appropriate mutable state method.
func fireTimePoint(mutableState historyi.MutableState, tp timePoint) error {
	switch tp.kind {
	case timePointKindUserTimer:
		return fireUserTimer(mutableState, tp.timerSeqID)
	case timePointKindActivityTimeout:
		return fireActivityTimeout(mutableState, tp.timerSeqID)
	case timePointKindWorkflowRunTimeout, timePointKindWorkflowExecutionTimeout:
		return fireWorkflowTimeout(mutableState)
	default:
		return serviceerror.NewInternal("unknown time point kind")
	}
}

func fireUserTimer(mutableState historyi.MutableState, seqID workflow.TimerSequenceID) error {
	timerInfo, ok := mutableState.GetUserTimerInfoByEventID(seqID.EventID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("user timer not found for event ID %d", seqID.EventID))
	}
	_, err := mutableState.AddTimerFiredEvent(timerInfo.TimerId)
	return err
}

func fireActivityTimeout(mutableState historyi.MutableState, seqID workflow.TimerSequenceID) error {
	ai, ok := mutableState.GetActivityInfo(seqID.EventID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("activity not found for event ID %d", seqID.EventID))
	}

	failureMsg := fmt.Sprintf(common.FailureReasonActivityTimeout, seqID.TimerType.String())
	timeoutFailure := failure.NewTimeoutFailure(failureMsg, seqID.TimerType)
	retryState, err := mutableState.RetryActivity(ai, timeoutFailure)
	if err != nil {
		return err
	}

	if retryState == enumspb.RETRY_STATE_IN_PROGRESS {
		// Activity will be retried; no timeout event needed.
		return nil
	}

	// Convert timeout type for historical consistency.
	if retryState == enumspb.RETRY_STATE_TIMEOUT && seqID.TimerType != enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START {
		timeoutFailure = failure.NewTimeoutFailure(
			"Not enough time to schedule next retry before activity ScheduleToClose timeout, giving up retrying",
			enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		)
	}

	timeoutFailure.GetTimeoutFailureInfo().LastHeartbeatDetails = ai.LastHeartbeatDetails

	_, err = mutableState.AddActivityTaskTimedOutEvent(
		ai.ScheduledEventId,
		ai.StartedEventId,
		timeoutFailure,
		retryState,
	)
	return err
}

func fireWorkflowTimeout(mutableState historyi.MutableState) error {
	// For workflow timeouts, we use RETRY_STATE_TIMEOUT and no new execution.
	_, err := mutableState.AddTimeoutWorkflowEvent(
		mutableState.GetNextEventID(),
		enumspb.RETRY_STATE_TIMEOUT,
		"",
	)
	return err
}
