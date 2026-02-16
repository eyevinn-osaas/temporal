package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type TimeSkippingTestSuite struct {
	testcore.FunctionalTestBase
}

// timeSkippingTaskProcessingDelta is the maximum expected wall-clock time
// between related events in a test (e.g., creating a timer and advancing past
// it). Every use of this constant means "the only error source here is real
// elapsed time during test execution."
const timeSkippingTaskProcessingDelta = 2 * time.Second

func TestTimeSkippingTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(TimeSkippingTestSuite))
}

func (s *TimeSkippingTestSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
	s.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
}

func (s *TimeSkippingTestSuite) TestAdvanceTimePoint_SingleTimer() {
	tq := "ts-tq-" + uuid.NewString()
	identity := "worker1"

	we := s.startWorkflowWithTimeSkipping(tq)

	timerScheduled := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !timerScheduled {
			timerScheduled = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,
				Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
					TimerId:            "timer-1",
					StartToFireTimeout: durationpb.New(1 * time.Hour),
				}},
			}}, nil
		}
		// Timer was fired by time-skipping advance; complete the workflow.
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("done"),
			}},
		}}, nil
	}

	poller := s.newPoller(tq, identity, wtHandler)

	// Schedule the timer.
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Get history to extract exact event times for subsequent assertions.
	// After the first workflow task, history is: Started, TaskScheduled, TaskStarted, TaskCompleted, TimerStarted.
	earlyHistory := s.GetHistory(s.Namespace().String(), we)
	s.GreaterOrEqual(len(earlyHistory), 5)

	startEventTime := earlyHistory[0].GetEventTime()                      // WorkflowExecutionStarted
	firstTaskStartedTime := earlyHistory[2].GetEventTime()                // WorkflowTaskStarted (3)
	firstTaskCompletedTime := earlyHistory[3].GetEventTime()              // WorkflowTaskCompleted (4)
	timerStartedTime := earlyHistory[4].GetEventTime()                    // TimerStarted (5)
	expectedTimerFireTime := timerStartedTime.AsTime().Add(1 * time.Hour) // ExpiryTime = TimerStarted.EventTime + duration

	// Describe before advance: verify time-skipping info and upcoming time points.
	descBefore := s.describeWorkflow(we)

	tsInfo := descBefore.GetTimeSkippingInfo()
	s.NotNil(tsInfo)
	s.True(tsInfo.GetConfig().GetEnabled())
	// Initial offset is zero (no advances yet).
	s.Equal(time.Duration(0), tsInfo.GetVirtualTimeOffset().AsDuration())

	// Find the timer in upcoming time points.
	upcomingBefore := descBefore.GetUpcomingTimePoints()
	s.GreaterOrEqual(len(upcomingBefore), 1)
	var timerPoint *workflowpb.UpcomingTimePointInfo
	for _, pt := range upcomingBefore {
		if pt.GetTimer() != nil && pt.GetTimer().GetTimerId() == "timer-1" {
			timerPoint = pt
			break
		}
	}
	s.NotNil(timerPoint, "expected timer-1 in upcoming time points")
	// Timer fire time == TimerStarted.EventTime + 1h exactly.
	s.Equal(expectedTimerFireTime, timerPoint.GetFireTime().AsTime())
	s.Equal(earlyHistory[4].GetEventId(), timerPoint.GetTimer().GetStartedEventId())

	// Advance time: fire the 1h timer.
	advResp := s.advanceTimePoint(we)
	s.False(advResp.NoUpcomingTimePoint)
	// Offset ≈ 1h. The only error is wall-clock elapsed between TimerStarted
	// event and the advance call.
	advOffset := advResp.GetVirtualTimeOffset().AsDuration()
	s.InDelta(float64(1*time.Hour), float64(advOffset), float64(timeSkippingTaskProcessingDelta))

	// Describe after advance: offset should match the advance response.
	descAfter := s.describeWorkflow(we)
	tsInfoAfter := descAfter.GetTimeSkippingInfo()
	s.NotNil(tsInfoAfter)
	s.Equal(advOffset, tsInfoAfter.GetVirtualTimeOffset().AsDuration())
	// timer-1 should no longer appear in upcoming time points.
	for _, pt := range descAfter.GetUpcomingTimePoints() {
		if pt.GetTimer() != nil {
			s.NotEqual("timer-1", pt.GetTimer().GetTimerId(), "timer-1 should have been fired and removed")
		}
	}

	// Complete the workflow.
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Verify full history event sequence.
	historyEvents := s.GetHistory(s.Namespace().String(), we)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 TimerStarted
  6 WorkflowExecutionTimePointAdvanced
  7 TimerFired
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted
 11 WorkflowExecutionCompleted`, historyEvents)

	// Verify event times are monotonically non-decreasing.
	for i := 1; i < len(historyEvents); i++ {
		s.False(historyEvents[i].GetEventTime().AsTime().Before(historyEvents[i-1].GetEventTime().AsTime()),
			"event %d time should not be before event %d time", i+1, i)
	}

	// Verify all event times against known values.
	// Events 1-5 were verified above from earlyHistory. Now verify events 6-11.
	s.Equal(startEventTime.AsTime(), historyEvents[0].GetEventTime().AsTime())         // 1: WorkflowExecutionStarted
	s.Equal(firstTaskStartedTime.AsTime(), historyEvents[2].GetEventTime().AsTime())   // 3: WorkflowTaskStarted
	s.Equal(firstTaskCompletedTime.AsTime(), historyEvents[3].GetEventTime().AsTime()) // 4: WorkflowTaskCompleted
	s.Equal(timerStartedTime.AsTime(), historyEvents[4].GetEventTime().AsTime())       // 5: TimerStarted

	// TimePointAdvanced (6) is at old offset (0). TimerFired (7) is at new
	// offset. Since both share the same wall clock, timerFired event time
	// equals exactly the timer's fire time.
	timerFiredEventTime := historyEvents[6].GetEventTime()
	s.Equal(expectedTimerFireTime, timerFiredEventTime.AsTime())

	// WorkflowTaskStarted (3): before advance, wall clock only. Delta is
	// elapsed time from event creation to this assertion.
	s.WithinDuration(time.Now(), firstTaskStartedTime.AsTime(), timeSkippingTaskProcessingDelta)
	// WorkflowTaskStarted (9): after advance, wall clock + offset. Delta is
	// elapsed time from event creation to this assertion.
	secondTaskStartedTime := historyEvents[8].GetEventTime() // 9: WorkflowTaskStarted
	s.WithinDuration(time.Now().Add(advOffset), secondTaskStartedTime.AsTime(), timeSkippingTaskProcessingDelta)

	// TIME_POINT_ADVANCED event (6): verify all attributes.
	advancedEvent := historyEvents[5]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_POINT_ADVANCED, advancedEvent.GetEventType())
	s.True(advancedEvent.GetWorkerMayIgnore())
	attrs := advancedEvent.GetWorkflowExecutionTimePointAdvancedEventAttributes()
	s.NotNil(attrs)
	s.Equal("test-advancer", attrs.GetIdentity())
	s.NotEmpty(attrs.GetRequestId())
	// Exact invariant: advanceEvent.EventTime + DurationAdvanced == fireTime
	// (durationAdvanced is computed as fireTime - eventTime in the EventFactory).
	durationAdvanced := attrs.GetDurationAdvanced().AsDuration()
	s.True(durationAdvanced > 0, "duration_advanced should be positive")
	computedFireTime := advancedEvent.GetEventTime().AsTime().Add(durationAdvanced)
	s.Equal(expectedTimerFireTime, computedFireTime)

	// TimerFired event (7): verify timer ID and started event reference.
	timerFiredEvent := historyEvents[6]
	s.Equal(enumspb.EVENT_TYPE_TIMER_FIRED, timerFiredEvent.GetEventType())
	firedAttrs := timerFiredEvent.GetTimerFiredEventAttributes()
	s.Equal("timer-1", firedAttrs.GetTimerId())
	s.Equal(earlyHistory[4].GetEventId(), firedAttrs.GetStartedEventId())
}

// Helpers

func (s *TimeSkippingTestSuite) startWorkflowWithTimeSkipping(tq string) *commonpb.WorkflowExecution {
	id := "ts-" + uuid.NewString()
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: "time-skipping-wf"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            "test-worker",
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true},
	}
	resp, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err)
	return &commonpb.WorkflowExecution{WorkflowId: id, RunId: resp.GetRunId()}
}

func (s *TimeSkippingTestSuite) newPoller(tq, identity string, handler func(*workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error)) *testcore.TaskPoller {
	return &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: handler,
		Logger:              s.Logger,
		T:                   s.T(),
	}
}

func (s *TimeSkippingTestSuite) advanceTimePoint(we *commonpb.WorkflowExecution) *workflowservice.AdvanceWorkflowExecutionTimePointResponse {
	resp, err := s.FrontendClient().AdvanceWorkflowExecutionTimePoint(testcore.NewContext(), &workflowservice.AdvanceWorkflowExecutionTimePointRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		Identity:          "test-advancer",
		RequestId:         uuid.NewString(),
	})
	s.NoError(err)
	return resp
}

func (s *TimeSkippingTestSuite) describeWorkflow(we *commonpb.WorkflowExecution) *workflowservice.DescribeWorkflowExecutionResponse {
	resp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: we,
	})
	s.NoError(err)
	return resp
}
