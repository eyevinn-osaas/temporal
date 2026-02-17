package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type UpdateWorkflowTransientHistorySuite struct {
	testcore.FunctionalTestBase
}

func TestUpdateWorkflowTransientHistorySuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(UpdateWorkflowTransientHistorySuite))
}

// TestUpdateBasicWithHistoryFetch replicates the features/update/basic scenario.
// This test verifies that when a worker receives an update task with transient events,
// then requests full history via GetWorkflowExecutionHistory, the history includes
// all events without gaps (no "premature end of stream" error).
func (s *UpdateWorkflowTransientHistorySuite) TestUpdateBasicWithHistoryFetch() {
	id := "update-basic-with-history-fetch"
	wt := "update-basic-with-history-fetch-type"
	tl := "update-basic-with-history-fetch-taskqueue"
	identity := "worker1"
	updateID := "update-id-1"
	handlerName := "update-handler"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err)

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Drain first workflow task
			return nil, nil
		case 2:
			// Process valid update - return protocol message commands
			return []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
					Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
						MessageId: "update-accepted",
					}},
				},
				{
					CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
					Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
						MessageId: "update-completed",
					}},
				},
			}, nil
		default:
			s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
			return nil, nil
		}
	}

	msgHandlerCalls := 0
	msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
		msgHandlerCalls++
		switch msgHandlerCalls {
		case 1:
			return nil, nil
		case 2:
			// Process the update request
			updRequestMsg := task.Messages[0]
			updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

			return []*protocolpb.Message{
				{
					Id:                 "update-accepted",
					ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
					SequencingId:       nil,
					Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
						AcceptedRequestMessageId:         updRequestMsg.GetId(),
						AcceptedRequestSequencingEventId: updRequestMsg.GetEventId(),
						AcceptedRequest:                  updRequest,
					}),
				},
				{
					Id:                 "update-completed",
					ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
					SequencingId:       nil,
					Body: protoutils.MarshalAny(s.T(), &updatepb.Response{
						Meta: updRequest.GetMeta(),
						Outcome: &updatepb.Outcome{
							Value: &updatepb.Outcome_Success{
								Success: payloads.EncodeString("success-result"),
							},
						},
					}),
				},
			}, nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	//nolint:staticcheck // SA1019 TaskPoller replacement needed
	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first workflow task
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Send update
	go func() {
		_, err := s.FrontendClient().UpdateWorkflowExecution(testcore.NewContext(), &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: workflowExecution,
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{UpdateId: updateID},
				Input: &updatepb.Input{
					Name: handlerName,
					Args: payloads.EncodeString("args-value"),
				},
			},
		})
		if err != nil {
			s.T().Logf("Update error: %v", err)
		}
	}()

	// Brief delay to ensure update is sent
	time.Sleep(100 * time.Millisecond)

	// Process update in workflow - this creates a speculative workflow task
	ctx := newSDKContext()
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  identity,
	})
	s.NoError(err)
	s.NotNil(pollResp)
	s.NotEmpty(pollResp.TaskToken)

	// Verify poll response contains transient events
	s.GreaterOrEqual(len(pollResp.History.Events), 6, "Poll response should include transient events")
	s.Equal(int64(6), pollResp.StartedEventId, "StartedEventId should point to transient WorkflowTaskStarted")

	// Simulate cache eviction - worker requests full history via GetWorkflowExecutionHistory
	histResp, err := s.FrontendClient().GetWorkflowExecutionHistory(ctx,
		&workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:       s.Namespace().String(),
			Execution:       workflowExecution,
			MaximumPageSize: 100,
		})
	s.NoError(err, "GetWorkflowExecutionHistory should not return 'premature end of stream' error")
	s.NotNil(histResp)
	s.NotNil(histResp.History)
	s.NotEmpty(histResp.History.Events)

	// Verify history includes all events up to and including the transient events
	events := histResp.History.Events
	lastEventID := events[len(events)-1].GetEventId()
	s.GreaterOrEqual(lastEventID, pollResp.GetStartedEventId(),
		"Last event ID in history (%d) should be >= StartedEventId from poll response (%d) to prevent 'premature end of stream'",
		lastEventID, pollResp.GetStartedEventId())

	// Verify no event ID gaps
	for i := 0; i < len(events); i++ {
		expectedID := int64(i + 1)
		s.Equal(expectedID, events[i].GetEventId(), "Event ID should be sequential, no gaps")
	}

	// Verify transient WorkflowTaskScheduled and Started events are present
	s.GreaterOrEqual(len(events), 6, "History should include at least 6 events (including transient WFT)")
	if len(events) >= 6 {
		s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, events[4].GetEventType(), "Event 5 should be WorkflowTaskScheduled")
		s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, events[5].GetEventType(), "Event 6 should be WorkflowTaskStarted")
	}

	// Complete the workflow task
	commands, _ := wtHandler(pollResp)
	messages, _ := msgHandler(pollResp)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: pollResp.TaskToken,
		Identity:  identity,
		Commands:  commands,
		Messages:  messages,
	})
	s.NoError(err)

	s.Equal(2, wtHandlerCalls)
	s.Equal(2, msgHandlerCalls)
}
