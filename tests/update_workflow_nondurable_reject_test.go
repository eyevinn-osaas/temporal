package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type UpdateWorkflowNonDurableRejectSuite struct {
	testcore.FunctionalTestBase
}

func TestUpdateWorkflowNonDurableRejectSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(UpdateWorkflowNonDurableRejectSuite))
}

// TestNonDurableReject replicates the features/update/non_durable_reject test.
// This test verifies the "premature end of stream" bug where:
// 1. Worker polls workflow task with transient events (expectedLastEventID=6)
// 2. Update is rejected in validator (non-durable, not persisted)
// 3. Worker requests full history via GetWorkflowExecutionHistory
// 4. History must include transient events up to event 6 to prevent premature end error
func (s *UpdateWorkflowNonDurableRejectSuite) TestNonDurableReject() {
	id := "update-non-durable-reject"
	wt := "NonDurableReject"
	tl := "update-non-durable-reject-taskqueue"
	identity := "worker1"
	updateID := "update-to-reject"

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
		s.T().Logf("wtHandler called, attempt %d, history events: %d, started event ID: %d",
			wtHandlerCalls, len(task.History.Events), task.StartedEventId)

		switch wtHandlerCalls {
		case 1:
			// First WT: just started, drain it
			s.Len(task.History.Events, 3, "First WT should have 3 events")
			return nil, nil
		case 2:
			// Second WT: speculative workflow task for update rejection
			// This should include transient WFT events (event 5: WFTScheduled, event 6: WFTStarted)
			s.T().Logf("Second WT history events:")
			for i, event := range task.History.Events {
				s.T().Logf("  Event %d (ID=%d): %s", i, event.EventId, event.EventType.String())
			}

			// We expect events 1-6:
			// 1: WorkflowExecutionStarted
			// 2: WorkflowTaskScheduled
			// 3: WorkflowTaskStarted
			// 4: WorkflowTaskCompleted (from first WT)
			// 5: WorkflowTaskScheduled (speculative/transient)
			// 6: WorkflowTaskStarted (speculative/transient)
			s.GreaterOrEqual(len(task.History.Events), 6, "Second WT should have at least 6 events including transient")
			s.Equal(int64(6), task.StartedEventId, "StartedEventId should be 6 (the transient WFTStarted)")

			// Return empty commands - we'll reject the update in message handler
			return nil, nil
		default:
			s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
			return nil, nil
		}
	}

	msgHandlerCalls := 0
	msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
		msgHandlerCalls++
		s.T().Logf("msgHandler called, attempt %d, messages: %d", msgHandlerCalls, len(task.Messages))

		switch msgHandlerCalls {
		case 1:
			// First WT: no messages
			return nil, nil
		case 2:
			// Second WT: reject the update
			s.Require().NotEmpty(task.Messages, "Second WT should have update request message")
			updRequestMsg := task.Messages[0]
			updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

			s.T().Logf("Rejecting update: %s", updRequest.GetMeta().GetUpdateId())

			// Reject the update (non-durable rejection via validator)
			return []*protocolpb.Message{
				{
					Id:                 "update-rejected",
					ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
					SequencingId:       nil,
					Body: protoutils.MarshalAny(s.T(), &updatepb.Rejection{
						RejectedRequestMessageId:         updRequestMsg.GetId(),
						RejectedRequestSequencingEventId: updRequestMsg.GetEventId(),
						RejectedRequest:                  updRequest,
						Failure: &failurepb.Failure{
							Message:     "Update rejected by validator",
							FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{}},
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

	// Step 1: Drain first workflow task
	s.T().Log("Step 1: Polling and processing first workflow task")
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	s.Equal(1, wtHandlerCalls, "First WT should have been processed")

	// Step 2: Send update that will be rejected
	s.T().Log("Step 2: Sending update (will be rejected)")
	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		resp, err := s.FrontendClient().UpdateWorkflowExecution(testcore.NewContext(), &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: workflowExecution,
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{UpdateId: updateID},
				Input: &updatepb.Input{
					Name: "update-handler",
					Args: payloads.EncodeString("arg-value"),
				},
			},
		})
		if err != nil {
			s.T().Logf("Update returned error (expected for rejection): %v", err)
		}
		updateResultCh <- resp
	}()

	// Brief delay to ensure update is sent and speculative WT is created
	time.Sleep(100 * time.Millisecond)

	// Step 3: Poll the speculative workflow task (this is where the bug manifests)
	// The worker receives the workflow task with transient events in the poll response
	s.T().Log("Step 3: Polling speculative workflow task with transient events")
	ctx := newSDKContext()
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  identity,
	})
	s.NoError(err)
	s.NotNil(pollResp)
	s.NotEmpty(pollResp.TaskToken)

	s.T().Logf("Poll response: %d events, StartedEventId=%d", len(pollResp.History.Events), pollResp.StartedEventId)

	// Verify poll response contains transient events
	s.GreaterOrEqual(len(pollResp.History.Events), 6, "Poll response should include transient events (events 5 and 6)")
	s.Equal(int64(6), pollResp.StartedEventId, "StartedEventId should be 6")

	// Step 4: Simulate cache eviction - worker requests full history
	// THIS IS WHERE THE BUG OCCURS: GetWorkflowExecutionHistory must return events up to ID 6
	// to match what the worker received in the poll response
	s.T().Log("Step 4: Simulating cache eviction - requesting full history via GetWorkflowExecutionHistory")
	histResp, err := s.FrontendClient().GetWorkflowExecutionHistory(ctx,
		&workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:       s.Namespace().String(),
			Execution:       workflowExecution,
			MaximumPageSize: 100,
		})

	// This should NOT return "premature end of stream" error
	s.NoError(err, "GetWorkflowExecutionHistory should not return 'premature end of stream' error")
	s.NotNil(histResp)
	s.NotNil(histResp.History)
	s.NotEmpty(histResp.History.Events)

	// Verify history includes all events up to and including the transient events
	events := histResp.History.Events
	s.T().Logf("GetWorkflowExecutionHistory returned %d events", len(events))
	for i, event := range events {
		s.T().Logf("  Event %d (ID=%d): %s", i, event.EventId, event.EventType.String())
	}

	lastEventID := events[len(events)-1].GetEventId()
	s.GreaterOrEqual(lastEventID, pollResp.GetStartedEventId(),
		"Last event ID in history (%d) should be >= StartedEventId from poll response (%d) to prevent 'premature end of stream'",
		lastEventID, pollResp.GetStartedEventId())

	// Verify no event ID gaps
	for i := 0; i < len(events); i++ {
		expectedID := int64(i + 1)
		s.Equal(expectedID, events[i].GetEventId(), "Event ID should be sequential, no gaps at position %d", i)
	}

	// Verify transient WorkflowTaskScheduled and Started events are present
	s.GreaterOrEqual(len(events), 6, "History should include at least 6 events (including transient WFT)")
	if len(events) >= 6 {
		s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, events[4].GetEventType(), "Event 5 should be WorkflowTaskScheduled (transient)")
		s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, events[5].GetEventType(), "Event 6 should be WorkflowTaskStarted (transient)")
	}

	// Step 5: Complete the workflow task (reject the update)
	s.T().Log("Step 5: Completing workflow task with update rejection")
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

	// Wait for update result (should be rejection)
	s.T().Log("Step 6: Waiting for update result")
	select {
	case updateResult := <-updateResultCh:
		if updateResult != nil {
			s.T().Logf("Update result: %+v", updateResult)
		}
	case <-time.After(5 * time.Second):
		s.T().Log("Update response timed out (may be expected for rejection)")
	}

	s.Equal(2, wtHandlerCalls, "Should have processed 2 workflow tasks")
	s.Equal(2, msgHandlerCalls, "Should have processed 2 message batches")

	s.T().Log("✅ Test passed - no 'premature end of stream' error!")
}

// TestNonDurableRejectWithPagination tests the same scenario but with pagination
// to ensure transient events appear on the last page correctly
func (s *UpdateWorkflowNonDurableRejectSuite) TestNonDurableRejectWithPagination() {
	id := "update-non-durable-reject-pagination"
	wt := "NonDurableRejectPagination"
	tl := "update-non-durable-reject-pagination-tq"
	identity := "worker1"
	updateID := "update-to-reject-pagination"

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

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		return nil, nil
	}

	msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
		if len(task.Messages) == 0 {
			return nil, nil
		}

		// Reject the update
		updRequestMsg := task.Messages[0]
		updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

		return []*protocolpb.Message{
			{
				Id:                 "update-rejected",
				ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
				SequencingId:       nil,
				Body: protoutils.MarshalAny(s.T(), &updatepb.Rejection{
					RejectedRequestMessageId:         updRequestMsg.GetId(),
					RejectedRequestSequencingEventId: updRequestMsg.GetEventId(),
					RejectedRequest:                  updRequest,
					Failure: &failurepb.Failure{
						Message:     "Update rejected",
						FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{}},
					},
				}),
			},
		}, nil
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
		_, _ = s.FrontendClient().UpdateWorkflowExecution(testcore.NewContext(), &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: workflowExecution,
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{UpdateId: updateID},
				Input: &updatepb.Input{
					Name: "update-handler",
					Args: payloads.EncodeString("arg-value"),
				},
			},
		})
	}()

	time.Sleep(100 * time.Millisecond)

	// Poll the speculative workflow task
	ctx := newSDKContext()
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  identity,
	})
	s.NoError(err)
	s.NotNil(pollResp)

	// Fetch history with small page size to force pagination
	var allEvents []*historypb.HistoryEvent
	nextPageToken := []byte(nil)
	pageCount := 0

	s.T().Log("Fetching history with pagination (page size 2)")
	for {
		histResp, err := s.FrontendClient().GetWorkflowExecutionHistory(ctx,
			&workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace:       s.Namespace().String(),
				Execution:       workflowExecution,
				MaximumPageSize: 2, // Small page size to force multiple pages
				NextPageToken:   nextPageToken,
			})
		s.NoError(err, "GetWorkflowExecutionHistory page %d should not fail", pageCount+1)
		s.NotNil(histResp)

		pageCount++
		allEvents = append(allEvents, histResp.History.Events...)
		s.T().Logf("Page %d: %d events (total so far: %d)", pageCount, len(histResp.History.Events), len(allEvents))

		if len(histResp.NextPageToken) == 0 {
			break
		}
		nextPageToken = histResp.NextPageToken
	}

	// Verify we got multiple pages and all events
	s.Greater(pageCount, 1, "Should have multiple pages with page size 2")
	s.GreaterOrEqual(len(allEvents), 6, "Should have at least 6 events including transient WFT")

	// Verify transient events are included
	lastEventID := allEvents[len(allEvents)-1].GetEventId()
	s.GreaterOrEqual(lastEventID, pollResp.GetStartedEventId(),
		"Last event ID should be >= StartedEventId to prevent premature end of stream")

	// Clean up - complete the task
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

	s.T().Log("✅ Pagination test passed!")
}
