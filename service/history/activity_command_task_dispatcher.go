package history

import (
	"context"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workerpb "go.temporal.io/api/worker/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

const (
	activityCommandTaskTimeout = time.Second * 10 * debug.TimeoutMultiplier
)

// activityCommandTaskDispatcher handles dispatching activity command tasks to workers.
type activityCommandTaskDispatcher struct {
	shardContext      historyi.ShardContext
	cache             wcache.Cache
	matchingRawClient resource.MatchingRawClient
	config            *configs.Config
	metricsHandler    metrics.Handler
	logger            log.Logger
}

func newActivityCommandTaskDispatcher(
	shardContext historyi.ShardContext,
	cache wcache.Cache,
	matchingRawClient resource.MatchingRawClient,
	config *configs.Config,
	metricsHandler metrics.Handler,
	logger log.Logger,
) *activityCommandTaskDispatcher {
	return &activityCommandTaskDispatcher{
		shardContext:      shardContext,
		cache:             cache,
		matchingRawClient: matchingRawClient,
		config:            config,
		metricsHandler:    metricsHandler,
		logger:            logger,
	}
}

func (d *activityCommandTaskDispatcher) execute(
	ctx context.Context,
	task *tasks.ActivityCommandTask,
) error {
	if !d.config.EnableActivityCancellationNexusTask() {
		return nil
	}

	if len(task.ScheduledEventIDs) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, activityCommandTaskTimeout)
	defer cancel()

	taskTokens, err := d.buildTaskTokens(ctx, task)
	if err != nil {
		return err
	}
	if len(taskTokens) == 0 {
		return nil
	}

	return d.dispatchToWorker(ctx, task, taskTokens)
}

// buildTaskTokens loads mutable state and builds task tokens for activities that need commands.
// Lock is acquired and released within this method.
func (d *activityCommandTaskDispatcher) buildTaskTokens(
	ctx context.Context,
	task *tasks.ActivityCommandTask,
) ([][]byte, error) {
	weContext, release, err := getWorkflowExecutionContextForTask(ctx, d.shardContext, d.cache, task)
	if err != nil {
		return nil, err
	}
	defer release(nil)

	mutableState, err := weContext.LoadMutableState(ctx, d.shardContext)
	if err != nil {
		return nil, err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil, nil
	}

	var taskTokens [][]byte
	for _, scheduledEventID := range task.ScheduledEventIDs {
		ai, ok := mutableState.GetActivityInfo(scheduledEventID)
		if !ok || ai.StartedEventId == common.EmptyEventID {
			continue
		}
		if task.CommandType == enumsspb.ACTIVITY_COMMAND_TYPE_CANCEL && !ai.CancelRequested {
			continue
		}

		taskToken := &tokenspb.Task{
			NamespaceId:      task.NamespaceID,
			WorkflowId:       task.WorkflowID,
			RunId:            task.RunID,
			ScheduledEventId: scheduledEventID,
			Attempt:          ai.Attempt,
			ActivityId:       ai.ActivityId,
			StartedEventId:   ai.StartedEventId,
			Version:          ai.Version,
		}
		taskTokenBytes, err := taskToken.Marshal()
		if err != nil {
			return nil, err
		}
		taskTokens = append(taskTokens, taskTokenBytes)
	}
	return taskTokens, nil
}

func (d *activityCommandTaskDispatcher) dispatchToWorker(
	ctx context.Context,
	task *tasks.ActivityCommandTask,
	taskTokens [][]byte,
) error {
	notificationRequest := &workerpb.ActivityNotificationRequest{
		NotificationType: workerpb.ActivityNotificationType(task.CommandType),
		TaskTokens:       taskTokens,
	}
	requestPayload, err := payload.Encode(notificationRequest)
	if err != nil {
		return fmt.Errorf("failed to encode activity command request: %w", err)
	}

	nexusRequest := &nexuspb.Request{
		Header: map[string]string{},
		Variant: &nexuspb.Request_StartOperation{
			StartOperation: &nexuspb.StartOperationRequest{
				Service:   workerpb.WorkerService.ServiceName,
				Operation: workerpb.WorkerService.NotifyActivity.Name(),
				Payload:   requestPayload,
			},
		},
	}

	resp, err := d.matchingRawClient.DispatchNexusTask(ctx, &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: task.NamespaceID,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: task.Destination,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Request: nexusRequest,
	})
	if err != nil {
		d.logger.Warn("Failed to dispatch activity command to worker",
			tag.NewStringTag("control_queue", task.Destination),
			tag.Error(err))
		return err
	}

	return d.handleDispatchResponse(resp, task.Destination)
}

func (d *activityCommandTaskDispatcher) handleDispatchResponse(
	resp *matchingservice.DispatchNexusTaskResponse,
	controlQueue string,
) error {
	// Check for timeout (no worker polling)
	if resp.GetRequestTimeout() != nil {
		d.logger.Warn("No worker polling control queue for activity command",
			tag.NewStringTag("control_queue", controlQueue))
		return fmt.Errorf("no worker polling control queue")
	}

	// Check for worker handler failure
	if failure := resp.GetFailure(); failure != nil {
		d.logger.Warn("Worker handler failed for activity command",
			tag.NewStringTag("control_queue", controlQueue),
			tag.NewStringTag("failure_message", failure.GetMessage()))
		return fmt.Errorf("worker handler failed: %s", failure.GetMessage())
	}

	// Check operation-level response
	nexusResp := resp.GetResponse()
	if nexusResp == nil {
		return nil
	}

	startOpResp := nexusResp.GetStartOperation()
	if startOpResp == nil {
		return nil
	}

	// Check for operation failure (terminal - don't retry)
	if opFailure := startOpResp.GetFailure(); opFailure != nil {
		d.logger.Warn("Activity command operation failure",
			tag.NewStringTag("control_queue", controlQueue),
			tag.NewStringTag("failure_message", opFailure.GetMessage()))
		return nil
	}

	return nil
}

