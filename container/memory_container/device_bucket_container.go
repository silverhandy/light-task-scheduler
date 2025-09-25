package memeorycontainer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	lighttaskscheduler "github.com/memory-overflow/light-task-scheduler"
)

// deviceBucketContainer: 按设备分桶的内存任务容器，每个设备一个等待队列（channel），支持同时多设备调度。
// 优点：不同设备等待队列互不阻塞，避免单一设备任务把全局 FIFO 阻塞；
// 缺点：仍然是进程内结构，不支持多进程共享；没有优先级。
type deviceBucketContainer struct {
	MemeoryContainer

	// deviceId -> waiting queue
	deviceQueues map[string]chan lighttaskscheduler.Task
	defaultQueue chan lighttaskscheduler.Task // 未指定或未配置的设备任务进入默认队列

	timeout time.Duration

	// 运行中任务
	runningTaskMap   sync.Map // taskId -> Task
	runningTaskCount int32

	// 等待中被请求停止/删除的任务标记（只在取出时判定丢弃）
	stoppedOrDeleted sync.Map // taskId -> struct{}
}

// MakeDeviceBucketContainer 构造设备分桶容器
// deviceQueueSize: 每个设备队列大小；defaultQueueSize: 默认队列大小；timeout: 入队/出队等待超时时间
func MakeDeviceBucketContainer(deviceQueueSize map[string]int, defaultQueueSize int, timeout time.Duration) *deviceBucketContainer {
	dq := make(map[string]chan lighttaskscheduler.Task)
	for dev, sz := range deviceQueueSize {
		if sz <= 0 {
			sz = 1024
		}
		dq[dev] = make(chan lighttaskscheduler.Task, sz)
	}
	if defaultQueueSize <= 0 {
		defaultQueueSize = 1024
	}
	return &deviceBucketContainer{
		deviceQueues: dq,
		defaultQueue: make(chan lighttaskscheduler.Task, defaultQueueSize),
		timeout:      timeout,
	}
}

// AddTask 把任务加入对应设备队列
func (d *deviceBucketContainer) AddTask(ctx context.Context, task lighttaskscheduler.Task) error {
	// 如果之前被标记停止/删除，再次 Add 视为复活，从标记表移除
	d.stoppedOrDeleted.LoadAndDelete(task.TaskId)
	task.TaskStatus = lighttaskscheduler.TASK_STATUS_WAITING
	ch, ok := d.deviceQueues[task.DeviceId]
	if !ok || task.DeviceId == "" { // 使用默认队列
		ch = d.defaultQueue
	}
	select {
	case ch <- task:
		return nil
	case <-time.After(d.timeout):
		return fmt.Errorf("add task timeout")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// AddRunningTask 恢复运行中任务（用于可持久化恢复场景）
func (d *deviceBucketContainer) AddRunningTask(ctx context.Context, task lighttaskscheduler.Task) error {
	if _, ok := d.runningTaskMap.LoadOrStore(task.TaskId, task); !ok {
		atomic.AddInt32(&d.runningTaskCount, 1)
	}
	return nil
}

// GetRunningTask 返回所有运行中任务快照
func (d *deviceBucketContainer) GetRunningTask(ctx context.Context) ([]lighttaskscheduler.Task, error) {
	var tasks []lighttaskscheduler.Task
	d.runningTaskMap.Range(func(_, v interface{}) bool {
		tasks = append(tasks, v.(lighttaskscheduler.Task))
		return true
	})
	return tasks, nil
}

// GetRunningTaskCount 返回运行中任务总数
func (d *deviceBucketContainer) GetRunningTaskCount(ctx context.Context) (int32, error) {
	return atomic.LoadInt32(&d.runningTaskCount), nil
}

// GetWaitingTask 轮询各设备队列（含默认队列）做近似轮询聚合，直到获取 limit 或超时。
func (d *deviceBucketContainer) GetWaitingTask(ctx context.Context, limit int32) ([]lighttaskscheduler.Task, error) {
	if limit <= 0 {
		return nil, nil
	}
	var result []lighttaskscheduler.Task
	// 构造顺序列表：deviceQueues + defaultQueue 最后
	order := make([]chan lighttaskscheduler.Task, 0, len(d.deviceQueues)+1)
	for _, ch := range d.deviceQueues {
		order = append(order, ch)
	}
	order = append(order, d.defaultQueue)

	// 轮询次数上限，防止全部空队列忙等：设备数 * limit * 2
	maxScan := len(order)*int(limit)*2 + 1
	attempts := 0
Outer:
	for int32(len(result)) < limit && attempts < maxScan {
		attempts++
		for _, ch := range order {
			if int32(len(result)) >= limit {
				break Outer
			}
			select {
			case task := <-ch:
				if _, skipped := d.stoppedOrDeleted.LoadAndDelete(task.TaskId); skipped {
					continue // 丢弃
				}
				result = append(result, task)
			default:
				// 没有数据，继续下一个队列
			}
		}
		// 如果一轮什么都没取到，做一个短暂让步或等待
		if int32(len(result)) < limit {
			select {
			case <-time.After(d.timeout / 10):
			case <-ctx.Done():
				return result, ctx.Err()
			}
		}
	}
	return result, nil
}

// ToRunningStatus 转成运行中
func (d *deviceBucketContainer) ToRunningStatus(ctx context.Context, task *lighttaskscheduler.Task) (*lighttaskscheduler.Task, error) {
	task.TaskStartTime = time.Now()
	task.TaskStatus = lighttaskscheduler.TASK_STATUS_RUNNING
	if _, ok := d.runningTaskMap.LoadOrStore(task.TaskId, *task); !ok {
		atomic.AddInt32(&d.runningTaskCount, 1)
	} else { // 更新尝试次数
		v, _ := d.runningTaskMap.Load(task.TaskId)
		t := v.(lighttaskscheduler.Task)
		t.TaskAttemptsTime = task.TaskAttemptsTime
		d.runningTaskMap.Store(task.TaskId, t)
	}
	return task, nil
}

// ToStopStatus 停止（运行中释放；等待中标记）
func (d *deviceBucketContainer) ToStopStatus(ctx context.Context, task *lighttaskscheduler.Task) (*lighttaskscheduler.Task, error) {
	if _, ok := d.runningTaskMap.LoadAndDelete(task.TaskId); ok {
		atomic.AddInt32(&d.runningTaskCount, -1)
	} else {
		d.stoppedOrDeleted.Store(task.TaskId, struct{}{})
	}
	task.TaskStatus = lighttaskscheduler.TASK_STATUS_STOPED
	return task, nil
}

// ToDeleteStatus 删除（语义与 Stop 类似但状态不同）
func (d *deviceBucketContainer) ToDeleteStatus(ctx context.Context, task *lighttaskscheduler.Task) (*lighttaskscheduler.Task, error) {
	if _, ok := d.runningTaskMap.LoadAndDelete(task.TaskId); ok {
		atomic.AddInt32(&d.runningTaskCount, -1)
	} else {
		d.stoppedOrDeleted.Store(task.TaskId, struct{}{})
	}
	task.TaskStatus = lighttaskscheduler.TASK_STATUS_DELETE
	return task, nil
}

// ToFailedStatus 失败
func (d *deviceBucketContainer) ToFailedStatus(ctx context.Context, task *lighttaskscheduler.Task, reason error) (*lighttaskscheduler.Task, error) {
	if _, ok := d.runningTaskMap.LoadAndDelete(task.TaskId); ok {
		atomic.AddInt32(&d.runningTaskCount, -1)
	}
	task.TaskStatus = lighttaskscheduler.TASK_STATUS_FAILED
	task.FailedReason = reason
	task.TaskEndTime = time.Now()
	return task, nil
}

// ToExportStatus 导出中
func (d *deviceBucketContainer) ToExportStatus(ctx context.Context, task *lighttaskscheduler.Task) (*lighttaskscheduler.Task, error) {
	if _, ok := d.runningTaskMap.LoadAndDelete(task.TaskId); ok {
		atomic.AddInt32(&d.runningTaskCount, -1)
	}
	task.TaskStatus = lighttaskscheduler.TASK_STATUS_EXPORTING
	return task, nil
}

// ToSuccessStatus 成功
func (d *deviceBucketContainer) ToSuccessStatus(ctx context.Context, task *lighttaskscheduler.Task) (*lighttaskscheduler.Task, error) {
	if _, ok := d.runningTaskMap.LoadAndDelete(task.TaskId); ok {
		atomic.AddInt32(&d.runningTaskCount, -1)
	}
	task.TaskStatus = lighttaskscheduler.TASK_STATUS_SUCCESS
	task.TaskEndTime = time.Now()
	return task, nil
}

// UpdateRunningTaskStatus 这里简单忽略（可扩展进度持久化）
func (d *deviceBucketContainer) UpdateRunningTaskStatus(ctx context.Context, task *lighttaskscheduler.Task, status lighttaskscheduler.AsyncTaskStatus) error {
	return nil
}
