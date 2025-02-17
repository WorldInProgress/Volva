package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// 错误定义
var (
    ErrTaskTimeout    = errors.New("task execution timeout") // 任务执行超时错误
    ErrPoolOverload   = errors.New("worker pool overloaded") // 工作池过载错误
    ErrTaskValidation = errors.New("task validation failed") // 任务验证失败错误
)

// 任务优先级
type Priority int

const (
    LowPriority Priority = iota // 低优先级
    MediumPriority // 中等优先级
    HighPriority // 高优先级
)

// 任务状态
type TaskStatus int32

const (
    TaskPending TaskStatus = iota // 任务待处理
    TaskRunning // 任务运行中
    TaskCompleted // 任务完成
    TaskFailed // 任务失败
)

// 任务接口，定义了任务的执行方法、获取ID和优先级的方法
type Task interface {
    Execute(ctx context.Context) (interface{}, error)
    GetID() string
    GetPriority() Priority
}

// 基础任务结构，定义了任务的ID、优先级、状态、重试次数、数据和结果
type BaseTask struct {
    ID       string
    Priority Priority
    Status   TaskStatus
    Retries  int32
    Data     interface{}
    Result   interface{}
    Error    error
    Created  time.Time
}

// 任务结果，定义了任务ID、结果、错误、开始时间和结束时间
type TaskResult struct {
    TaskID    string
    Result    interface{}
    Error     error
    StartTime time.Time
    EndTime   time.Time
}

// 工作池配置，定义了工作池的配置，包括工作者数量、队列大小、速率限制、最大重试次数、任务超时时间、重试间隔时间、指标是否启用
type WorkerPoolConfig struct {
    NumWorkers     int
    QueueSize      int
    RateLimit      float64
    MaxRetries     int32
    TaskTimeout    time.Duration
    RetryInterval  time.Duration
    MetricsEnabled bool
}

// 工作池结构，定义了工作池的配置、任务队列、结果队列、指标收集器、速率限制器、上下文、取消函数、等待组、错误处理函数、互斥锁、工作者映射
type WorkerPool struct {
    config       WorkerPoolConfig
    tasks        chan Task
    results      chan TaskResult
    metrics      *Metrics
    limiter      *rate.Limiter
    ctx          context.Context
    cancel       context.CancelFunc
    wg           sync.WaitGroup
    errorHandler func(error)
    mu           sync.RWMutex
    workers      map[int]*Worker
    quit         chan struct{}
}

// 指标收集器，定义了任务处理总数、失败数、成功数、处理时间、任务延迟
type Metrics struct {
    TasksProcessed   uint64
    TasksFailed      uint64
    TasksSucceeded   uint64
    ProcessingTime   time.Duration
    mu              sync.RWMutex
    taskLatencies   []time.Duration
}

// 工作者结构，定义了工作者ID、工作池、任务计数、最后错误
type Worker struct {
    ID        int
    pool      *WorkerPool
    taskCount uint64
    // lastError error
}

// 创建新的工作池，初始化工作池的配置、任务队列、结果队列、指标收集器、速率限制器、上下文、取消函数、工作者映射
func NewWorkerPool(config WorkerPoolConfig) *WorkerPool {
    // 创建上下文和取消函数
    ctx, cancel := context.WithCancel(context.Background())
    
    wp := &WorkerPool{
        config:       config,
        tasks:        make(chan Task, config.QueueSize),
        results:      make(chan TaskResult, config.QueueSize),
        metrics:      &Metrics{taskLatencies: make([]time.Duration, 0)},
        limiter:      rate.NewLimiter(rate.Limit(config.RateLimit), 1), // 速率限制器，令牌桶
        ctx:          ctx,
        cancel:       cancel,
        workers:      make(map[int]*Worker),
        errorHandler: defaultErrorHandler,
        quit:         make(chan struct{}),
    }

    return wp
}

// 启动工作池，启动工作者、启动指标收集
func (wp *WorkerPool) Start() {
    // 启动工作者
    for i := 0; i < wp.config.NumWorkers; i++ {
        worker := &Worker{
            ID:   i + 1,
            pool: wp,
        }
        wp.mu.Lock()
        wp.workers[i+1] = worker
        wp.mu.Unlock()
        
        wp.wg.Add(1)
        go worker.start()
    }

    // 启动信号处理
    go wp.handleSignals()

    // 启动指标收集
    if wp.config.MetricsEnabled {
        go wp.collectMetrics()
    }
}

// 工作者处理逻辑，启动工作者、处理任务、更新指标、发送结果
func (w *Worker) start() {
    defer w.pool.wg.Done()

    for {
        select {
        case <-w.pool.ctx.Done():
            return
        case task, ok := <-w.pool.tasks:
            if !ok {
                return
            }

            // 速率限制
            err := w.pool.limiter.Wait(w.pool.ctx)
            if err != nil {
                continue
            }

            // 执行任务
            result := w.processTask(task)
            
            // 更新指标
            atomic.AddUint64(&w.taskCount, 1)
            if result.Error != nil {
                atomic.AddUint64(&w.pool.metrics.TasksFailed, 1)
            } else {
                atomic.AddUint64(&w.pool.metrics.TasksSucceeded, 1)
            }

            // 发送结果
            w.pool.results <- result
        }
    }
}

// 任务处理，记录任务开始时间、创建带超时的上下文、执行任务、等待任务完成或超时、记录任务结束时间、返回任务结果 
func (w *Worker) processTask(task Task) TaskResult {
    log.Printf("Worker %d starting task %s", w.ID, task.GetID())
    startTime := time.Now()
    result := TaskResult{
        TaskID:    task.GetID(),
        StartTime: startTime,
    }

    // 创建带超时的上下文
    ctx, cancel := context.WithTimeout(w.pool.ctx, w.pool.config.TaskTimeout)
    defer cancel()

    // 执行任务
    done := make(chan struct{})
    go func() {
        defer close(done)
        taskResult, err := task.Execute(ctx)
        result.Result = taskResult
        result.Error = err
    }()

    // 等待任务完成或超时
    select {
    case <-ctx.Done():
        result.Error = ErrTaskTimeout
    case <-done:
    }

    result.EndTime = time.Now()
    log.Printf("Worker %d completed task %s", w.ID, task.GetID())

    // 计算并记录任务延迟
    taskDuration := time.Since(startTime)
    w.pool.recordTaskLatency(taskDuration)
    
    return result
}

// 提交任务，检查工作池是否停止、提交任务到任务队列
func (wp *WorkerPool) SubmitTask(task Task) error {
    if wp.ctx.Err() != nil {
        return errors.New("worker pool is stopped")
    }

    select {
    case wp.tasks <- task:
        log.Printf("Successfully submitted task: %s", task.GetID())
        return nil
    default:
        return ErrPoolOverload
    }
}

// 优雅关闭，发送取消信号、创建关闭通道、等待工作者完成、关闭任务队列、关闭结果队列
func (wp *WorkerPool) Shutdown(timeout time.Duration) error {
    // 发送取消信号
    wp.cancel()

    // 创建关闭通道
    go func() {
        wp.wg.Wait()
        close(wp.quit)
    }()

    // 等待超时或完成
    select {
    case <-time.After(timeout):
        return errors.New("shutdown timeout")
    case <-wp.quit:
        return nil
    }
}

// 指标收集，创建定时器、延迟关闭定时器、更新指标
func (wp *WorkerPool) collectMetrics() {
    ticker := time.NewTicker(time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-wp.ctx.Done():
            return
        case <-ticker.C:
            total := atomic.LoadUint64(&wp.metrics.TasksProcessed)
            failed := atomic.LoadUint64(&wp.metrics.TasksFailed)
            succeeded := atomic.LoadUint64(&wp.metrics.TasksSucceeded)
            
            min, max, avg := wp.getLatencyStats()  // 使用统计函数
            
            log.Printf("Metrics - Total: %d, Succeeded: %d, Failed: %d, Latency(min/avg/max): %v/%v/%v",
                total, succeeded, failed, min, avg, max)
        }
    }
}

func (wp *WorkerPool) handleSignals() {
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, os.Interrupt)
    
    <-sigChan
    log.Println("Received shutdown signal, initiating graceful shutdown...")
    
    if err := wp.Shutdown(30 * time.Second); err != nil {
        log.Printf("Shutdown error: %v", err)
    }
}

// 默认错误处理，记录错误
func defaultErrorHandler(err error) {
    log.Printf("Error: %v", err)
}

// 示例任务实现，定义了任务的执行方法、获取ID和优先级的方法
type ExampleTask struct {
    BaseTask
    ProcessingFunc func(context.Context) (interface{}, error)
}

// 执行任务，执行任务处理函数
func (t *ExampleTask) Execute(ctx context.Context) (interface{}, error) {
    return t.ProcessingFunc(ctx)
}

// 获取ID，返回任务ID
func (t *ExampleTask) GetID() string {
    return t.ID
}

// 获取优先级，返回任务优先级
func (t *ExampleTask) GetPriority() Priority {
    return t.Priority
}

// 添加新方法来记录任务延迟
func (wp *WorkerPool) recordTaskLatency(duration time.Duration) {
    wp.metrics.mu.Lock()
    defer wp.metrics.mu.Unlock()
    
    // 保持最近 N 个任务的延迟记录
    const maxLatencyRecords = 1000
    
    wp.metrics.taskLatencies = append(wp.metrics.taskLatencies, duration)
    
    // 如果记录太多，删除最旧的记录
    if len(wp.metrics.taskLatencies) > maxLatencyRecords {
        // 保留最后 maxLatencyRecords 个记录
        wp.metrics.taskLatencies = wp.metrics.taskLatencies[len(wp.metrics.taskLatencies)-maxLatencyRecords:]
    }
}

// 添加方法获取延迟统计信息
func (wp *WorkerPool) getLatencyStats() (min, max, avg time.Duration) {
    wp.metrics.mu.RLock()
    defer wp.metrics.mu.RUnlock()
    
    if len(wp.metrics.taskLatencies) == 0 {
        return 0, 0, 0
    }
    
    min = wp.metrics.taskLatencies[0]
    max = wp.metrics.taskLatencies[0]
    var sum time.Duration
    
    for _, lat := range wp.metrics.taskLatencies {
        sum += lat
        if lat < min {
            min = lat
        }
        if lat > max {
            max = lat
        }
    }
    
    avg = sum / time.Duration(len(wp.metrics.taskLatencies))
    return min, max, avg
}

// 主函数
func main() {
    // 配置工作池
    config := WorkerPoolConfig{
        NumWorkers:     5,
        QueueSize:      100,
        RateLimit:      10.0,
        MaxRetries:     3,
        TaskTimeout:    5 * time.Second,
        RetryInterval:  time.Second,
        MetricsEnabled: true,
    }

    // 创建并启动工作池
    pool := NewWorkerPool(config)
    pool.Start()

    // 创建示例任务
    for i := 0; i < 20; i++ {
        task := &ExampleTask{
            BaseTask: BaseTask{
                ID:       fmt.Sprintf("task-%d", i),
                Priority: Priority(i % 3),
                Created:  time.Now(),
            },
            ProcessingFunc: func(ctx context.Context) (interface{}, error) {
                time.Sleep(time.Duration(500+i*100) * time.Millisecond)
                return fmt.Sprintf("Result of task-%d", i), nil
            },
        }

        if err := pool.SubmitTask(task); err != nil {
            log.Printf("Failed to submit task: %v", err)
        } else {
            log.Printf("Successfully submitted task: %s", task.GetID())
        }
    }

    // 等待信号处理程序处理关闭
    <-pool.quit
    log.Println("Program exit")
}