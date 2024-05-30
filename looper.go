package looper

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Panic handler
type PanicHandlerFunc func(jobName string, recoverData interface{})

var (
	panicHandler      PanicHandlerFunc
	panicHandlerMutex = sync.RWMutex{}
)

func SetPanicHandler(handler PanicHandlerFunc) {
	panicHandlerMutex.Lock()
	defer panicHandlerMutex.Unlock()
	panicHandler = handler
}

// Looper
type Looper struct {
	running     bool
	jobs        []*Job
	startupTime time.Duration
	hooks       hooks
	mu          sync.RWMutex
	locker      locker
}

type (
	HookBeforeJob     func(jobName string)
	HookAfterJob      func(jobName string, duration time.Duration)
	HookAfterJobError func(jobName string, duration time.Duration, err error)
)

type hooks struct {
	beforeJob     HookBeforeJob
	afterJob      HookAfterJob
	afterJobError HookAfterJobError
}

type Config struct {
	// Startup time ensuring a consistent delay between registered jobs on start of looper.
	//
	// StartupTime = 1 second; 5 registered jobs; Jobs would be initiated
	// with 200ms delay
	StartupTime time.Duration

	Locker locker
}

type JobFn func(ctx context.Context) error

type Job struct {
	// Job function which get triggered by looper.
	JobFn JobFn

	// Name of the job.
	Name string

	// Timeout for job, maximum time, the job can run, after timeout the job get killed.
	Timeout time.Duration

	// Wait duration before next job execution after successful execution of previous job.
	WaitAfterSuccess time.Duration

	// Wait duration before next job execution after unsuccessful execution of previous job.
	WaitAfterError time.Duration

	// If job is Active, and can be started.
	Active bool

	// If job is started.
	Started bool

	// If job is currently running.
	Running bool

	// Last time the job ran.
	LastRun time.Time

	// Count of successful job runs.
	RunCountSuccess uint64

	// Count of unsuccessful job runs.
	RunCountError uint64

	// Copy of last error, that occured.
	LastError error

	// Hook function before job runs.
	BeforeJob HookBeforeJob

	// Hook function after job runs successfully.
	AfterJob HookAfterJob

	// Hook function after job runs unsuccessfully.
	AfterJobError HookAfterJobError

	// If the job should use locker
	WithLocker bool

	// Locker
	locker locker

	// Context cancel
	contextCancel context.CancelFunc

	mu sync.RWMutex
}

func New(config Config) *Looper {
	l := &Looper{
		jobs:        []*Job{},
		startupTime: setDefaultDuration(config.StartupTime, time.Second),
		hooks: hooks{
			beforeJob:     func(jobName string) {},
			afterJob:      func(jobName string, duration time.Duration) {},
			afterJobError: func(jobName string, duration time.Duration, err error) {},
		},
		locker: newNopLocker(),
	}

	if config.Locker != nil {
		l.locker = config.Locker
	}

	return l
}

func (l *Looper) RegisterHooks(
	beforeJob HookBeforeJob,
	afterJob HookAfterJob,
	afterJobError HookAfterJobError,
) {
	l.hooks.beforeJob = beforeJob
	l.hooks.afterJobError = afterJobError
	l.hooks.afterJob = afterJob
}

func (l *Looper) AddJob(ctx context.Context, jobInput *Job) (err error) {
	if jobInput == nil {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	beforeJob := jobInput.BeforeJob
	if beforeJob == nil {
		beforeJob = l.hooks.beforeJob
	}

	afterJob := jobInput.AfterJob
	if afterJob == nil {
		afterJob = l.hooks.afterJob
	}

	afterJobError := jobInput.AfterJobError
	if afterJobError == nil {
		afterJobError = l.hooks.afterJobError
	}

	j := &Job{
		JobFn:            jobInput.JobFn,
		Name:             l.uniqueName(jobInput.Name),
		Timeout:          setDefaultDuration(jobInput.Timeout, time.Minute),
		WaitAfterSuccess: setDefaultDuration(jobInput.WaitAfterSuccess, time.Second),
		WaitAfterError:   setDefaultDuration(jobInput.WaitAfterError, time.Second),
		Active:           jobInput.Active,
		BeforeJob:        beforeJob,
		AfterJob:         afterJob,
		AfterJobError:    afterJobError,
		WithLocker:       jobInput.WithLocker,
		locker:           l.locker,
		mu:               sync.RWMutex{},
	}

	l.jobs = append(l.jobs, j)

	return nil
}

func setDefaultDuration(duration time.Duration, defaultDuration time.Duration) time.Duration {
	if duration == time.Duration(0) {
		return defaultDuration
	}

	return duration
}

func (l *Looper) uniqueName(jobInputName string) string {
	var counter int
	for _, j := range l.jobs {
		parts := strings.Split(j.Name, "-")
		jobName := strings.Join(parts[:len(parts)-1], "-")
		if jobName == jobInputName {
			counter++
		}
	}

	return fmt.Sprintf("%s-%v", jobInputName, counter)
}

func (l *Looper) StartJobByName(jobName string) error {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var found bool
	for _, j := range l.jobs {
		j.mu.Lock()
		parts := strings.Split(j.Name, "-")
		name := strings.Join(parts[:len(parts)-1], "-")
		if name == jobName {
			found = true
			if j.Active && !j.Started {
				j.Started = true
				go j.startLoop()
			}
		}

		j.mu.Unlock()
		if found {
			break
		}
	}

	if !found {
		return fmt.Errorf("job with name(%s) not found", jobName)
	}

	return nil
}

func (l *Looper) Start() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.running {
		go l.startJobs()
		l.running = true
	}
}

func (l *Looper) startJobs() {
	if len(l.jobs) == 0 {
		return
	}

	delay := time.Duration(l.startupTime) / time.Duration(len(l.jobs))
	for _, j := range l.jobs {
		j.mu.Lock()
		if j.Active && !j.Started {
			j.Started = true
			go j.startLoop()
			time.Sleep(delay)
		}

		j.mu.Unlock()
	}
}

func (l *Looper) Stop() {
	l.mu.Lock()
	for _, j := range l.jobs {
		j.mu.Lock()
		j.Started = false
		if j.contextCancel != nil {
			j.contextCancel()
		}

		j.mu.Unlock()
	}

	l.mu.Unlock()

	for {
		rj := l.runningJobs()
		if rj == 0 {
			break
		}

		time.Sleep(time.Millisecond * 200)
	}

	l.mu.Lock()
	l.running = false
	l.mu.Unlock()
}

func (j *Job) startLoop() {
	defer func() {
		j.mu.Lock()
		j.Started = false
		j.contextCancel = nil
		j.mu.Unlock()
	}()

	for {
		j.mu.RLock()
		if !j.Active || !j.Started {
			j.mu.RUnlock()
			break
		}
		j.mu.RUnlock()

		start := time.Now()

		j.BeforeJob(j.Name)
		err := j.start()
		if err != nil {
			j.AfterJobError(j.Name, time.Since(start), err)
			time.Sleep(j.WaitAfterError)
		} else {
			j.AfterJob(j.Name, time.Since(start))
			time.Sleep(j.WaitAfterSuccess)
		}
	}
}

func (j *Job) start() error {
	defer func() {
		j.mu.Lock()
		j.Running = false
		j.mu.Unlock()
	}()

	j.mu.Lock()
	j.Running = true
	j.mu.Unlock()

	lo, err := j.lock()
	if err != nil {
		if errors.Is(err, ErrFailedToObtainLock) {
			time.Sleep(time.Second)
			return nil
		}

		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), j.Timeout)
	defer cancel()

	j.contextCancel = cancel

	err = j.run(ctx)
	if err != nil {
		errLock := j.unlock(lo)
		if errLock != nil {
			return errors.Join(err, errLock)
		}

		return err
	}

	err = j.unlock(lo)
	if err != nil {
		return err
	}

	return nil
}

func (j *Job) lock() (lo lock, err error) {
	if j.WithLocker {
		lo, err = j.locker.lock(context.Background(), j.Name, j.Timeout)
	}

	return lo, err
}

func (j *Job) unlock(lo lock) (err error) {
	if j.WithLocker {
		return lo.unlock(context.Background())
	}

	return nil
}

func (j *Job) run(ctx context.Context) (err error) {
	defer func() {
		j.mu.Lock()
		defer j.mu.Unlock()

		j.LastRun = time.Now()

		r := recover()
		if r != nil {
			recErr, ok := r.(error)
			if ok {
				err = recErr
			} else {
				err = fmt.Errorf("%v", r)
			}

			if panicHandler != nil {
				panicHandler(j.Name, r)
			}
		}

		if err != nil {
			j.RunCountError++
			j.LastError = err
		} else {
			j.RunCountSuccess++
		}
	}()

	err = j.JobFn(ctx)

	return err
}

func (l *Looper) runningJobs() (count int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, j := range l.jobs {
		j.mu.RLock()
		if j.Running {
			count++
		}

		j.mu.RUnlock()
	}

	return count
}

func (l *Looper) Jobs() []*Job {
	return l.jobs
}
