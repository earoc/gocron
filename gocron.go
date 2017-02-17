// goCron : A Golang Job Scheduling Package.
//
// An in-process scheduler for periodic jobs that uses the builder pattern
// for configuration. Schedule lets you run Golang functions periodically
// at pre-determined intervals using a simple, human-friendly syntax.
//
// Inspired by the Ruby module clockwork <https://github.com/tomykaira/clockwork>
// and
// Python package schedule <https://github.com/dbader/schedule>
//
// See also
// http://adam.heroku.com/past/2010/4/13/rethinking_cron/
// http://adam.heroku.com/past/2010/6/30/replace_cron_with_clockwork/
//
// Copyright 2014 Jason Lyu. jasonlvhit@gmail.com .
// All rights reserved.
// Use of this source code is governed by a BSD-style .
// license that can be found in the LICENSE file.
package gocron

import (
	"reflect"
	"runtime"
	"sort"
	"sync"
	"time"
)

// Time location, default set by the time.Local (*time.Location)
var loc = time.Local

// Change the time location
func ChangeLoc(newLocation *time.Location) {
	loc = newLocation
}

// Max number of jobs, hack it if you need.
const MAXJOBNUM = 10000

type Job struct {

	// pause interval * unit bettween runs
	interval uint64

	// the job jobFunc to run, func[jobFunc]
	jobFunc string
	// time units, ,e.g. 'minutes', 'hours'...
	unit string
	// optional time at which this job runs
	atTime string

	// datetime of last run
	lastRun time.Time
	// datetime of next run
	nextRun time.Time
	// cache the period between last an next run
	period time.Duration

	// Specific day of the week to start on
	startDay time.Weekday

	// Map for the function task store
	funcs map[string]interface{}

	// Map for function and  params of function
	fparams map[string]([]interface{})

	m sync.RWMutex
}

// Create a new job with the time interval.
func NewJob(interval uint64) *Job {
	return &Job{
		interval: interval,
		jobFunc:  "",
		unit:     "",
		atTime:   "",
		lastRun:  time.Unix(0, 0),
		nextRun:  time.Unix(0, 0),
		period:   0,
		startDay: time.Sunday,
		funcs:    make(map[string]interface{}),
		fparams:  make(map[string]([]interface{})),
	}
}

// True if the job should be run now
func (j *Job) shouldRun() bool {
	j.m.RLock()
	defer j.m.RUnlock()
	return time.Now().After(j.nextRun)
}

//Run the job and immdiately reschedulei it
func (j *Job) run() {
	f := reflect.ValueOf(j.funcs[j.jobFunc])
	params := j.fparams[j.jobFunc]
	in := make([]reflect.Value, len(params))
	for k, param := range params {
		in[k] = reflect.ValueOf(param)
	}
	j.lastRun = time.Now()
	j.scheduleNextRun()
	go f.Call(in) // fix the bug https://github.com/jasonlvhit/gocron/issues/16

}

// for given function fn , get the name of funciton.
func getFunctionName(fn interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf((fn)).Pointer()).Name()
}

// Specifies the jobFunc that should be called every time the job runs
func (j *Job) Do(jobFun interface{}, params ...interface{}) {
	typ := reflect.TypeOf(jobFun)
	if typ.Kind() != reflect.Func {
		panic("only function can be schedule into the job queue.")
	}

	fname := getFunctionName(jobFun)
	j.m.Lock()
	j.funcs[fname] = jobFun
	j.fparams[fname] = params
	j.jobFunc = fname
	//schedule the next run
	j.scheduleNextRun()
	j.m.Unlock()
}

func (j *Job) DoImmediate(jobFun interface{}, params ...interface{}) {
	typ := reflect.TypeOf(jobFun)
	if typ.Kind() != reflect.Func {
		panic("only function can be schedule into the job queue.")
	}

	fname := getFunctionName(jobFun)
	j.m.Lock()
	j.funcs[fname] = jobFun
	j.fparams[fname] = params
	j.jobFunc = fname
	j.m.Unlock()
	//immediate run
	j.run()
}

func (j *Job) at(t string) *Job {
	hour := int((t[0]-'0')*10 + (t[1] - '0'))
	min := int((t[3]-'0')*10 + (t[4] - '0'))
	if hour < 0 || hour > 23 || min < 0 || min > 59 {
		panic("time format error.")
	}
	// time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	mock := time.Date(time.Now().Year(), time.Now().Month(), time.Now().Day(), int(hour), int(min), 0, 0, loc)

	if j.unit == "days" {
		if time.Now().After(mock) {
			j.lastRun = mock
		} else {
			j.lastRun = time.Date(time.Now().Year(), time.Now().Month(), time.Now().Day()-1, hour, min, 0, 0, loc)
		}
	} else if j.unit == "weeks" {
		if time.Now().After(mock) {
			i := mock.Weekday() - j.startDay
			if i < 0 {
				i = 7 + i
			}
			j.lastRun = time.Date(time.Now().Year(), time.Now().Month(), time.Now().Day()-int(i), hour, min, 0, 0, loc)
		} else {
			j.lastRun = time.Date(time.Now().Year(), time.Now().Month(), time.Now().Day()-7, hour, min, 0, 0, loc)
		}
	}
	return j
}

//	s.Every(1).Day().At("10:30").Do(task)
//	s.Every(1).Monday().At("10:30").Do(task)
func (j *Job) At(t string) *Job {
	j.m.Lock()
	defer j.m.Unlock()
	return j.at(t)
}

//Compute the instant when this job should run next
func (j *Job) scheduleNextRun() {
	if j.lastRun == time.Unix(0, 0) {
		if j.unit == "weeks" {
			i := time.Now().Weekday() - j.startDay
			if i < 0 {
				i = 7 + i
			}
			j.lastRun = time.Date(time.Now().Year(), time.Now().Month(), time.Now().Day()-int(i), 0, 0, 0, 0, loc)

		} else {
			j.lastRun = time.Now()
		}
	}

	if j.period != 0 {
		// translate all the units to the Seconds
		j.nextRun = j.lastRun.Add(j.period * time.Second)
	} else {
		switch j.unit {
		case "minutes":
			j.period = time.Duration(j.interval * 60)
			break
		case "hours":
			j.period = time.Duration(j.interval * 60 * 60)
			break
		case "days":
			j.period = time.Duration(j.interval * 60 * 60 * 24)
			break
		case "weeks":
			j.period = time.Duration(j.interval * 60 * 60 * 24 * 7)
			break
		case "seconds":
			j.period = time.Duration(j.interval)
		}
		j.nextRun = j.lastRun.Add(j.period * time.Second)
	}
}

// the follow functions set the job's unit with seconds,minutes,hours...

func (j *Job) second() *Job {
	if j.interval != 1 {
		panic("")
	}
	return j.seconds()
}

// Set the unit with second
func (j *Job) Second() *Job {
	j.m.Lock()
	defer j.m.Unlock()
	return j.second()
}

func (j *Job) seconds() *Job {
	j.unit = "seconds"
	return j
}

// Set the unit with seconds
func (j *Job) Seconds() *Job {
	j.m.Lock()
	defer j.m.Unlock()
	return j.seconds()
}

// Set the unit  with minute, which interval is 1
func (j *Job) minute() *Job {
	if j.interval != 1 {
		panic("")
	}
	return j.minutes()

}

func (j *Job) Minute() *Job {
	j.m.Lock()
	defer j.m.Unlock()
	return j.minute()
}

//set the unit with minute
func (j *Job) minutes() *Job {
	j.unit = "minutes"
	return j
}

func (j *Job) Minutes() *Job {
	j.m.Lock()
	defer j.m.Unlock()
	return j.minutes()
}

//set the unit with hour, which interval is 1
func (j *Job) hour() *Job {
	if j.interval != 1 {
		panic("")
	}
	return j.hours()
}

func (j *Job) Hour() *Job {
	j.m.Lock()
	defer j.m.Unlock()
	return j.hour()
}

// Set the unit with hours
func (j *Job) hours() *Job {
	j.unit = "hours"
	return j
}

func (j *Job) Hours() *Job {
	j.m.Lock()
	defer j.m.Unlock()
	return j.hours()
}

// Set the job's unit with day, which interval is 1
func (j *Job) day() *Job {
	if j.interval != 1 {
		panic("")
	}
	return j.days()
}

func (j *Job) Day() *Job {
	j.m.Lock()
	defer j.m.Unlock()
	return j.day()
}

// Set the job's unit with days
func (j *Job) days() *Job {
	j.unit = "days"
	return j
}

func (j *Job) Days() *Job {
	j.m.Lock()
	defer j.m.Unlock()
	return j.days()
}

// s.Every(1).Monday().Do(task)
// Set the start day with Monday
func (j *Job) monday() *Job {
	if j.interval != 1 {
		panic("")
	}
	j.startDay = 1
	return j.weeks()
}

func (j *Job) Monday() *Job {
	j.m.Lock()
	defer j.m.Unlock()
	return j.monday()
}

// Set the start day with Tuesday
func (j *Job) tuesday() *Job {
	if j.interval != 1 {
		panic("")
	}
	j.startDay = 2
	return j.weeks()
}

func (j *Job) Tuesday() *Job {
	j.m.Lock()
	defer j.m.Unlock()
	return j.tuesday()
}

// Set the start day woth Wednesday
func (j *Job) wednesday() *Job {
	if j.interval != 1 {
		panic("")
	}
	j.startDay = 3
	return j.weeks()

}

func (j *Job) Wednesday() *Job {
	j.m.Lock()
	defer j.m.Unlock()
	return j.wednesday()
}

// Set the start day with thursday
func (j *Job) thursday() *Job {
	if j.interval != 1 {
		panic("")
	}
	j.startDay = 4
	return j.weeks()
}

func (j *Job) Thursday() *Job {
	j.m.Lock()
	defer j.m.Unlock()
	return j.thursday()
}

// Set the start day with friday
func (j *Job) friday() *Job {
	if j.interval != 1 {
		panic("")
	}
	j.startDay = 5
	return j.weeks()
}

func (j *Job) Friday() *Job {
	j.m.Lock()
	defer j.m.Unlock()
	return j.friday()
}

// Set the start day with saturday
func (j *Job) saturday() *Job {
	if j.interval != 1 {
		panic("")
	}
	j.startDay = 6
	return j.weeks()
}

func (j *Job) Saturday() *Job {
	j.m.Lock()
	defer j.m.Unlock()
	return j.saturday()
}

// Set the start day with sunday
func (j *Job) sunday() *Job {
	if j.interval != 1 {
		panic("")
	}
	j.startDay = 0
	return j.weeks()
}

func (j *Job) Sunday() *Job {
	j.m.Lock()
	defer j.m.Unlock()
	return j.sunday()
}

//Set the units as weeks
func (j *Job) weeks() *Job {
	j.unit = "weeks"
	return j
}

func (j *Job) Weeks() *Job {
	j.m.Lock()
	j.m.Unlock()
	return j.weeks()
}

// Class Scheduler, the only data member is the list of jobs.
type Scheduler struct {
	// Array store jobs
	jobs [MAXJOBNUM]*Job

	// Size of jobs which jobs holding.
	size int

	m sync.RWMutex
}

// Scheduler implements the sort.Interface{} for sorting jobs, by the time nextRun

func (s *Scheduler) Len() int {
	s.m.RLock()
	defer s.m.RUnlock()
	return s.size
}

func (s *Scheduler) swap(i, j int) {
	s.jobs[i], s.jobs[j] = s.jobs[j], s.jobs[i]
}

func (s *Scheduler) Swap(i, j int) {
	s.m.Lock()
	s.swap(i, j)
	defer s.m.Unlock()
}

func (s *Scheduler) less(i, j int) bool {
	return s.jobs[j].nextRun.After(s.jobs[i].nextRun)
}

func (s *Scheduler) Less(i, j int) bool {
	s.jobs[i].m.RLock()
	defer s.jobs[i].m.RUnlock()
	return s.less(i, j)
}

// Create a new scheduler
func NewScheduler() *Scheduler {
	return &Scheduler{
		jobs: [MAXJOBNUM]*Job{},
		size: 0,
	}
}

// Get the current runnable jobs, which shouldRun is True
func (s *Scheduler) getRunnableJobs() ([MAXJOBNUM]*Job, int) {
	runnableJobs := [MAXJOBNUM]*Job{}
	n := 0
	sort.Sort(s)
	s.m.RLock()
	defer s.m.RUnlock()
	for i := 0; i < s.size; i++ {
		if s.jobs[i].shouldRun() {

			runnableJobs[n] = s.jobs[i]
			//fmt.Println(runnableJobs)
			n++
		} else {
			break
		}
	}
	return runnableJobs, n
}

// Datetime when the next job should run.
func (s *Scheduler) NextRun() (*Job, time.Time) {
	if s.size <= 0 {
		return nil, time.Now()
	}
	s.m.Lock()
	sort.Sort(s)
	s.m.Unlock()
	return s.jobs[0], s.jobs[0].nextRun
}

// Schedule a new periodic job
func (s *Scheduler) every(interval uint64) *Job {
	job := NewJob(interval)
	s.jobs[s.size] = job
	s.size++
	return job
}

func (s *Scheduler) Every(interval uint64) *Job {
	s.m.Lock()
	defer s.m.Unlock()
	return s.every(interval)
}

// Run all the jobs that are scheduled to run.
func (s *Scheduler) RunPending() {
	runnableJobs, n := s.getRunnableJobs()

	if n != 0 {
		for i := 0; i < n; i++ {
			runnableJobs[i].run()
		}
	}
}

// Run all jobs regardless if they are scheduled to run or not
func (s *Scheduler) RunAll() {
	for i := 0; i < s.size; i++ {
		s.m.Lock()
		s.jobs[i].run()
		s.m.Unlock()
	}
}

func (s *Scheduler) RunOne(j interface{}) {
	i := 0
	s.m.RLock()
	defer s.m.RUnlock()
	for ; i < s.size; i++ {
		if s.jobs[i].jobFunc == getFunctionName(j) {
			s.jobs[i].run()
		}
	}
}

// Run all jobs with delay seconds
func (s *Scheduler) RunAllwithDelay(d int) {
	for i := 0; i < s.size; i++ {
		s.jobs[i].run()
		time.Sleep(time.Duration(d))
	}
}

// Remove specific job j
func (s *Scheduler) remove(j interface{}) {
	i := 0
	for ; i < s.size; i++ {
		if s.jobs[i].jobFunc == getFunctionName(j) {
			break
		}
	}

	for j := (i + 1); j < s.size; j++ {
		s.jobs[i] = s.jobs[j]
		i++
	}
	s.size = s.size - 1
}

func (s *Scheduler) Remove(j interface{}) {
	s.m.Lock()
	s.remove(j)
	s.m.Unlock()
}

// Delete all scheduled jobs
func (s *Scheduler) Clear() {
	for i := 0; i < s.size; i++ {
		s.jobs[i] = nil
	}
	s.size = 0
}

// Start all the pending jobs
// Add seconds ticker
func (s *Scheduler) Start() chan bool {
	stopped := make(chan bool, 1)
	ticker := time.NewTicker(1 * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				s.RunPending()
			case <-stopped:
				return
			}
		}
	}()

	return stopped
}

// The following methods are shortcuts for not having to
// create a Schduler instance

var defaultScheduler = NewScheduler()
var jobs = defaultScheduler.jobs

// Schedule a new periodic job
func Every(interval uint64) *Job {
	return defaultScheduler.Every(interval)
}

// Run all jobs that are scheduled to run
//
// Please note that it is *intended behavior that run_pending()
// does not run missed jobs*. For example, if you've registered a job
// that should run every minute and you only call run_pending()
// in one hour increments then your job won't be run 60 times in
// between but only once.
func RunPending() {
	defaultScheduler.RunPending()
}

// Run all jobs regardless if they are scheduled to run or not.
func RunAll() {
	defaultScheduler.RunAll()
}

// Run all the jobs with a delay in seconds
//
// A delay of `delay` seconds is added between each job. This can help
// to distribute the system load generated by the jobs more evenly over
// time.
func RunAllwithDelay(d int) {
	defaultScheduler.RunAllwithDelay(d)
}

// Run all jobs that are scheduled to run
func Start() chan bool {
	return defaultScheduler.Start()
}

// Clear
func Clear() {
	defaultScheduler.Clear()
}

// Remove
func Remove(j interface{}) {
	defaultScheduler.Remove(j)
}

// NextRun gets the next running time
func NextRun() (*Job, time.Time) {
	return defaultScheduler.NextRun()
}
