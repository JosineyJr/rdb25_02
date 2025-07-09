package routing

import (
	"sync"
	"time"
)

type PIDController struct {
	Kp, Ki, Kd    float64
	setpoint      State
	integral      float64
	prevError     float64
	prevTimestamp time.Time
	mutex         sync.Mutex
}

func NewPIDController(kp, ki, kd float64, target State) *PIDController {
	return &PIDController{
		Kp: kp, Ki: ki, Kd: kd,
		setpoint:      target,
		prevTimestamp: time.Now(),
	}
}

func (pid *PIDController) Compute(current State) float64 {
	pid.mutex.Lock()
	defer pid.mutex.Unlock()
	now := time.Now()
	dt := now.Sub(pid.prevTimestamp).Seconds()
	pid.prevTimestamp = now
	err := (current.Latency - pid.setpoint.Latency) + (current.ErrorRate - pid.setpoint.ErrorRate)
	pid.integral += err * dt
	derivative := 0.0
	if dt > 0 {
		derivative = (err - pid.prevError) / dt
	}
	pid.prevError = err
	out := pid.Kp*err + pid.Ki*pid.integral + pid.Kd*derivative
	if out < 0 {
		out = 0
	} else if out > 1 {
		out = 1
	}
	return out
}
