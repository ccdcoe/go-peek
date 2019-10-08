package utils

import "context"

type WorkerStoppers []WorkerStopper

func NewWorkerStoppers(count int) WorkerStoppers {
	if count < 1 {
		return nil
	}
	w := make(WorkerStoppers, count)
	for i := 0; i < count; i++ {
		w[i] = *NewWorkerStopper(i)
	}
	return w
}

type WorkerStopper struct {
	Id     int
	Ctx    context.Context
	Cancel context.CancelFunc
}

func NewWorkerStopper(id int) *WorkerStopper {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerStopper{
		Id:     id,
		Ctx:    ctx,
		Cancel: cancel,
	}
}
