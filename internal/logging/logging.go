package logging

const bufsize = 256

type LogHandler interface {
	LogListener
	LogSender
}

type LogListener interface {
	Notifications() <-chan interface{}
	Errors() <-chan error
}

type LogSender interface {
	Notify(interface{}) bool
	Error(error) bool
}

type loghandle struct {
	notifications chan interface{}
	errors        chan error
	ringsize      int
}

func NewLogHandler() LogHandler {
	return &loghandle{
		ringsize:      bufsize,
		errors:        make(chan error, bufsize),
		notifications: make(chan interface{}, bufsize),
	}
}

func (l *loghandle) Notifications() <-chan interface{} {
	return l.notifications
}

func (l *loghandle) Errors() <-chan error {
	return l.errors
}

func (l *loghandle) Notify(msg interface{}) (full bool) {
	if l.ringsize > 0 && len(l.notifications) == l.ringsize {
		<-l.notifications
		full = true
	}
	l.notifications <- msg
	return full
}

func (l *loghandle) Error(err error) (full bool) {
	if l.ringsize > 0 && len(l.errors) == l.ringsize {
		<-l.errors
		full = true
	}
	l.errors <- err
	return full
}
