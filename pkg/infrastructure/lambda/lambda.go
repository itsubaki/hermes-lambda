package lambda

import (
	"fmt"
	"strings"
)

type Writer interface {
	Write() error
}

type HermesLambda struct {
	Output []string
	Writer map[string]Writer
}

func Default(o []string) *HermesLambda {
	return &HermesLambda{
		Output: o,
		Writer: make(map[string]Writer),
	}
}

func (l *HermesLambda) Add(name string, w Writer) {
	l.Writer[name] = w
}

func (l *HermesLambda) Run() error {
	for _, o := range l.Output {
		w, ok := l.Writer[strings.ToLower(o)]
		if !ok {
			return fmt.Errorf("output=%s not found", o)
		}

		if err := w.Write(); err != nil {
			return fmt.Errorf("write: %v", err)
		}
	}

	return nil
}
