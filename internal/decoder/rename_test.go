package decoder

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ccdcoe/go-peek/pkg/events"
)

func TestRename(t *testing.T) {
	ren, err := NewRename("/tmp/")
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	pretty := ren.Check("my.awesome.server")
	fmt.Println(pretty)
	fmt.Println(ren.ByName)
	fmt.Println(ren.NameSet.Get(pretty))
}

func AsyncNameCheck(workers int, input chan events.Event, ren *Rename) chan string {
	var output = make(chan string)

	go func() {
		var wg sync.WaitGroup
		defer close(output)

		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

			loop:
				for {
					select {
					case msg, ok := <-input:
						if !ok {
							break loop
						}
						pretty := ren.Check(msg.Source().IP)
						output <- pretty
					}
				}
			}(i)
		}
		wg.Wait()
	}()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ticker.C:
				fmt.Println(ren.ByName.RawValues())
			}
		}
	}()

	return output
}

func TestAsyncRename(t *testing.T) {
	var messages = make(chan events.Event)
	ren, _ := NewRename("/tmp/")

	names := AsyncNameCheck(4, messages, ren)

	go func() {
		for i := 0; i < 10; i++ {
			messages <- events.NewSyslogTestMessage("my.awesome.server")
		}
		close(messages)
	}()

	var (
		lastname string
		count    int
	)
loop:
	for name := range names {
		if count > 0 {
			if name != lastname {
				t.Errorf("error on %d %s != %s", count, lastname, name)
				t.Fail()
				break loop
			}
		}
		lastname = name
		count++
	}

}
