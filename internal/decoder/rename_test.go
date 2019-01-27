package decoder

import (
	"fmt"
	"os"
	"testing"

	"github.com/ccdcoe/go-peek/internal/types"
)

func TestNewNameSet(t *testing.T) {
	hosts := []string{os.Getenv("ELASTIC_HOST")}
	if hosts[0] == "" {
		hosts[0] = "http://localhost:9200"
	}
	names, err := newSyncNamePool("/tmp", types.ElaTargetInventoryConfig{
		Hosts: hosts,
		Index: "inventory-latest",
	})
	if err != nil {
		t.Fatal(err)
	}
	items := 0
	names.NamePool.Range(func(k, v interface{}) bool {
		items++
		return true
	})
	fmt.Println(items)
	if err := saveNameSetToGob("/tmp/items.gob", names.NamePool); err != nil {
		t.Fatal(err)
	}
	if loaded, err := loadNameSetFromGob("/tmp/items.gob"); err != nil {
		t.Fatal(err)
	} else {
		items2 := 0
		loaded.Range(func(k, v interface{}) bool {
			val, ok := names.NamePool.Load(k)
			if !ok {
				t.Fatalf("%s missing in loaded map", k)
			}
			if val != v {
				t.Fatalf("%s wrong. Should be %s is %s", k, val, v)
			}
			items2++
			return true
		})
		if items2 != items {
			t.Fatalf("actual %d != loaded %d\n", items, items2)
		}
	}
	fmt.Println(names.LenAvailableName)
	names.NameReMap.Range(func(k interface{}, v interface{}) bool {
		fmt.Printf("%s - %s\n", k, v)
		return true
	})
}

/*
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
*/
