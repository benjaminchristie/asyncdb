package asyncdb

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

type TestingValue struct {
	Gender string `json:"gender"`
	Type   string `json:"type"`
	Count  int    `json:"count"`
}

func genderCheck(gender string, e map[string]string) bool {
	_, exists := e[gender]
	return exists
}

func TestAsync(t *testing.T) {
	var wg sync.WaitGroup
	db := MakeDB[string, map[string]string]()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			value := make(map[string]string)
			key := fmt.Sprintf("user%d", i)
			value["count"] = strconv.Itoa(i)
			AddItem(db, key, value)
		}(i)
	}
	wg.Wait()
	for i := 0; i < 100; i++ {
		s, err := GetValueFromKey(db, fmt.Sprintf("user%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if s["count"] != strconv.Itoa(i) {
			t.Fatal(s)
		}
	}
	t.Log("Success!")
	db = MakeDB[string, map[string]string]()
	value := make(map[string]string)
	key := fmt.Sprintf("user%d", 0)
	value["count"] = strconv.Itoa(0)
	AddItem(db, key, value)
	for i := 0; i < 99; i++ {
		wg.Add(1)
		go func(j int) {

			defer wg.Done()
			value := make(map[string]string)
			key := fmt.Sprintf("user%d", j)
			value["count"] = strconv.Itoa(j)
			AddItem(db, key, value)

			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				key := fmt.Sprintf("user%d", k)
				value := make(map[string]string)
				value["count"] = strconv.Itoa(k)
				AddItem(db, key, value)
			}(j + 1)

			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				key := fmt.Sprintf("user%d", k-1)
				value := make(map[string]string)
				value["count"] = strconv.Itoa(k - 1)
				InsertInto(db, key, value)
			}(j + 1)

			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				key := fmt.Sprintf("user%d", k-1)
				value := make(map[string]string)
				value["count"] = strconv.Itoa(k - 1)
				InsertInto(db, key, value)
			}(j + 1)

		}(i)
	}

	value = make(map[string]string)
	key = fmt.Sprintf("user%d", 100)
	value["count"] = strconv.Itoa(100)
	AddItem(db, key, value)
	wg.Wait()

	for i := 0; i < 100; i++ {
		s, err := GetValueFromKey(db, fmt.Sprintf("user%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if s["count"] != strconv.Itoa(i) {
			t.Fatal(s)
		}
	}

	db = MakeDB[string, map[string]string]()

	AddItem(db, key, value)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(j int) {

			defer wg.Done()
			value := make(map[string]string)
			key := fmt.Sprintf("user%d", j)
			value["count"] = strconv.Itoa(j)
			time.Sleep(time.Duration(float32(time.Microsecond) * 10 * rand.Float32()))
			AddItem(db, key, value)
			time.Sleep(time.Duration(float32(time.Microsecond) * 10 * rand.Float32()))
			ChangeItem(db, key, value)

		}(i)
	}
	wg.Wait()
	for i := 0; i < 100; i++ {
		s, err := GetValueFromKey(db, fmt.Sprintf("user%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if s["count"] != strconv.Itoa(i) {
			t.Fatal(s)
		}
	}
}

func TestSync(t *testing.T) {
	var strings []string
	var m map[string]string
	var err error
	db := MakeDB[string, map[string]string]()
	strings = SelectFrom(db, func(e map[string]string) bool { return genderCheck("male", e) })
	if len(strings) != 0 {
		t.Fatalf("%s", strings[0])
	}
	val1 := make(map[string]string)
	val1["male"] = "true"
	AddItem(db, "user1", val1)
	strings = SelectFrom(db, func(e map[string]string) bool { return genderCheck("male", e) })
	if len(strings) == 0 {
		t.Fatalf("%s", strings)
	}
	if strings[0] != "user1" {
		t.Fatalf("%s", strings[0])
	}
	val1["male"] = "false"
	err = Update(db, "user1", val1)
	if err != nil {
		t.Fatal(err)
	}
	m, err = GetValueFromKey(db, "user1")
	if err != nil {
		t.Fatal(err)
	}
	if m["male"] != "false" {
		t.Fatal(m)
	}
	t.Log("Success!")
}
