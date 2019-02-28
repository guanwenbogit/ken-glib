package mysql

import (
	"fmt"
	"sync"
	"testing"
)

func newClient(t *testing.T) *Client {
	c := NewClient()
	err := c.AppendMaster("root", "1qaz", "127.0.0.1:3306", "ken_user")
	if err != nil {
		t.Logf("Append Master err=%v", err)
	}
	err = c.AppendMaster("root", "1qaz", "127.0.0.1:3306", "ryu_user")

	err = c.AppendSlave("root", "1qaz", "127.0.0.1:3306", "ken_user")
	if err != nil {
		t.Logf("Append Slave err=%v", err)
	}
	err = c.AppendSlave("root", "1qaz", "127.0.0.1:3306", "ryu_user")
	return c
}

func Test_NewClient(t *testing.T) {
	newClient(t)

}

func Test_Insert(t *testing.T) {
	c := newClient(t)
	sql := "insert into `user_table` (`nick`,`phone`) values (?,?)"
	id, err := c.Insert(sql, "Tom", "15101167473")

	if err != nil {
		t.Logf("Insert err=%v", err)
	}
	t.Logf("Insert end. id=%v", id)
}

func Test_GOInsert(t *testing.T) {
	c := newClient(t)
	var wg sync.WaitGroup
	sql := "insert into `user_table` (`nick`,`phone`) values (?,?)"
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			name := fmt.Sprintf("June_%v", i)
			id, err := c.Insert(sql, name, "15101167473")
			if err != nil {
				t.Logf("Insert err=%v", err)
			}
			t.Logf("Insert end. id=%v", id)
			wg.Done()
		}(i)
	}
	wg.Wait()
}
