package mysql

import (
	"database/sql"
	"sync"
)

type DBNode struct {
	ID   int
	DB   *sql.DB
	Next *DBNode
}

type DBList struct {
	mu   sync.Mutex
	cur  *DBNode
	Head *DBNode
	Last *DBNode
}

func (list *DBList) Append(node *DBNode) {
	list.mu.Lock()
	defer list.mu.Unlock()
	if list.Head == nil {
		list.Head = node
		list.Last = node
		list.cur = node
	} else {
		list.Last.Next = node
		list.Last = node
	}
}

func (list *DBList) Empty() bool {
	return list.Head == list.Last && list.Head == nil
}

func (list *DBList) NextNode() (res *DBNode) {
	list.mu.Lock()
	defer list.mu.Unlock()
	if list.Empty() {
		return
	}
	res = list.cur
	list.cur = list.cur.Next
	if list.cur == nil {
		list.cur = list.Head
	}
	return
}
