package mysql

import (
	"database/sql"
	"errors"
	"sync"

	"github.com/go-sql-driver/mysql"
)

var ErrNoUseableDB = errors.New("no usealbe mysql")
var ErrNoDBInArr = errors.New("no db in the arr")

type DList struct {
	mu  sync.Mutex
	cur int
	arr []*sql.DB
}

func (l *DList) Cursor() (*sql.DB, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	length := len(l.arr)
	if length == 0 {
		return nil, -1
	}
	res := l.arr[l.cur]
	index := l.cur
	l.cur++
	if l.cur >= length {
		l.cur = 0
	}
	return res, index
}

func (l *DList) Append(db *sql.DB) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.arr = append(l.arr, db)
}

type Client struct {
	wList *DList
	rList *DList
}

func NewClient() *Client {
	return &Client{wList: &DList{}, rList: &DList{}}
}

func (this *Client) AppendReaderDB(db *sql.DB) {
	this.rList.Append(db)
}

func (this *Client) AppendWriteDB(db *sql.DB) {
	this.wList.Append(db)
}

func (this *Client) AppendMaster(user string, pwd string, addr string, dbName string) error {
	conf := config(user, pwd, addr, dbName)
	d, err := db(conf)
	if err != nil {
		return err
	}
	this.AppendWriteDB(d)
	return nil
}

func (this *Client) AppendSlave(user string, pwd string, addr string, dbName string) error {
	conf := config(user, pwd, addr, dbName)
	d, err := db(conf)
	if err != nil {
		return err
	}
	this.AppendReaderDB(d)
	return nil
}

func (this *Client) getReader() (res *sql.DB, err error) {
	return getDB(this.rList)
}

func (this *Client) getWriter() (res *sql.DB, err error) {
	return getDB(this.wList)
}

func getDB(l *DList) (db *sql.DB, err error) {
	db, flag := l.Cursor()
	if db == nil {
		err = ErrNoDBInArr
		return
	}

	index := flag
	for {
		if db.Ping() == nil {
			break
		}

		//log here
		db, index = l.Cursor()
		if index == flag {
			err = ErrNoUseableDB
			break
		}
	}
	return
}

func config(user string, pwd string, addr string, dbName string) *mysql.Config {
	res := &mysql.Config{}
	res.User = user
	res.Passwd = pwd
	res.Addr = addr
	res.DBName = dbName
	return res
}

func db(conf *mysql.Config) (res *sql.DB, err error) {
	dsn := conf.FormatDSN()
	res, err = sql.Open("mysql", dsn)
	return
}

func (this *Client) Insert(sql string, args ...interface{}) (id int64, err error) {
	db, err := this.getWriter()
	if err != nil {
		return
	}
	res, err := db.Exec(sql, args...)
	if err != nil {
		return
	}

	id, err = res.LastInsertId()
	return
}

func (this *Client) Update(sql string, args ...interface{}) (lines int64, err error) {
	db, err := this.getWriter()
	if err != nil {
		return
	}
	res, err := db.Exec(sql, args...)
	if err != nil {
		return
	}

	lines, err = res.RowsAffected()
	return
}

func (this *Client) Delete(sql string, args ...interface{}) (lines int64, err error) {
	db, err := this.getWriter()
	if err != nil {
		return
	}
	res, err := db.Exec(sql, args...)
	if err != nil {
		return
	}
	lines, err = res.RowsAffected()
	return
}

func (this *Client) FetchRows(sql string, args ...interface{}) (res []map[string]string, err error) {
	db, err := this.getReader()
	if err != nil {
		return
	}
	return readRows(db, sql, args...)
}

func readRows(dbsql *sql.DB, sql string, args ...interface{}) ([]map[string]string, error) {
	rows, err := dbsql.Query(sql, args...)

	if err != nil {
		return nil, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([][]byte, len(columns))
	scanArgs := make([]interface{}, len(values))
	var rets = make([]map[string]string, 0)

	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		var ret = make(map[string]string) //这里要注意(对语法的理解)

		var value string
		for i, col := range values {
			if col == nil {
				value = "" //把数据表中所有为null的地方改成“”
			} else {
				value = string(col)
			}

			ret[columns[i]] = value
		}

		rets = append(rets, ret)
	}

	return rets, err
}
