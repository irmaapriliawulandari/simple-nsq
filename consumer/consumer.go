package main

import (
	"fmt"
	"sync"
	"log"
	"time"
	"errors"
	"encoding/json"
	"database/sql"

	"github.com/nsqio/go-nsq"
	_ "github.com/lib/pq"
	"github.com/gomodule/redigo/redis"

	"../user"
)

func main(){
	wg := &sync.WaitGroup{}
	wg.Add(1)
  
	config := nsq.NewConfig()
	q, _ := nsq.NewConsumer("TopicNSQ", "Channel1", config)
	q.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		err := isTimeValid()
		if err != nil {
			log.Println(err)
			delayReq := 20 * time.Second
			message.RequeueWithoutBackoff(delayReq)
		} else {
			usr := user.User{}
			err = json.Unmarshal([]byte(string(message.Body)), &usr)
			
			if err != nil {
				log.Panic(err)
			}

			err = isUserExisting(usr.Username)
			if err != nil {
				log.Println(err)
			} else {
				err = createUser(&usr)
				if err != nil {
					log.Println(err)
				} else {
					log.Printf("new user added with id:%d", usr.UserId)
					getRedisData(&usr)
				}
			}
			
			wg.Done()
		}
		return nil
	}))
	err := q.ConnectToNSQD("127.0.0.1:4150")
	if err != nil {
		log.Panic("Could not connect")
	}
	wg.Wait()
}

func isUserExisting(username string) error {
	pool := newPool()
	conn := pool.Get()
	defer conn.Close()

	//ping to test connectivity
	err := ping(conn)
	if err != nil {
		panic(err)
	}

	_, err = redis.String(conn.Do("GET", username))
	if err == redis.ErrNil {
		return nil
	} else if err != nil {
		return (err)
	}
	
	return errors.New(fmt.Sprintf("username %s existed", username))
}

func isTimeValid() error {
	now 	:= time.Now()
	minute	:= now.Minute()

	if minute % 2 == 0 {
		return nil
	}

	return errors.New("not the time yet")
}

const (
	host		= "localhost"
	port		= 5432
	dbuser		= "postgres"
	password	= "1234"
	dbname		= "postgres"
)

func createUser(usr *user.User) error {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, dbuser, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)

	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	fmt.Println("db connected!")

	//insert data to db
	sqlStatement := `INSERT into "users" (username, first_name, last_name)
	VALUES ($1, $2, $3)
	RETURNING user_id`

	UserId := 0
	err = db.QueryRow(sqlStatement, usr.Username, usr.FirstName, usr.LastName).Scan(&UserId)
	usr.UserId = UserId

	return err
}

func getRedisData(usr *user.User){
	pool := newPool()
	conn := pool.Get()
	defer conn.Close()

	//ping to test connectivity
	err := ping(conn)
	if err != nil {
		panic(err)
	}
	fmt.Println("redis connected!")

	err = setUser(conn, usr.UserId, usr.Username, usr.FirstName, usr.LastName)
	err = getuser(conn, usr.UserId)
}

func setUser(c redis.Conn, userId int, username, firstName, lastName string) error {
	usr := user.User{
		UserId: userId, 
		Username: username, 
		FirstName: firstName, 
		LastName: lastName,
	}
	json, err := json.Marshal(usr)
	if err != nil {
		return err
	}

	_, err = c.Do("SET", username, json)
	if err != nil {
		return err
	}
	fmt.Printf("set %s\n", username)

	return nil
}

func getuser(c redis.Conn, lastUserId int) error {
	for i := 1; i <= lastUserId; i++ {
		key := fmt.Sprintf("user%d", i)

		s, err := redis.String(c.Do("GET", key))

		if err != nil {
			return (err)
		}
	
		usr := user.User{}
		err = json.Unmarshal([]byte(s), &usr)
		fmt.Printf("get %s -> %v \n", key, usr)
	}
	
	return nil
}

func newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle: 80,
		MaxActive: 12000,
		Dial: func() (redis.Conn, error){
			c, err := redis.Dial("tcp", ":6379")
			if err != nil{
				panic(err.Error())
			}
			return c, err
		},
	}
}

func ping(c redis.Conn) error {
	_, err := redis.String(c.Do("PING"))
	if err != nil{
		return err
	}

	return nil
}