package main

import (
	"fmt"
	"encoding/json"

	"github.com/nsqio/go-nsq"

	"../simple-nsq/user"
)

func main(){
	config	:= nsq.NewConfig()
	w, _ 	:= nsq.NewProducer("127.0.0.1:4150", config)

	currentUserCount	:= 21
	userId				:= currentUserCount
	username			:= fmt.Sprintf("user%d", currentUserCount)
	firstName			:= fmt.Sprintf("User %d", currentUserCount)
	lastName			:= "User"

	usr:= user.User{
		UserId: userId,
		Username: username,
		FirstName: firstName,
		LastName: lastName,
	}
	msg, _:= json.Marshal(usr)

	err := w.Publish("TopicNSQ", msg)
	if err != nil {
		panic(err)
	}

	w.Stop()
}