package producer

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/Shopify/sarama"
	"net/http"
	"strconv"
	"time"
	"fmt"
)

type Request struct {
	ID int `json:"id"`
	Name  string `json:"name"`
}

var request Request

const (
	PRODUCER_URL string = "localhost:9092"
	KAFKA_TOPIC string = "simple-kafka-golang"
)

func message(c *gin.Context) {

	c.Bind(&request)
	fmt.Println("Hello world! String:&request=", &request)
	reqMarshal,err := json.Marshal(request)

	if err != nil {
		panic(err)
	}

	reqString := string(reqMarshal)
        fmt.Println("Hello world! String:reqString=", reqString)
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	brokers := []string{PRODUCER_URL}
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	strTime := strconv.Itoa(int(time.Now().Unix()))
	 fmt.Println("Hello world! String:strTime", strTime)
	msg := &sarama.ProducerMessage{
		Topic: KAFKA_TOPIC,
		Key:   sarama.StringEncoder(strTime),
		Value: sarama.StringEncoder(reqString),
	}
    	 fmt.Println("Hello world! String:msg", msg)
	producer.Input() <- msg

	resp := gin.H{
		"status": http.StatusOK,
		"message": "Message has been sent.",
		"data": reqString,
	}
        fmt.Println("Hello world! http.StatausOk", http.StatusOK)
	fmt.Println("Hello world! String:resp", resp)
	c.IndentedJSON(http.StatusOK, resp)

}

func Producemethod() {
	//return "produceMethodCalled"
	//message()
	fmt.Println("producermethodcalled");
}
/*
func main() {

	router := gin.Default()
	router.POST("/",message)
	router.Run(":3000")

}
*/
