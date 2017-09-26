package reactor

/*
   @1. Main process subscribe from redis, if get datas, send to SubQueue, and
       start a worker
   @2. worker get request from SubQueue, deal with request
   @3. worker called Suber.Pub to send Response to PubQueue
   @4. puber get data from PubQueue and sent to Redis

   workers deal with request and called Pub function, Pub send response to PubQueue
   and
   redis -> Suber -> dispatch -> Subqueue -> worker -> |
                                          -> worker -> | PubQueue -> puber ->
                                          -> worker -> |                   |
    ^                                                                      |
    |                                                                      |
    <----------------------------------------------------------------------|
*/

import (
	"fmt"
	log "github.com/alecthomas/log4go"
	"github.com/garyburd/redigo/redis"
	"sync"
	"time"
)

var (
	Suber *Subscriber
)

const (
	SubQueueLen         int = 200
	PubQueueLen         int = 20 // block queue
	MaxCurNum           int = 20
	PuberMaxRetry       int = 3
	SubscriberBlockTime int = 1200
)

/* The Data struct which send to pub queue*/
type PubData struct {
	key   string
	value string
}

type Subscriber struct {
	Port      int
	Host      string
	DB        int
	Password  string
	Sem       chan int      // Sem to control the max mutil goroutines at one time
	SubQueue  chan string   // Queue to sent request to goroutines
	PubQueue  chan *PubData // Receive response from goroutine and will be send to Redis
	SubConn   redis.Conn    // Subscriber connection
	PubConn   redis.Conn    // Publisher connection
	PubQuit   chan bool
	syncGroup sync.WaitGroup
}

func InitSubscriber(host string, port int, db int, password string) error {
	dialOptionDb := redis.DialDatabase(db)
	dialOptionPwd := redis.DialPassword(password)

	address := fmt.Sprintf("%s:%d", host, port)
	subconn, errsub := redis.Dial("tcp", address, dialOptionDb, dialOptionPwd)
	pubconn, errpub := redis.Dial("tcp", address, dialOptionDb, dialOptionPwd)

	log.Info("Dail to redis:[redis@%s:%d]", host, port)
	if errsub != nil || errpub != nil {
		log.Error("Redis dial failed, error:%s", errsub)
		return errsub
	}

	Suber = &Subscriber{
		Port:      port,
		Host:      host,
		DB:        db,
		Password:  password,
		Sem:       make(chan int, MaxCurNum),
		SubQueue:  make(chan string, SubQueueLen),
		PubQueue:  make(chan *PubData, PubQueueLen),
		SubConn:   subconn,
		PubConn:   pubconn,
		syncGroup: sync.WaitGroup{},
	}
	/* starting receive response data from PubQueue */
	go Suber.puber(Suber.PubQueue, Suber.PubQuit, &Suber.syncGroup)
	return nil
}

/*
   reconnect to MQ with try count
   @trycount
   0 -> infinity
   N -> try with N count
*/
func (suber *Subscriber) reconnect(trycount int, method string) {
	dialOptionDb := redis.DialDatabase(suber.DB)
	dialOptionPwd := redis.DialPassword(suber.Password)
	address := fmt.Sprintf("%s:%d", suber.Host, suber.Port)

	i := trycount
	for {
		log.Info("Reconnect to MQ:<redis@%s>", address)
		conn, err := redis.Dial("tcp", address, dialOptionDb, dialOptionPwd)
		if err != nil {
			time.Sleep(3 * time.Second)
			if trycount == 0 {
				continue
			}

			if i == 0 {
				break
			} else {
				i--
				continue
			}
		} else {
			if method == "pub" {
				suber.PubConn = conn
			} else {
				suber.SubConn = conn
			}
			return
		}
	}
}

func (suber *Subscriber) dispatch(data string) {
	suber.SubQueue <- data
}

func (suber *Subscriber) Sub(
	key string, onSuccess func(string) error, onError func(string) error) {

	for {
		// TODO: treat timeout as ok, donot reconnect
		ret, err := redis.Strings(
			suber.SubConn.Do("BLPOP", key, SubscriberBlockTime))
		if err != nil {
			suber.reconnect(0, "sub")
		} else if len(ret) > 0 {
			/*Send data to request SubQueue*/
			log.Debug("Push data to inner Channel:%s", ret[1])
			suber.SubQueue <- ret[1]

			/*
				anonymous function, all argument passed directly while calling
				routine should share variables with other processes but should
				with channel.
				control max concurrency numbers
				add one sem number
			*/
			suber.Sem <- 1
			suber.syncGroup.Add(1)
			go func(sem chan int, queue chan string, wg *sync.WaitGroup,
				onSuccess func(string) error, onError func(string) error) {

				data := <-queue
				log.Debug("Get data from Channel:%s", data)
				if err := onSuccess(data); err != nil {
					onError(data)
				}
				/*release sem*/
				<-sem
				wg.Done()
			}(suber.Sem, suber.SubQueue, &suber.syncGroup, onSuccess, onError)
		}
	}
}

func (suber *Subscriber) puber(queue chan *PubData, quit chan bool, wg *sync.WaitGroup) {
	innerChan := make(chan int)
	wg.Add(1)
	for {
		select {
		case data := <-queue:
			key, value := data.key, data.value
			log.Info("Subscriber, puber running with key:%s, value:%s", key, value)
			for i := 0; i < PuberMaxRetry; i++ {
				if ret, err := redis.Int(suber.PubConn.Do("RPUSH", key, value)); err != nil {
					log.Error("Subscriber, redis connection error, %s", err)
					suber.reconnect(5, "pub")
				} else {
					log.Info("Subscriber, Push data:%s to queue:%s, and ret:%d", value, key, ret)
					break
				}
			}
		case <-quit:
			log.Info("Subscriber, puber receive quit signal")
			goto clean
		case <-innerChan:
			goto clean
		}
	}
clean:
	wg.Done()
}

/*
Publish data to PubQueue, this operation may be sync
and the puber would receive it and send to Redis
*/
func (suber *Subscriber) Pub(key string, value string) error {
	suber.PubQueue <- &PubData{key: key, value: value}
	return nil
}

func (suber *Subscriber) Close() {

	/* send quit signal to puber */
	suber.PubQuit <- true
	/* waiting for goroutine finish */
	suber.syncGroup.Wait()

	close(suber.SubQueue)
	close(suber.PubQueue)
	close(suber.Sem)
	close(suber.PubQuit)

	if suber.PubConn != nil {
		suber.PubConn.Close()
	}
	if suber.SubConn != nil {
		suber.SubConn.Close()
	}
}
