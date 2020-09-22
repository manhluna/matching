package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
)

// MIN price
const MIN = 0.01

// MAX price
const MAX = 1000000000

// Order struct about info of order
type Order struct {
	OpenID string  `json:"openID"`
	Amount float64 `json:"amount"`
}

var mapOrder = make(map[float64][]Order)

// Unit about info of order
type Unit struct {
	Price    float64 `json:"price"`
	QtyTotal float64 `json:"qtyTotal"`
}

// Book struct about info of order
type Book struct {
	Buy  []Unit `json:"buy"`
	Sell []Unit `json:"sell"`
}

var book = Book{}

var reader = kafka.NewReader(kafka.ReaderConfig{
	Brokers:   []string{Env("KAFKA_HOST")},
	Topic:     "new-order-request-" + Env("LEFT") + Env("RIGHT"),
	Partition: 0,
	MinBytes:  10e3, // 10KB
	MaxBytes:  10e6, // 10MB
})

var writer = kafka.NewWriter(kafka.WriterConfig{
	Brokers:  []string{Env("KAFKA_HOST")},
	Topic:    "new-order-response-" + Env("LEFT") + Env("RIGHT"),
	Balancer: &kafka.LeastBytes{},
})

// InsertUnit : insert a unit to side
func InsertUnit(a []Unit, e Unit, i int) []Unit {
	a = append(a, Unit{0, 0})
	copy(a[i+1:], a[i:])
	a[i] = e
	return a
}

// InsertBook : insert a unit to side
func InsertBook(side []Unit, item Unit, buy bool) []Unit {
	for i := 0; i < len(side); i++ {
		if side[i].Price == item.Price {
			side[i].QtyTotal += item.QtyTotal
			return side
		}
		ins := side[i].Price > item.Price
		if buy {
			ins = side[i].Price < item.Price
		}
		if ins {
			side = InsertUnit(side, item, i)
			return side
		}
	}
	return append(side, item)
}

// FillSell ...
func FillSell(_book Book) []Unit {
	var dec = Env("DEC")
	x, _ := strconv.ParseInt(dec, 10, 64)
	var sc = math.Pow10(-int(x))

	var listPrice []Unit
	for i := 0; i < (len(_book.Sell)-1)/2; i++ {
		amp := _book.Sell[i+1].Price - _book.Sell[i].Price
		if amp > sc*10 {
			var start = _book.Sell[i].Price
			for {
				if _book.Sell[i+1].Price-start <= sc*10 {
					break
				}
				start = start + sc*randomInt(1, 10)
				max := _book.Sell[i].QtyTotal
				min := _book.Sell[i+1].QtyTotal
				if min > max {
					min = _book.Sell[i].QtyTotal
					max = _book.Sell[i+1].QtyTotal
				}
				listPrice = append(listPrice, Unit{start, random(min/20, max/10, 3)})
			}
		}
	}
	return listPrice
}

// FillBuy ...
func FillBuy(_book Book) []Unit {
	var dec = Env("DEC")
	x, _ := strconv.ParseInt(dec, 10, 64)
	var sc = math.Pow10(-int(x))

	var listPrice []Unit
	for i := 0; i < (len(_book.Buy)-1)/2; i++ {
		amp := _book.Buy[i+1].Price - _book.Buy[i].Price
		if amp > sc*10 {
			var start = _book.Buy[i].Price
			for {
				if _book.Buy[i+1].Price-start <= sc*10 {
					break
				}
				start = start + sc*randomInt(1, 10)
				max := _book.Buy[i].QtyTotal
				min := _book.Buy[i+1].QtyTotal
				if min > max {
					min = _book.Buy[i].QtyTotal
					max = _book.Buy[i+1].QtyTotal
				}
				listPrice = append(listPrice, Unit{start, random(min/20, max/10, 3)})
			}
		}
	}
	return listPrice
}

// Request ...........
type Request struct {
	UserID       string  `json:"userID"`
	OpenID       string  `json:"openID"`
	Type         string  `json:"_type"`
	Side         bool    `json:"side"`
	Price        float64 `json:"price"`
	Amount       float64 `json:"amount"`
	BalanceLeft  float64 `json:"balanceLeft"`
	BalanceRight float64 `json:"balanceRight"`
}

// Response of Neworder
type Response struct {
	UserID string  `json:"userID"`
	OpenID string  `json:"openID"`
	Type   string  `json:"_type"`
	Price  float64 `json:"price"`
	Amount float64 `json:"amount"`
	Left   string  `json:"left"`
	Right  string  `json:"right"`
	Side   bool    `json:"side"`

	Queue     []Order `json:"queue"`
	OrderBook Book    `json:"orderBook"`
	LastPrice float64 `json:"lastPrice"`
}

func newOrder(userID, openID, _type string, side bool, price, amount, balanceLeft, balanceRight float64) Response {
	var response = Response{}
	response.OpenID = openID
	response.UserID = userID

	if side {
		switch _type {
		case "Limit":
			{
				// if Env("MAKER") == "true" {
				// 	var amp = book.Sell[0].Price * 100 / book.Buy[0].Price
				// 	fmt.Println(amp)
				// }
				if amount == 0 || price == 0 {
					response.OrderBook = book
					response.OpenID = " "
					return response
				}

				if price*amount > balanceRight || amount < 0 {
					response.OpenID = "err"
					return response
				}

				_price := price
				if len(book.Sell) > 0 && price >= book.Sell[0].Price {
					_price = MIN
					if len(book.Buy) > 0 {
						_price = book.Buy[0].Price
					}
				}
				book.Buy = InsertBook(book.Buy, Unit{_price, amount}, side)
				mapOrder[_price] = append(mapOrder[_price], Order{openID, amount})
				response.Price = _price
				response.OrderBook = book
				response.Type = _type
				response.Amount = amount
				response.Side = side
				response.Left = Env("LEFT")
				response.Right = Env("RIGHT")

				if Env("MAKER") == "true" {
					_book := book
					seedOrders := FillBuy(_book)
					for _, v := range seedOrders {
						_book.Buy = InsertBook(_book.Buy, v, side)
					}
					response.OrderBook = _book
				}

				return response
			}
		case "Market":
			{
				if amount == 0 {
					response.OrderBook = book
					response.OpenID = " "
					return response
				}
				var ix = -1
				var pe []float64
				var last Unit
				_amount := amount
				var kq []Order

				var totalValue float64
				for i := 0; i < len(book.Sell); i++ {
					if _amount > 0 {
						if _amount >= book.Sell[i].QtyTotal {
							_amount -= book.Sell[i].QtyTotal
							ix = i
							pe = append(pe, book.Sell[i].Price)
							totalValue += book.Sell[i].QtyTotal * book.Sell[i].Price
							response.LastPrice = book.Sell[i].Price
						} else {
							last.Price = book.Sell[i].Price
							last.QtyTotal = _amount
							book.Sell[i].QtyTotal -= _amount
							totalValue += _amount * book.Sell[i].Price
							response.LastPrice = book.Sell[i].Price
							_amount = 0
							break
						}
					}
				}
				if totalValue > balanceRight || amount < 0 {
					response.OpenID = "err"
					return response
				}
				if ix >= 0 {
					book.Sell = book.Sell[ix+1:]
					for _, v := range pe {
						kq = append(kq, mapOrder[v]...)
						delete(mapOrder, v)
					}
				}

				if last.Price > 0 {
					var value = last.QtyTotal
					var _i = -1
					for i := 0; i < len(mapOrder[last.Price]); i++ {
						if value > 0 {
							if value >= mapOrder[last.Price][i].Amount {
								value -= mapOrder[last.Price][i].Amount
								kq = append(kq, mapOrder[last.Price][i])
								_i = i
							} else {
								var temp = mapOrder[last.Price][i]
								temp.Amount = value
								mapOrder[last.Price][i].Amount -= value
								kq = append(kq, temp)
								value = 0
							}
						}
					}
					mapOrder[last.Price] = mapOrder[last.Price][_i+1:]
				}
				response.Queue = kq
				response.Price = totalValue / (amount - _amount)
				response.OrderBook = book
				response.Type = _type
				response.Amount = amount - _amount
				response.Side = side
				response.Left = Env("LEFT")
				response.Right = Env("RIGHT")
				return response
			}
		case "Cancel":
			{
				for i, order := range mapOrder[price] {
					if order.OpenID == openID {
						if len(mapOrder[price]) > 1 {
							mapOrder[price] = append(mapOrder[price][:i], mapOrder[price][i+1:]...)
						} else {
							delete(mapOrder, price)
						}
						break
					}
				}
				if price <= book.Buy[0].Price {
					for i := 0; i < len(book.Buy); i++ {
						if book.Buy[i].Price == price {
							book.Buy[i].QtyTotal -= amount
							if book.Buy[i].QtyTotal == 0 {
								book.Buy = append(book.Buy[:i], book.Buy[i+1:]...)
							}
						}
					}
				}
				response.Price = price
				response.OrderBook = book
				response.Type = _type
				response.Amount = amount
				response.Side = side
				response.Left = Env("LEFT")
				response.Right = Env("RIGHT")
				return response
			}
		}
	} else {
		switch _type {
		case "Limit":
			{
				if amount == 0 || price == 0 {
					response.OrderBook = book
					response.OpenID = " "
					return response
				}

				if amount > balanceLeft || amount < 0 {
					response.OpenID = "err"
					return response
				}

				_price := price
				if len(book.Buy) > 0 && price <= book.Buy[0].Price {
					_price = MAX
					if len(book.Sell) > 0 {
						_price = book.Sell[0].Price
					}
				}
				book.Sell = InsertBook(book.Sell, Unit{_price, amount}, side)
				mapOrder[_price] = append(mapOrder[_price], Order{openID, amount})
				response.Price = _price
				response.OrderBook = book
				response.Type = _type
				response.Amount = amount
				response.Side = side
				response.Left = Env("LEFT")
				response.Right = Env("RIGHT")

				if Env("MAKER") == "true" {
					_book := book
					seedOrders := FillSell(_book)
					for _, v := range seedOrders {
						_book.Sell = InsertBook(_book.Sell, v, side)
					}
					response.OrderBook = _book
				}

				return response
			}
		case "Market":
			{
				if amount == 0 {
					response.OrderBook = book
					response.OpenID = " "
					return response
				}
				if amount > balanceLeft || amount < 0 {
					response.OpenID = "err"
					return response
				}

				var ix = -1
				var pe []float64
				var last Unit
				_amount := amount
				var kq []Order
				var totalValue float64
				for i := 0; i < len(book.Buy); i++ {
					if _amount > 0 {
						if _amount >= book.Buy[i].QtyTotal {
							_amount -= book.Buy[i].QtyTotal
							ix = i
							pe = append(pe, book.Buy[i].Price)
							totalValue += book.Buy[i].QtyTotal * book.Buy[i].Price
							response.LastPrice = book.Buy[i].Price
						} else {
							last.Price = book.Buy[i].Price
							last.QtyTotal = _amount
							book.Buy[i].QtyTotal -= _amount
							totalValue += _amount * book.Buy[i].Price
							response.LastPrice = book.Buy[i].Price
							_amount = 0

							break
						}
					}
				}

				if ix >= 0 {
					book.Buy = book.Buy[ix+1:]
					for _, v := range pe {
						kq = append(kq, mapOrder[v]...)
						delete(mapOrder, v)
					}
				}

				if last.Price > 0 {
					var value = last.QtyTotal
					var _i = -1
					for i := 0; i < len(mapOrder[last.Price]); i++ {
						if value > 0 {
							if value >= mapOrder[last.Price][i].Amount {
								value -= mapOrder[last.Price][i].Amount
								kq = append(kq, mapOrder[last.Price][i])
								_i = i
							} else {
								var temp = mapOrder[last.Price][i]
								temp.Amount = value
								mapOrder[last.Price][i].Amount -= value
								kq = append(kq, temp)
								value = 0
							}
						}
					}
					mapOrder[last.Price] = mapOrder[last.Price][_i+1:]
				}
				response.Queue = kq
				response.Price = totalValue / (amount - _amount)
				response.OrderBook = book
				response.Type = _type
				response.Amount = amount - _amount
				response.Side = side
				response.Left = Env("LEFT")
				response.Right = Env("RIGHT")
				return response
			}
		case "Cancel":
			{
				for i, order := range mapOrder[price] {
					if order.OpenID == openID {
						mapOrder[price] = append(mapOrder[price][:i], mapOrder[price][i+1:]...)
						break
					}
				}
				if price >= book.Sell[0].Price {
					for i := 0; i < len(book.Sell); i++ {
						if book.Sell[i].Price == price {
							book.Sell[i].QtyTotal -= amount
							if book.Sell[i].QtyTotal == 0 {
								book.Sell = append(book.Sell[:i], book.Sell[i+1:]...)
							}
						}
					}
				}
				response.Price = price
				response.OrderBook = book
				response.Type = _type
				response.Amount = amount
				response.Side = side
				response.Left = Env("LEFT")
				response.Right = Env("RIGHT")
				return response
			}
		}
	}
	return response
}

// Push to Queue
func Push(value string) {
	// defer writer.Close()
	writer.WriteMessages(context.Background(),
		kafka.Message{
			Value: []byte(value),
		},
	)
}

// ToStringResponse .......
func ToStringResponse(res Response) string {
	a := &res

	out, err := json.Marshal(a)
	if err != nil {
		panic(err)
	}

	return string(out)
}

// ToStringBook ...
func ToStringBook(res Book) string {
	a := &res

	out, err := json.Marshal(a)
	if err != nil {
		panic(err)
	}

	return string(out)
}

// ToStringOrder ...
func ToStringOrder(res []Order) string {
	a := &res

	out, err := json.Marshal(a)
	if err != nil {
		panic(err)
	}

	return string(out)
}

// ToStructRequest ...
func ToStructRequest(res string) Request {
	s := string(res)
	data := Request{}
	json.Unmarshal([]byte(s), &data)
	return data
}

// ToStructBook ..............
func ToStructBook(res string) Book {
	s := string(res)
	data := Book{}
	json.Unmarshal([]byte(s), &data)
	return data
}

// ToStructOrder ..............
func ToStructOrder(res string) []Order {
	s := string(res)
	data := []Order{}
	json.Unmarshal([]byte(s), &data)
	return data
}

// Env to read .env file
// return the value of the key
func Env(key string) string {
	viper.SetConfigFile(".env")
	err := viper.ReadInConfig()

	if err != nil {
		log.Fatalf("Error while reading config file %s", err)
	}
	value, ok := viper.Get(key).(string)
	if !ok {
		log.Fatalf("Invalid type assertion")
	}
	return value
}

// RediSet ...
func RediSet(key string, value int64) {
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	_, err = conn.Do("SET", key, value)
	if err != nil {
		log.Fatal(err)
	}
}

// RediSetString ...
func RediSetString(key string, value string) {
	conn, err := redis.Dial("tcp", Env("REDIS_HOST"))
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	_, err = conn.Do("SET", key, value)
	if err != nil {
		log.Fatal(err)
	}
}

// RediSetBoolExp ...
func RediSetBoolExp(key string, value bool) {
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	_, err = conn.Do("SETEX", key, 15, value)
	if err != nil {
		log.Fatal(err)
	}
}

// RediGet ...
func RediGet(key string) int64 {
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	val, err := redis.Int64(conn.Do("GET", key))
	if err != nil {
		// log.Fatal(err)
		return -1
	}
	return val
}

// RediGetString ...
func RediGetString(key string) string {
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	val, err := redis.String(conn.Do("GET", key))
	if err != nil {
		// log.Fatal(err)
		return ""
	}
	return val
}

// RediGetBool ...
func RediGetBool(key string) bool {
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	val, err := redis.Bool(conn.Do("GET", key))
	if err != nil {
		// log.Fatal(err)
		return false
	}
	return val
}

// RediIncr ....
func RediIncr(key string) {
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	_, err = conn.Do("INCR", key)
	if err != nil {
		log.Fatal(err)
	}
}

// Backup ...
func Backup(book Book) {
	RediSetString(Env("LEFT")+Env("RIGHT"), ToStringBook(book))
	for _, v := range book.Sell {
		RediSetString(Env("LEFT")+Env("RIGHT")+fmt.Sprintf("%g", v.Price), ToStringOrder(mapOrder[v.Price]))
	}
	for _, v := range book.Buy {
		RediSetString(Env("LEFT")+Env("RIGHT")+fmt.Sprintf("%g", v.Price), ToStringOrder(mapOrder[v.Price]))
	}
}

// Restore ...
func Restore() {
	rawBook := RediGetString(Env("LEFT") + Env("RIGHT"))
	if rawBook != "" {
		book = ToStructBook(rawBook)
		for _, v := range book.Sell {
			rawOrder := RediGetString(Env("LEFT") + Env("RIGHT") + fmt.Sprintf("%g", v.Price))
			if rawOrder != "" {
				mapOrder[v.Price] = ToStructOrder(rawOrder)
			}
		}

		for _, v := range book.Buy {
			rawOrder := RediGetString(Env("LEFT") + Env("RIGHT") + fmt.Sprintf("%g", v.Price))
			if rawOrder != "" {
				mapOrder[v.Price] = ToStructOrder(rawOrder)
			}
		}
	}
}

func random(min, max float64, dec int) float64 {
	maxI := int(math.Round(max * math.Pow10(dec)))
	minI := int(math.Round(min*math.Pow10(dec)) + 1)
	rand.Seed(time.Now().UnixNano())
	return float64(rand.Intn(maxI-minI+1)+minI) / math.Pow10(dec)
}

func randomInt(min, max int) float64 {
	rand.Seed(time.Now().UnixNano())
	return float64(rand.Intn(max-min+1) + min)
}

func main() {
	fmt.Println("Starting")
	Restore()
	for {
		// reader.SetOffset(5)
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			break
		}

		if m.Topic == "new-order-request-"+Env("LEFT")+Env("RIGHT") {
			if RediGet("Offset"+Env("LEFT")+Env("RIGHT")) == -1 {
				RediSet("Offset"+Env("LEFT")+Env("RIGHT"), m.Offset)
				req := ToStructRequest(string(m.Value))

				if !RediGetBool(req.OpenID) {
					RediSetBoolExp(req.OpenID, true)
					res := newOrder(req.UserID, req.OpenID, req.Type, req.Side, req.Price, req.Amount, req.BalanceLeft, req.BalanceRight)
					fmt.Println(res.OpenID)

					Push(ToStringResponse(res))
					Backup(book)
				}
			}

			if m.Offset-RediGet("Offset"+Env("LEFT")+Env("RIGHT")) == 1 {
				RediIncr("Offset" + Env("LEFT") + Env("RIGHT"))
				req := ToStructRequest(string(m.Value))

				if !RediGetBool(req.OpenID) {
					RediSetBoolExp(req.OpenID, true)
					res := newOrder(req.UserID, req.OpenID, req.Type, req.Side, req.Price, req.Amount, req.BalanceLeft, req.BalanceRight)
					fmt.Println(res.OpenID)

					Push(ToStringResponse(res))
					Backup(book)
				}
			}
		}
	}
}
