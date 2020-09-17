package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

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
	OpenID string  `json:"openid"` // Mã xác định
	Amount float64 `json:"amount"` // So Luong
	// price  float64 // Gia
	// userID string  // Dinh danh nguoi dung
	// side   bool    // Phia: Buy, Sell
	// _type  string  // Loai Lenh: Limit, Market, Cancel
	// left      string  // Coin Trai: Vd: BTC
	// right     string  // Coin Phai: Vd: USDT
	// fill      float64 // Mức độ Khớp
	// Status    string  // Trạng thái, tình trạng lệnh
	// timestamp int64   // Dấu thời gian
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

// Response of Neworder
type Response struct {
	OpenID       string  `json:"openID"`
	ChangePrice  float64 `json:"changePrice"`
	ChangeAmount float64 `json:"changeAmount"`
	Queue        []Order `json:"queue"`
	Status       bool    `json:"status"`
	AvgPrice     float64 `json:"avgPrice"`
	OrderBook    Book    `json:"orderBook"`
}

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

// Request ...........
type Request struct {
	UserID string  `json:"userID"`
	OpenID string  `json:"openID"`
	Type   string  `json:"_type"`
	Side   bool    `json:"side"`
	Price  float64 `json:"price"`
	Amount float64 `json:"amount"`
}

func newOrder(userID, openID, _type string, side bool, price, amount float64) Response {
	var response = Response{}
	response.OpenID = openID

	if side {
		switch _type {
		case "Limit":
			{
				if amount == 0 || price == 0 {
					response.Status = true
					response.OrderBook = book
					response.OpenID = " "
					return response
				}
				_price := price
				if len(book.Sell) > 0 && price >= book.Sell[0].Price {
					_price = MIN
					response.ChangePrice = _price
					if len(book.Buy) > 0 {
						_price = book.Buy[0].Price
						response.ChangePrice = _price
					}
				}
				book.Buy = InsertBook(book.Buy, Unit{_price, amount}, side)
				mapOrder[_price] = append(mapOrder[_price], Order{openID, amount})
				response.Status = true
				response.OrderBook = book
				return response
			}
		case "Market":
			{
				if amount == 0 {
					response.Status = true
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
						} else {
							last.Price = book.Sell[i].Price
							last.QtyTotal = _amount
							book.Sell[i].QtyTotal -= _amount
							totalValue += _amount * book.Sell[i].Price
							_amount = 0
							break
						}
					}
				}
				if _amount > 0 {
					response.ChangeAmount = _amount
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
				response.Status = true

				if amount > 0 {
					response.AvgPrice = totalValue / (amount - _amount)
				} else {
					response.AvgPrice = 0
				}
				response.OrderBook = book
				return response
			}
		case "Cancel":
			{
				for i, order := range mapOrder[price] {
					if order.OpenID == openID {
						response.Status = true
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
				response.OrderBook = book
				return response
			}
		}
	} else {
		switch _type {
		case "Limit":
			{
				if amount == 0 || price == 0 {
					response.Status = true
					response.OrderBook = book
					response.OpenID = " "
					return response
				}
				_price := price
				if len(book.Buy) > 0 && price <= book.Buy[0].Price {
					_price = MAX
					response.ChangePrice = _price
					if len(book.Sell) > 0 {
						_price = book.Sell[0].Price
						response.ChangePrice = _price
					}
				}
				book.Sell = InsertBook(book.Sell, Unit{_price, amount}, side)
				mapOrder[_price] = append(mapOrder[_price], Order{openID, amount})
				response.Status = true
				response.OrderBook = book
				return response
			}
		case "Market":
			{
				if amount == 0 {
					response.Status = true
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
				for i := 0; i < len(book.Buy); i++ {
					if _amount > 0 {
						if _amount >= book.Buy[i].QtyTotal {
							_amount -= book.Buy[i].QtyTotal
							ix = i
							pe = append(pe, book.Buy[i].Price)
							totalValue += book.Buy[i].QtyTotal * book.Buy[i].Price
						} else {
							last.Price = book.Buy[i].Price
							last.QtyTotal = _amount
							book.Buy[i].QtyTotal -= _amount
							totalValue += _amount * book.Buy[i].Price
							_amount = 0

							break
						}
					}
				}
				if _amount > 0 {
					response.ChangeAmount = _amount
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
				response.Status = true
				if amount > 0 {
					response.AvgPrice = totalValue / (amount - _amount)
				} else {
					response.AvgPrice = 0
				}
				response.OrderBook = book
				return response
			}
		case "Cancel":
			{
				for i, order := range mapOrder[price] {
					if order.OpenID == openID {
						response.Status = true
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
				response.OrderBook = book
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
			if RediGet("Offset") == -1 {
				RediSet("Offset", m.Offset)
				req := ToStructRequest(string(m.Value))

				if !RediGetBool(req.OpenID) {
					RediSetBoolExp(req.OpenID, true)
					res := newOrder(req.UserID, req.OpenID, req.Type, req.Side, req.Price, req.Amount)
					fmt.Println(ToStringResponse(res))

					Push(ToStringResponse(res))
					Backup(book)
				}
			}

			if m.Offset-RediGet("Offset") == 1 {
				RediIncr("Offset")
				req := ToStructRequest(string(m.Value))

				if !RediGetBool(req.OpenID) {
					RediSetBoolExp(req.OpenID, true)
					res := newOrder(req.UserID, req.OpenID, req.Type, req.Side, req.Price, req.Amount)
					fmt.Println(ToStringResponse(res))

					Push(ToStringResponse(res))
					Backup(book)
				}
			}
		}
	}
}
