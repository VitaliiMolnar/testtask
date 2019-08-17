package main

import (
	"context"
	"encoding/json"
	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"net/http"
	"sync"
	"time"
)

var client *mongo.Client

type UsersMap struct {
	mx  sync.Mutex
	Map map[uint64]*User
}

var usersMap = UsersMap{}

type Stat struct {
	DepositCount int64
	DepositSum   float64
	BetCount     int64
	BetSum       float64
	WinCount     int64
	WinSum       float64
}

type StatMap struct {
	mx  sync.Mutex
	Map map[int64]*Stat
}

var statMap = StatMap{}

type UserDeposit struct {
	UserId        int64   `json:"userId"`
	DepositId     int64   `json:"depositId"`
	Amount        float64 `json:"amount"`
	BalanceBefore float64 `json:"balanceBefore"`
	BalanceAfter  float64 `json:"balanceAfter"`
	Time          string  `json:"time"`
	newDeposit    bool    `json:"-"`
}

type UserTransaction struct {
	UserId         int64   `json:"userId"`
	TransactionId  int64   `json:"transactionId"`
	Type           string  `json:"type"`
	Amount         float64 `json:"amount"`
	BalanceBefore  float64 `json:"balanceBefore"`
	BalanceAfter   float64 `json:"balanceAfter"`
	Time           string  `json:"time"`
	newTransaction bool    `json:"-"`
}

type User struct {
	Id      int64   `json:"id"`
	Balance float64 `json:"balance"`

	deposits     []*UserDeposit     `json:"-"`
	transactions []*UserTransaction `json:"-"`

	newUser             bool `json:"-"`
	changedUser         bool `json:"-"`
	changedDeposits     bool `json:"-"`
	changedTransactions bool `json:"-"`
}

type RequestError struct {
	Error string `json:"error"`
}

type AddUserStruct struct {
	Id      int64   `json:"id"`
	Balance float64 `json:"balance"`
	Token   string  `json:"token"`
}

type GetUserStruct struct {
	Id    int64  `json:"id"`
	Token string `json:"token"`
}

type GetUserReturnStruct struct {
	Id           int64   `json:"id"`
	Balance      float64 `json:"balance"`
	DepositCount int64   `json:"depositCount"`
	DepositSum   float64 `json:"depositSum"`
	BetCount     int64   `json:"betCount"`
	BetSum       float64 `json:"betSum"`
	WinCount     int64   `json:"winCount"`
	WinSum       float64 `json:"winSum"`
}

type AddDepositStruct struct {
	UserId    int64   `json:"userId"`
	DepositId int64   `json:"depositId"`
	Amount    float64 `json:"amount"`
	Token     string  `json:"token"`
}

type TransactionStruct struct {
	UserId        int64   `json:"userId"`
	TransactionId int64   `json:"transactionId"`
	Type          string  `json:"type"`
	Amount        float64 `json:"amount"`
	Token         string  `json:"token"`
}

type BalanceReturnStruct struct {
	Error   string  `json:"error"`
	Balance float64 `json:"balance"`
}

func AddUser(response http.ResponseWriter, request *http.Request) {
	var requestStruct AddUserStruct
	var requestError RequestError
	var strError string
	response.Header().Set("content-type", "application/json")

	err := json.NewDecoder(request.Body).Decode(&requestStruct)

	if err != nil {
		requestError.Error = err.Error()
		json.NewEncoder(response).Encode(&requestError)
		return
	}

	if requestStruct.Token == "" {
		strError += "Missing token! "
	}
	if requestStruct.Token != "testtask" {
		strError += "Token not correct! "
	}

	usersMap.mx.Lock()
	defer usersMap.mx.Unlock()

	for _, checkUser := range usersMap.Map {
		if checkUser.Id == requestStruct.Id {
			strError += "Id is already set!"
			requestError.Error = strError
			json.NewEncoder(response).Encode(&requestError)
			return
		}
	}

	if strError == "" {
		var user = User{
			Id:      requestStruct.Id,
			Balance: requestStruct.Balance,
			newUser: true,
		}

		usersMap.Map[uint64(user.Id)] = &user
	}

	requestError.Error = strError
	json.NewEncoder(response).Encode(&requestError)
}

func GetUser(response http.ResponseWriter, request *http.Request) {
	var requestStruct GetUserStruct
	var requestError RequestError
	var strError string
	response.Header().Set("content-type", "application/json")

	err := json.NewDecoder(request.Body).Decode(&requestStruct)
	if err != nil {
		requestError.Error = err.Error()
		json.NewEncoder(response).Encode(&requestError)
		return
	}
	if requestStruct.Token == "" {
		strError += "Missing token! "
	} else if requestStruct.Token != "testtask" {
		strError += "Token not correct! "
	}

	if strError != "" {
		requestError.Error = strError
		json.NewEncoder(response).Encode(&requestError)
		return
	}

	usersMap.mx.Lock()
	defer usersMap.mx.Unlock()

	for _, user := range usersMap.Map {
		if user.Id == requestStruct.Id {

			statMap.mx.Lock()

			var returnStruct = GetUserReturnStruct{}
			returnStruct.Id = user.Id
			returnStruct.Balance = user.Balance

			if stat, ok := statMap.Map[user.Id]; ok {
				returnStruct.DepositCount = stat.DepositCount
				returnStruct.DepositSum = stat.DepositSum
				returnStruct.BetCount = stat.BetCount
				returnStruct.BetSum = stat.BetSum
				returnStruct.WinCount = stat.WinCount
				returnStruct.WinSum = stat.WinSum
			}
			statMap.mx.Unlock()

			json.NewEncoder(response).Encode(returnStruct)
			return
		}
	}

	strError = "Id not found"
	requestError.Error = strError
	json.NewEncoder(response).Encode(&requestError)
}

func AddDeposit(response http.ResponseWriter, request *http.Request) {
	var requestStruct AddDepositStruct
	var requestError RequestError
	var strError string
	response.Header().Set("content-type", "application/json")

	err := json.NewDecoder(request.Body).Decode(&requestStruct)
	if err != nil {
		requestError.Error = err.Error()
		json.NewEncoder(response).Encode(&requestError)
		return
	}
	if requestStruct.Token == "" {
		strError += "Missing token! "
	} else if requestStruct.Token != "testtask" {
		strError += "Token not correct! "
	}

	if strError != "" {
		requestError.Error = strError
		json.NewEncoder(response).Encode(&requestError)
		return
	}

	usersMap.mx.Lock()
	defer usersMap.mx.Unlock()

	for _, user := range usersMap.Map {
		if user.Id == requestStruct.UserId {

			for _, deposit := range user.deposits {
				if deposit.DepositId == requestStruct.DepositId {
					requestError.Error = "DepositId is already set!"
					json.NewEncoder(response).Encode(&requestError)
					return
				}
			}

			var newDeposit = UserDeposit{
				UserId:        requestStruct.UserId,
				DepositId:     requestStruct.DepositId,
				Amount:        requestStruct.Amount,
				BalanceBefore: user.Balance,
				BalanceAfter:  user.Balance + requestStruct.Amount,
				Time:          time.Now().Format(time.RFC850),
				newDeposit:    true,
			}

			user.deposits = append(user.deposits, &newDeposit)
			user.changedUser = true
			user.changedDeposits = true
			user.Balance += requestStruct.Amount

			var returnStruct = BalanceReturnStruct{
				Error:   "",
				Balance: user.Balance,
			}

			statMap.mx.Lock()

			if stat, ok := statMap.Map[user.Id]; ok {
				stat.DepositCount++
				stat.DepositSum += requestStruct.Amount
			} else {
				var stat = Stat{
					DepositCount: 1,
					DepositSum:   requestStruct.Amount,
				}

				statMap.Map[user.Id] = &stat
			}
			statMap.mx.Unlock()

			json.NewEncoder(response).Encode(returnStruct)
			return
		}
	}

	strError = "userId not found"
	requestError.Error = strError
	json.NewEncoder(response).Encode(&requestError)
}

func Transaction(response http.ResponseWriter, request *http.Request) {
	var requestStruct TransactionStruct
	var requestError RequestError
	var strError string
	response.Header().Set("content-type", "application/json")

	err := json.NewDecoder(request.Body).Decode(&requestStruct)
	if err != nil {
		requestError.Error = err.Error()
		json.NewEncoder(response).Encode(&requestError)
		return
	}
	if requestStruct.Token == "" {
		strError += "Missing token! "
	} else if requestStruct.Token != "testtask" {
		strError += "Token not correct! "
	}

	if requestStruct.Type != "Win" && requestStruct.Type != "Bet" {
		strError += "\"type\" must be \"Win\" or \"Bet\"!"
	}

	if strError != "" {
		requestError.Error = strError
		json.NewEncoder(response).Encode(&requestError)
		return
	}

	usersMap.mx.Lock()
	defer usersMap.mx.Unlock()

	for _, user := range usersMap.Map {
		if user.Id == requestStruct.UserId {

			if requestStruct.Type == "Bet" && (user.Balance-requestStruct.Amount < 0) {
				requestError.Error = "User doesn`t have enough money!"
				json.NewEncoder(response).Encode(&requestError)
				return
			}

			for _, transaction := range user.transactions {
				if transaction.TransactionId == requestStruct.TransactionId {
					requestError.Error = "TransactionId is already set!"
					json.NewEncoder(response).Encode(&requestError)
					return
				}
			}

			var oldBalance = user.Balance

			if requestStruct.Type == "Bet" {
				user.Balance -= requestStruct.Amount
			} else {
				user.Balance += requestStruct.Amount
			}

			var newTransaction = UserTransaction{
				UserId:         requestStruct.UserId,
				TransactionId:  requestStruct.TransactionId,
				Type:           requestStruct.Type,
				Amount:         requestStruct.Amount,
				BalanceBefore:  oldBalance,
				BalanceAfter:   user.Balance,
				Time:           time.Now().Format(time.RFC850),
				newTransaction: true,
			}

			user.transactions = append(user.transactions, &newTransaction)
			user.changedUser = true
			user.changedTransactions = true

			var returnStruct = BalanceReturnStruct{
				Error:   "",
				Balance: user.Balance,
			}

			statMap.mx.Lock()

			if stat, ok := statMap.Map[user.Id]; ok {
				if requestStruct.Type == "Bet" {
					stat.BetCount++
					stat.BetSum += requestStruct.Amount
				} else {
					stat.WinCount++
					stat.WinSum += requestStruct.Amount
				}
			} else {
				var stat = Stat{}

				if requestStruct.Type == "Bet" {
					stat.BetCount = 1
					stat.BetSum = requestStruct.Amount
				} else {
					stat.WinCount = 1
					stat.WinSum = requestStruct.Amount
				}

				statMap.Map[user.Id] = &stat
			}
			statMap.mx.Unlock()

			json.NewEncoder(response).Encode(returnStruct)
			return
		}
	}

	strError = "userId not found"
	requestError.Error = strError
	json.NewEncoder(response).Encode(&requestError)
}

func main() {
	usersMap.Map = map[uint64]*User{}
	statMap.Map = map[int64]*Stat{}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, _ = mongo.Connect(ctx, clientOptions)

	go func() {
		for {
			usersMap.mx.Lock()

			for _, user := range usersMap.Map {
				if user.newUser == true {
					collection := client.Database("test").Collection("users")
					ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
					_, err := collection.InsertOne(ctx, user)

					if err != nil {
						panic(err)
					}

					user.newUser = false
				}

				if user.changedUser == true {

					filter := bson.D{{"id", user.Id}}
					update := bson.D{
						{"$set", bson.D{
							{"balance", user.Balance},
						}},
					}

					collection := client.Database("test").Collection("users")
					ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
					_, err := collection.UpdateOne(ctx, filter, update)

					if err != nil {
						panic(err)
					}

					user.changedUser = false
				}

				if user.changedDeposits == true {

					for _, deposit := range user.deposits {
						if deposit.newDeposit == true {

							collection := client.Database("test").Collection("deposits")
							ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
							_, err := collection.InsertOne(ctx, deposit)

							if err != nil {
								panic(err)
							}

							deposit.newDeposit = false
						}
					}

					user.changedDeposits = false
				}

				if user.changedTransactions == true {
					for _, transaction := range user.transactions {

						if transaction.newTransaction == true {

							collection := client.Database("test").Collection("transactions")
							ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
							_, err := collection.InsertOne(ctx, transaction)

							if err != nil {
								panic(err)
							}

							transaction.newTransaction = false
						}
					}

					user.changedTransactions = false
				}
			}

			usersMap.mx.Unlock()
			time.Sleep(time.Second * 10)
		}
	}()

	router := mux.NewRouter()
	router.HandleFunc("/user/create", AddUser).Methods("POST")
	router.HandleFunc("/user/get", GetUser).Methods("POST")
	router.HandleFunc("/user/deposit", AddDeposit).Methods("POST")
	router.HandleFunc("/transaction", Transaction).Methods("POST")
	http.ListenAndServe(":9000", router)
}
