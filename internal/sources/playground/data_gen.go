package playground

import (
	"github.com/goccy/go-json"
	"github.com/jaswdr/faker"
	"time"
)

var fake faker.Faker

type userStruct struct {
	Id       string    `json:"id"`
	Name     string    `json:"name"`
	Age      uint16    `json:"age"`
	Date     time.Time `json:"date"`
	UserName string    `json:"username"`
	Email    string    `json:"email"`
}

type marketStruct struct {
	Company   string  `json:"company"`
	Currency  string  `json:"currency"`
	Valuation float64 `json:"valuation"`
	RecordId  string  `json:"record_id"`
}

func init() {
	fake = faker.New()
}

func generateMarketDataJsonBytes() []byte {
	market := marketStruct{
		Company:   fake.Company().Name(),
		Currency:  fake.Currency().Code(),
		Valuation: fake.Float64(10, 499999999, 999999999),
		RecordId:  fake.UUID().V4(),
	}

	b, _ := json.Marshal(&market)
	return b
}

func generateUserDataJsonBytes() []byte {
	user := userStruct{
		Name:     fake.Person().Name(),
		Age:      fake.UInt16Between(18, 65),
		Date:     time.Now(),
		Id:       fake.UUID().V4(),
		UserName: fake.Gamer().Tag(),
		Email:    fake.Internet().CompanyEmail(),
	}

	b, _ := json.Marshal(&user)
	return b
}
