package main

import (
	"math/rand"
	"time"

	"gopkg.in/mgo.v2/bson"
)

// DB is the interface you need to implement to support
// testing other databases
type DB interface {
	Run(DatabaseURI)
}

// Trade holds all fields for each trade
// mgo:model:tb.trades
type Trade struct {
	ID                  bson.ObjectId `json:"id" bson:"_id,omitempty"`
	Company             bson.ObjectId `json:"company" bson:"company"`
	Entity              string        `json:"entity"`
	AccountName         string        `json:"account_name" bson:"account_name"`
	AssetClass          string        `json:"asset_class" bson:"asset_class"`
	Strategy            string        `json:"strategy"`
	Currency            string        `json:"currency"`
	TransactionType     string        `json:"transaction_type" bson:"transaction_type"`
	TradeDate           time.Time     `json:"trade_date" bson:"trade_date"`
	InversionConversion float64       `json:"inversion_conversion" bson:"inversion_conversion"`
	GrossAdjuster       float64       `json:"gross_adjuster" bson:"gross_adjuster"`
	AdjusterFactor      float64       `json:"adjuster_factor" bson:"adjuster_factor"`
	CommissionUSD       float64       `json:"commission_usd" bson:"commission_usd"`
	GrossUSD            float64       `json:"gross_usd" bson:"gross_usd"`
	Security            string        `json:"security"`
	SecurityType        string        `json:"security_type" bson:"security_type"`
	Ticker              string        `json:"ticker"`
	Cusip               string        `json:"cusip"`
	Sedol               string        `json:"sedol"`
	Price               float64       `json:"price"`
	PriceUSD            float64       `json:"price_usd" bson:"price_usd"`
	Quantity            float64       `json:"quantity"`
	Commission          float64       `json:"commission"`
	BrokerageFirm       string        `json:"brokerage_firm" bson:"brokerage_firm"`
	Parsed              time.Time     `json:"parsed" bson:"parsed"`
	SequencingSTD0      float64       `json:"std_0" bson:"std_0"`
	SequencingSTD3      float64       `json:"std_3" bson:"std_3"`
	SequencingSTD7      float64       `json:"std_7" bson:"std_7"`
	SequencingSTD14     float64       `json:"std_14" bson:"std_14"`
	SequencingMean0     float64       `json:"mean_0" bson:"mean_0"`
	SequencingMean3     float64       `json:"mean_3" bson:"mean_3"`
	SequencingMean7     float64       `json:"mean_7" bson:"mean_7"`
	SequencingMean14    float64       `json:"mean_14" bson:"mean_14"`
	ZScore0             float64       `json:"zscore_0" bson:"zscore_0"`
	ZScore3             float64       `json:"zscore_3" bson:"zscore_3"`
	ZScore7             float64       `json:"zscore_7" bson:"zscore_7"`
	ZScore14            float64       `json:"zscore_14" bson:"zscore_14"`
	ZScore0Zone         string        `json:"zscore_0_zone" bson:"zscore_0_zone"`
	ZScore3Zone         string        `json:"zscore_3_zone" bson:"zscore_3_zone"`
	ZScore7Zone         string        `json:"zscore_7_zone" bson:"zscore_7_zone"`
	ZScore14Zone        string        `json:"zscore_14_zone" bson:"zscore_14_zone"`
	Multi               []string      `json:"multi" bson:"multi,omitempty"`
	Multi0              bool          `json:"multi_spread_0" bson:"multi_spread_0"`
	MultiDay            bool          `json:"multi_spread_day" bson:"multi_spread_day"`
	MultiWeek           bool          `json:"multi_spread_week" bson:"multi_spread_week"`
	MultiMonth          bool          `json:"multi_spread_month" bson:"multi_spread_month"`
	MultiQuarter        bool          `json:"multi_spread_quarter" bson:"multi_spread_quarter"`
	Block               bool          `json:"block" bson:"block,omitempty"`
	Impact0             float64       `json:"impact_0" bson:"impact_0,omitempty"`
	Impact3             float64       `json:"impact_3" bson:"impact_3,omitempty"`
	Impact7             float64       `json:"impact_7" bson:"impact_7,omitempty"`
	Impact14            float64       `json:"impact_14" bson:"impact_14,omitempty"`
	ImpactRatio0        float64       `json:"impact_ratio_0" bson:"impact_ratio_0,omitempty"`
	ImpactRatio3        float64       `json:"impact_ratio_3" bson:"impact_ratio_3,omitempty"`
	ImpactRatio7        float64       `json:"impact_ratio_7" bson:"impact_ratio_7,omitempty"`
	ImpactRatio14       float64       `json:"impact_ratio_14" bson:"impact_ratio_14,omitempty"`
	Impact0Abs          float64       `json:"impact_0_abs" bson:"impact_0_abs,omitempty"`
	Impact3Abs          float64       `json:"impact_3_abs" bson:"impact_3_abs,omitempty"`
	Impact7Abs          float64       `json:"impact_7_abs" bson:"impact_7_abs,omitempty"`
	Impact14Abs         float64       `json:"impact_14_abs" bson:"impact_14_abs,omitempty"`
	Net                 float64       `json:"net" bson:"net,omitempty"`
	MarkUp              float64       `json:"markup" bson:"markup,omitempty"`
	CommissionPerShare  float64       `json:"cps" bson:"cps"`
	RateBPS             int           `json:"rate_bps" bson:"rate_bps"`
	Isin                string        `json:"isin" bson:"isin"`
	ParentOrderID       string        `json:"parent_order_id" bson:"parent_order_id"`
	ACMTradeID          string        `json:"acm_trade_id" bson:"acm_trade_id"`
	TradeStatus         string        `json:"trade_status" bson:"trade_status"`
	Custodian           string        `json:"custodian" bson:"custodian"`
	SecFees             float64       `json:"sec_fees" bson:"sec_fees"`
	Fees                float64       `json:"fees" bson:"fees"`
	AccruedInterest     float64       `json:"accrued_interest" bson:"accrued_interest"`
	OrderID             string        `json:"order_id" bson:"order_id"`
	VersusDate          time.Time     `json:"versus_date" bson:"versus_date"`
	Strategy2           string        `json:"strategy_2" bson:"strategy_2"`
	Strategy3           string        `json:"strategy_3" bson:"strategy_3"`
	TradeTime1          time.Time     `json:"trade_time_1" bson:"trade_time_1"`
	TradeTime2          time.Time     `json:"trade_time_2" bson:"trade_time_2"`
	ExpirationDate      time.Time     `json:"expiration_date" bson:"expiration_date"`
	OtcIndicator        string        `json:"otc_indicator" bson:"otc_indicator"`
	Sector              string        `json:"sector" bson:"sector"`
	Industry            string        `json:"industry" bson:"industry"`
	Venue               string        `json:"venue" bson:"venue"`
	OccCode             string        `json:"occ_code" bson:"occ_code"`
	Underlying          string        `json:"underlying" bson:"underlying"`
	StrikePrice         float64       `json:"strike_price" bson:"strike_price"`
	ParAmount           float64       `json:"par_amount" bson:"par_amount"`
	PutCall             string        `json:"put_call" bson:"put_call"`
	ExerciseStyle       string        `json:"exercise_style" bson:"exercise_style"`
	MaturityType        string        `json:"maturity_type" bson:"maturity_type"`
	Maturity            time.Time     `json:"maturity" bson:"maturity"`
	IssueDate           time.Time     `json:"issue_date" bson:"issue_date"`
	EffectiveDate       time.Time     `json:"effective_date" bson:"effective_date"`
	FirstPaymentDate    time.Time     `json:"first_payment_date" bson:"first_payment_date"`
	CouponRate          float64       `json:"coupon_rate" bson:"coupon_rate"`
	CouponFrequency     string        `json:"coupon_frequency" bson:"coupon_frequency"`
	AccrualMethod       string        `json:"accrual_method" bson:"accrual_method"`
	CurrencyAmount1     float64       `json:"currency_amount_1" bson:"currency_amount_1"`
	CurrencyAmount2     float64       `json:"currency_amount_2" bson:"currency_amount_2"`
	CurrencyCurrency1   string        `json:"currency_currency_1" bson:"currency_currency_1"`
	CurrencyCurrency2   string        `json:"currency_currency_2" bson:"currency_currency_2"`
	FixingDate          time.Time     `json:"fixing_date" bson:"fixing_date"`
	TerminationDate     time.Time     `json:"termination_date" bson:"termination_date"`
	SettlementType      string        `json:"settlement_type" bson:"settlement_type"`
	SwapID1             string        `json:"swap_id_1" bson:"swap_id_1"`
	SwapIndicator1      string        `json:"swap_indicator_1" bson:"swap_indicator_1"`
	SwapType1           string        `json:"swap_type_1" bson:"swap_type_1"`
	SwapFrequency1      string        `json:"swap_frequency_1" bson:"swap_frequency_1"`
	SwapRate1           float64       `json:"swap_rate_1" bson:"swap_rate_1"`
	SwapConvention1     string        `json:"swap_convention_1" bson:"swap_convention_1"`
	SwapID2             string        `json:"swap_id_2" bson:"swap_id_2"`
	SwapIndicator2      string        `json:"swap_indicator_2" bson:"swap_indicator_2"`
	SwapType2           string        `json:"swap_type_2" bson:"swap_type_2"`
	SwapFrequency2      string        `json:"swap_frequency_2" bson:"swap_frequency_2"`
	SwapRate2           float64       `json:"swap_rate_2" bson:"swap_rate_2"`
	SwapConvention2     string        `json:"swap_convention_2" bson:"swap_convention_2"`
	RedCode             string        `json:"red_code" bson:"red_code"`
	ReferenceEntity     string        `json:"reference_entity" bson:"reference_entity"`
	UnderlyingCusip     string        `json:"underlying_cusip" bson:"underlying_cusip"`
	UnderlyingIsin      string        `json:"underlying_isin" bson:"underlying_isin"`
	RecoveryRate        string        `json:"recovery_rate" bson:"recovery_rate"`
	NewsCount           int           `json:"news_count" bson:"news_count"`
}

// GenRecord Generates a dummy record to inser over and over into the database
// It more or less simulates real life length of string and numeric values
// used in production
func GenRecord() Trade {
	return Trade{
		Company:             bson.ObjectIdHex("5995263500714064b6b9863d"),
		Entity:              "Entity Name",
		AccountName:         "ACCOUNT NAME CAN BE VERY LONG",
		AssetClass:          randStringBytes(10),
		Strategy:            randStringBytes(10),
		Currency:            "USD",
		TransactionType:     "BUY",
		TradeDate:           time.Now(),
		InversionConversion: 0.01,
		GrossAdjuster:       0.01,
		AdjusterFactor:      1,
		CommissionUSD:       0,
		GrossUSD:            1234567.09,
		PriceUSD:            1234567.09,
		Security:            randStringBytes(200),
		SecurityType:        randStringBytes(50),
		Ticker:              randStringBytes(10),
		Cusip:               randStringBytes(40),
		Sedol:               randStringBytes(40),
		Price:               1920394,
		Quantity:            100000,
		Commission:          0,
		BrokerageFirm:       randStringBytes(80),
		Parsed:              time.Now(),
		Net:                 1234567898,
		MarkUp:              2345677,
		CommissionPerShare:  678931,
		RateBPS:             678931,
		Isin:                randStringBytes(30),
		ParentOrderID:       randStringBytes(50),
		TradeStatus:         randStringBytes(50),
		Custodian:           randStringBytes(50),
		SecFees:             100,
		Fees:                100,
		AccruedInterest:     100,
		OrderID:             randStringBytes(50),
		VersusDate:          time.Now(),
		Strategy2:           randStringBytes(50),
		Strategy3:           randStringBytes(50),
		TradeTime1:          time.Now(),
		TradeTime2:          time.Now(),
		ExpirationDate:      time.Now(),
		OtcIndicator:        randStringBytes(50),
		Sector:              randStringBytes(50),
		Industry:            randStringBytes(50),
		Venue:               randStringBytes(50),
		OccCode:             randStringBytes(50),
		Underlying:          randStringBytes(50),
		StrikePrice:         12345.8997543,
		ParAmount:           12345.8997543,
		PutCall:             "putCall",
		ExerciseStyle:       randStringBytes(50),
		MaturityType:        randStringBytes(50),
		Maturity:            time.Now(),
		IssueDate:           time.Now(),
		EffectiveDate:       time.Now(),
		FirstPaymentDate:    time.Now(),
		CouponRate:          12345.8997543,
		CouponFrequency:     "couponFrequency",
		AccrualMethod:       "accrualMethod",
		CurrencyAmount1:     12345.8997543,
		CurrencyAmount2:     12345.8997543,
		CurrencyCurrency1:   "USD",
		CurrencyCurrency2:   "USD",
		FixingDate:          time.Now(),
		TerminationDate:     time.Now(),
		SettlementType:      randStringBytes(50),
		SwapID1:             randStringBytes(50),
		SwapIndicator1:      randStringBytes(50),
		SwapType1:           randStringBytes(50),
		SwapFrequency1:      randStringBytes(50),
		SwapRate1:           10.3,
		SwapConvention1:     randStringBytes(50),
		SwapID2:             randStringBytes(50),
		SwapIndicator2:      randStringBytes(50),
		SwapType2:           randStringBytes(50),
		SwapFrequency2:      randStringBytes(50),
		SwapRate2:           10.4,
		SwapConvention2:     randStringBytes(50),
		RedCode:             randStringBytes(50),
		ReferenceEntity:     randStringBytes(50),
		UnderlyingCusip:     randStringBytes(50),
		UnderlyingIsin:      randStringBytes(50),
		RecoveryRate:        randStringBytes(50),
	}
}

func GenSQLRecord() Trade {
	return Trade{
		Company:             bson.ObjectIdHex("5995263500714064b6b9863d"),
		Entity:              "Entity Name",
		AccountName:         "ACCOUNT NAME CAN BE VERY LONG",
		AssetClass:          randStringBytes(10),
		Strategy:            randStringBytes(10),
		Currency:            "USD",
		TransactionType:     "BUY",
		TradeDate:           time.Now(),
		InversionConversion: 0.01,
		GrossAdjuster:       0.01,
		AdjusterFactor:      1,
		CommissionUSD:       0,
		GrossUSD:            1234567.09,
		PriceUSD:            1234567.09,
		Security:            randStringBytes(200),
		SecurityType:        randStringBytes(50),
		Ticker:              randStringBytes(10),
		Cusip:               randStringBytes(40),
		Sedol:               randStringBytes(40),
		Price:               1920394,
		Quantity:            100000,
		Commission:          0,
		BrokerageFirm:       randStringBytes(80),
		Parsed:              time.Now(),
		Net:                 1234567898,
		MarkUp:              2345677,
		CommissionPerShare:  678931,
		RateBPS:             678931,
		Isin:                randStringBytes(30),
		ParentOrderID:       randStringBytes(50),
		TradeStatus:         randStringBytes(50),
		Custodian:           randStringBytes(50),
		SecFees:             100,
		Fees:                100,
		AccruedInterest:     100,
		OrderID:             randStringBytes(50),
		VersusDate:          time.Now(),
		Strategy2:           randStringBytes(50),
		Strategy3:           randStringBytes(50),
		TradeTime1:          time.Now(),
		TradeTime2:          time.Now(),
		ExpirationDate:      time.Now(),
		OtcIndicator:        randStringBytes(50),
		Sector:              randStringBytes(50),
		Industry:            randStringBytes(50),
		Venue:               randStringBytes(50),
		OccCode:             randStringBytes(50),
		Underlying:          randStringBytes(50),
		StrikePrice:         12345.8997543,
		ParAmount:           12345.8997543,
		PutCall:             "putCall",
		ExerciseStyle:       randStringBytes(50),
		MaturityType:        randStringBytes(50),
		Maturity:            time.Now(),
		IssueDate:           time.Now(),
		EffectiveDate:       time.Now(),
		FirstPaymentDate:    time.Now(),
		CouponRate:          12345.8997543,
		CouponFrequency:     "couponFrequency",
		AccrualMethod:       "accrualMethod",
		CurrencyAmount1:     12345.8997543,
		CurrencyAmount2:     12345.8997543,
		CurrencyCurrency1:   "USD",
		CurrencyCurrency2:   "USD",
		FixingDate:          time.Now(),
		TerminationDate:     time.Now(),
		SettlementType:      randStringBytes(50),
		SwapID1:             randStringBytes(50),
		SwapIndicator1:      randStringBytes(50),
		SwapType1:           randStringBytes(50),
		SwapFrequency1:      randStringBytes(50),
		SwapRate1:           10.3,
		SwapConvention1:     randStringBytes(50),
		SwapID2:             randStringBytes(50),
		SwapIndicator2:      randStringBytes(50),
		SwapType2:           randStringBytes(50),
		SwapFrequency2:      randStringBytes(50),
		SwapRate2:           10.4,
		SwapConvention2:     randStringBytes(50),
		RedCode:             randStringBytes(50),
		ReferenceEntity:     randStringBytes(50),
		UnderlyingCusip:     randStringBytes(50),
		UnderlyingIsin:      randStringBytes(50),
		RecoveryRate:        randStringBytes(50),
	}
}

// credit to https://stackoverflow.com/a/31832326/309896
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

var createCRDBTable = `CREATE TABLE IF NOT EXISTS trades ( 
	id STRING PRIMARY KEY,
	company STRING,
	entity STRING,
	account_name STRING,
	asset_class STRING,
	strategy STRING,
	currency STRING,
	transaction_type STRING,
	trade_date TIMESTAMP,
	inversion_conversion FLOAT,
	gross_adjuster FLOAT,
	adjuster_factor FLOAT,
	commission_usd FLOAT,
	gross_usd FLOAT,
	security STRING,
	security_type STRING,
	ticker STRING,
	cusip STRING,
	sedol STRING,
	price FLOAT,
	price_usd FLOAT,
	quantity FLOAT,
	commission FLOAT,
	brokerage_firm STRING,
	parsed TIMESTAMP,
	std_0  FLOAT,
	std_3  FLOAT,
	std_7  FLOAT,
	std_14  FLOAT,
	mean_0  FLOAT,
	mean_3  FLOAT,
	mean_7  FLOAT,
	mean_14  FLOAT,
	zscore_0  FLOAT,
	zscore_3  FLOAT,
	zscore_7  FLOAT,
	zscore_14  FLOAT,
	zscore_0_zone STRING,
	zscore_3_zone STRING,
	zscore_7_zone STRING,
	zscore_14_zone STRING,
	multi STRING,
	multi_spread_0 BOOL,
	multi_spread_day BOOL,
	multi_spread_week BOOL,
	multi_spread_month BOOL,
	multi_spread_quarter BOOL,
	block  BOOL,
	impact_0 FLOAT,
	impact_3 FLOAT,
	impact_7 FLOAT,
	impact_14 FLOAT,
	impact_ratio_0 FLOAT,
	impact_ratio_3 FLOAT,
	impact_ratio_7 FLOAT,
	impact_ratio_14 FLOAT,
	impact_0_abs FLOAT,
	impact_3_abs FLOAT,
	impact_7_abs FLOAT,
	impact_14_abs FLOAT,
	net  FLOAT,
	markup  FLOAT,
	cps  FLOAT,
	rate_bps INT ,
	isin STRING,
	parent_order_id STRING,
	acm_trade_id STRING,
	trade_status STRING,
	custodian STRING,
	sec_fees FLOAT,
	fees FLOAT,
	accrued_interest FLOAT,
	order_id STRING,
	versus_date TIMESTAMP,
	strategy_2 STRING,
	strategy_3 STRING,
	trade_time_1 TIMESTAMP,
	trade_time_2 TIMESTAMP,
	expiration_date TIMESTAMP,
	otc_indicator STRING,
	sector STRING,
	industry STRING,
	venue STRING,
	occ_code STRING,
	underlying STRING,
	strike_price FLOAT,
	par_amount FLOAT,
	put_call  STRING,
	exercise_style STRING,
	maturity_type STRING,
	maturity TIMESTAMP,
	issue_date TIMESTAMP,
	effective_date TIMESTAMP,
	first_payment_date TIMESTAMP,
	coupon_rate FLOAT,
	coupon_frequency STRING,
	accrual_method STRING,
	currency_amount_1 FLOAT,
	currency_amount_2 FLOAT,
	currency_currency_1 STRING,
	currency_currency_2 STRING,
	fixing_date TIMESTAMP,
	termination_date TIMESTAMP,
	settlement_type STRING,
	swap_id_1 STRING,
	swap_indicator_1 STRING,
	swap_type_1 STRING,
	swap_frequency_1 STRING,
	swap_rate_1 FLOAT,
	swap_convention_1 STRING,
	swap_id_2 STRING,
	swap_indicator_2 STRING,
	swap_type_2 STRING,
	swap_frequency_2 STRING,
	swap_rate_2 FLOAT,
	swap_convention_2 STRING,
	red_code STRING,
	reference_entity STRING,
	underlying_cusip STRING,
	underlying_isin STRING,
	recovery_rate STRING,
	news_count INT

)`
