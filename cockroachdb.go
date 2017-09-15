package main

import (
	"database/sql"
	"log"
	"net/url"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"gopkg.in/mgo.v2/bson"
)

func cockroachdbSetup(uri string) {
	parsedURL, err := url.Parse(uri)
	if err != nil {
		log.Fatal(err)
	}
	parsedURL.Path = "testing"
	db, err := sql.Open("postgres", parsedURL.String())
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec("DROP DATABASE If EXISTS testing"); err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS testing"); err != nil {
		log.Fatal(err)
	}

	if _, err = db.Exec(createCRDBTable); err != nil {
		log.Fatal(err)
	}

	if _, err = db.Exec("TRUNCATE TABLE trades"); err != nil {
		log.Fatal(err)
	}
}

func cockroachdbRun(uri string, wg *sync.WaitGroup) {
	parsedURL, err := url.Parse(uri)
	if err != nil {
		log.Fatal(err)
	}
	parsedURL.Path = "testing"
	db, err := sql.Open("postgres", parsedURL.String())
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	//for x := 0; x < 3; x++ {
	if err := cockroachdbLoad(db); err != nil {
		log.Fatalln("failed to insert docs into crdb ", err)
	}
	//}
	wg.Done()
}

func cockroachdbLoad(db *sql.DB) error {
	q := "INSERT INTO trades (" + insertHeader + ") VALUES ("

	q += `
	$1,
	$2,
	$3,
	$4,
	$5,
	$6,
	$7,
	$8,
	$9,
	$10,
	$11,
	$12,
	$13,
	$14,
	$15,
	$16,
	$17,
	$18,
	$19,
	$20,
	$21,
	$22,
	$23,
	$24,
	$25,
	$26,
	$27,
	$28,
	$29,
	$30,
	$31,
	$32,
	$33,
	$34,
	$35,
	$36,
	$37,
	$38,
	$39,
	$40,
	$41,
	$42,
	$43,
	$44,
	$45,
	$46,
	$47,
	$48,
	$49,
	$50,
	$51,
	$52,
	$53,
	$54,
	$55,
	$56,
	$57,
	$58,
	$59,
	$60,
	$61,
	$62,
	$63,
	$64,
	$65,
	$66,
	$67,
	$68,
	$69,
	$70,
	$71,
	$72,
	$73,
	$74,
	$75,
	$76,
	$77,
	$78,
	$79,
	$80,
	$81,
	$82,
	$83,
	$84,
	$85)
`

	stmt, err := db.Prepare(q)
	if err != nil {
		log.Fatalln("failed to prepare insert statement: ", err)
	}

	for y := 0; y <= 999; y++ {
		_, err = stmt.Exec(
			bson.NewObjectId(),
			bson.ObjectIdHex("5995263500714064b6b9863d"),
			"Entity Name",
			"ACCOUNT NAME CAN BE VERY LONG",
			randStringBytes(10),
			randStringBytes(10),
			"USD",
			"BUY",
			time.Now(),
			0.01,
			0.01,
			1,
			0,
			1234567.09,
			1234567.09,
			randStringBytes(200),
			randStringBytes(50),
			randStringBytes(10),
			randStringBytes(40),
			randStringBytes(40),
			1920394,
			100000,
			0,
			randStringBytes(80),
			time.Now(),
			1234567898,
			2345677,
			678931,
			678931,
			randStringBytes(30),
			randStringBytes(50),
			randStringBytes(50),
			randStringBytes(50),
			100,
			100,
			100,
			randStringBytes(50),
			time.Now(),
			randStringBytes(50),
			randStringBytes(50),
			time.Now(),
			time.Now(),
			time.Now(),
			randStringBytes(50),
			randStringBytes(50),
			randStringBytes(50),
			randStringBytes(50),
			randStringBytes(50),
			randStringBytes(50),
			12345.8997543,
			12345.8997543,
			"putCall",
			randStringBytes(50),
			randStringBytes(50),
			time.Now(),
			time.Now(),
			time.Now(),
			time.Now(),
			12345.8997543,
			"couponFrequency",
			"accrualMethod",
			12345.8997543,
			12345.8997543,
			"USD",
			"USD",
			time.Now(),
			time.Now(),
			randStringBytes(50),
			randStringBytes(50),
			randStringBytes(50),
			randStringBytes(50),
			randStringBytes(50),
			10.3,
			randStringBytes(50),
			randStringBytes(50),
			randStringBytes(50),
			randStringBytes(50),
			randStringBytes(50),
			10.4,
			randStringBytes(50),
			randStringBytes(50),
			randStringBytes(50),
			randStringBytes(50),
			randStringBytes(50),
			randStringBytes(50),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

var insertHeader = `
id,
company,
entity,
account_name,
asset_class,
strategy,
currency,
transaction_type,
trade_date,
inversion_conversion,
gross_adjuster,
adjuster_factor,
commission_usd,
gross_usd,
price_usd,
security,
security_type,
ticker,
cusip,
sedol,
price,
quantity,
commission,
brokerage_firm,
parsed,
net,
markup,
cps,
rate_bps,
isin,
parent_order_id,
trade_status,
custodian,
sec_fees,
fees,
accrued_interest,
order_id,
versus_date,
strategy_2,
strategy_3,
trade_time_1,
trade_time_2,
expiration_date,
otc_indicator,
sector,
industry,
venue,
occ_code,
underlying,
strike_price,
par_amount,
put_call,
exercise_style,
maturity_type,
maturity,
issue_date,
effective_date,
first_payment_date,
coupon_rate,
coupon_frequency,
accrual_method,
currency_amount_1,
currency_amount_2,
currency_currency_1,
currency_currency_2,
fixing_date,
termination_date,
settlement_type,
swap_id_1,
swap_indicator_1,
swap_type_1,
swap_frequency_1,
swap_rate_1,
swap_convention_1,
swap_id_2,
swap_indicator_2,
swap_type_2,
swap_frequency_2,
swap_rate_2,
swap_convention_2,
red_code,
reference_entity,
underlying_cusip,
underlying_isin,
recovery_rate,
news_count
`

func cockroachdbDelete(uri string) {
	parsedURL, err := url.Parse(uri)
	if err != nil {
		log.Fatal(err)
	}
	parsedURL.Path = "testing"
	db, err := sql.Open("postgres", parsedURL.String())
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err = db.Exec("TRUNCATE TABLE trades"); err != nil {
		log.Fatal(err)
	}

}
