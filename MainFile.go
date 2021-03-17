package main

import (
"bytes"
"database/sql"
"encoding/json"
fmt "fmt"
"io/ioutil"
"net/http"
"strconv"
"strings"
"time"
)

import _ "github.com/denisenkom/go-mssqldb"


type HomePageSize struct {
	Body string
	id int64
}

var (  // конфиденц.
	server string
	database string
	user   string
	password string
	port  int
)

func main() {

	results := make(chan HomePageSize)

	// DB
	connString := fmt.Sprintf("server=%s;database=%s;user id=m**;password=%s",server,database,password)
	DB, err := sql.Open("mssql",connString)
	if err != nil {
		//02.11.2020 fmt.Println("Error during connection with DB Server !!!")
		//02.11.2020 fmt.Println(err)
	}

	if err == nil {
		fmt.Println("Агент HTTP-запросов запущен !!!")
		fmt.Println("Не выключайте!")
	}

	result := HomePageSize {
		Body : "START",
		id: 1,
	}

	for result.Body != "STOP" {  // Остановка агента командой из-вне.

		if time.Now().Nanosecond() > 989800000 || result.Body == "ERR_EXEC_PROC"  {  // делаем новый connection к БД, либо если произошла ошибка, либо connection - старый
			DB.Close()

			connString = fmt.Sprintf("server=%s;database=%s;user id=mo2;password=%s", server, database, password)
			DB, err = sql.Open("mssql", connString)
			if err != nil {
				//02.11.2020 fmt.Println("Error during connection with DB Server !!!")
				//02.11.2020 fmt.Println(err)
			}

			if err == nil {
				//02.11.2020 fmt.Println("Renew DB-connection !!!")
				//02.11.2020 fmt.Println(time.Now())
				//fmt.Println(err)
			}
		}

		//time.Sleep(500*time.Millisecond)
		go WebServiceCall(DB, results )  	// запускаем горутин по работе с http-запросами.
											// в дальнейшем будем запускать несколько таких горутинов, в зависимости от кол-ва заданий на http-запрос и кол-ва работающих агентов в сети

		result := <-results

		if result.Body != "NEXT" && result.Body != "NOROWS" {
			_, err := DB.Query("exec dbo.HTTPReqRespHandling 'PUT_RESPONSE_TEST','" + result.Body/*str45*/ + "'," + strconv.FormatInt(result.id, 10)) // Кладем resp в КШД

			if len(result.Body) == 0 {
				//02.11.2020 fmt.Println("ALARM  !!!!!!!!!!!!!!!!!!!")
			}

			if err != nil {
				//02.11.2020 fmt.Println("Error INSERT into DB !!!")
				//02.11.2020 fmt.Println(err)
			}
		}
	}

	fmt.Println("STOP !!!!!!!!!!!!!!!!!")

	DB.Close()

}


func WebServiceCall(DB *sql.DB, results chan HomePageSize) {

	rows, err := DB.Query("exec dbo.HTTPReqRespHandling 'GET_REQUEST_TEST' ")  //считали из КШД очередную заявку на http-запрос

	if err != nil {
		results <- HomePageSize{
			Body: "ERR_EXEC_PROC",
			id:   0,
		}
	}

	var url1 string = ""
	var method string  = ""
	var headers string = ""
	var parameters string = ""
	var contenttype string = ""
	var idRequest int64 = 0

	if err == nil {

		for rows.Next() {
			err = rows.Scan(&url1, &method, &headers, &parameters, &contenttype, &idRequest)  //получили данные для http-запроса
		}
		err = rows.Err()

		defer rows.Close()

		if method == "POST" {
			var jsonStr = []byte(headers)

			req, _ := http.NewRequest("POST", url1 /*"http://10.2.14.17:3000"*/, bytes.NewBuffer(jsonStr) /*&requestBody*/)
			req.Header.Set("content-type", contenttype)
			AddHeadersAll (parameters, req)


			if req != nil {
							client := &http.Client{}
							resp, errresp := client.Do(req)

			if errresp != nil {

			results <- HomePageSize{   // посылаем ответ Error/Next в главный горутин
			Body: "NEXT",
			id:   0,
			}

			} else {  // Если ошибок нет в response

					if errresp == nil {
						//02.11.2020 fmt.Println("SUCCESS_POST!!!")
						//02.11.2020 fmt.Println(idRequest)
						//02.11.2020 fmt.Println(time.Now())
					}

					//defer resp.Body.Close()

					bs33, _ := ioutil.ReadAll(resp.Body)
					str := string(bs33)

					var result map[string]interface{}
					json.NewDecoder(resp.Body).Decode(&result)

					results <- HomePageSize{ // посылаем успешный response в главный горутин
					Body: str,
					id:   idRequest,
					}
			}

} else {
			results <- HomePageSize{
			Body: "NEXT",
			id:   0,
			}
		}
}

if method == "GET" {

	if 1 == 1/*strings.Contains(url1,"flomni")*/ {
	var jsonStr = []byte("")

	req, _ := http.NewRequest("GET", url1 /*"http://10.2.14.17:3000"*/, bytes.NewBuffer(jsonStr) /*&requestBody*/)
	req.Header.Set("content-type", contenttype)
	AddHeadersAll (parameters, req)


	if req != nil { //pointer is not <nil>
		client := &http.Client{}
		resp, erresp := client.Do(req)

		if erresp != nil {

			fmt.Println("NOT SUCCESS_GET!!!")

			results <- HomePageSize{
				Body: "NEXT",
				id:   0,
			}
		} else {

			bs33, _ := ioutil.ReadAll(resp.Body)
			str := string(bs33)
			//fmt.Println("SUCCESS_GET!!!")

			results <- HomePageSize{
				Body: str,
				id:   idRequest,
			}
		}

	}
	//---------------------------------------

	} else { // старый кусок

	resp, err := http.Get(url1)

	//fmt.Println(resp)
	if err != nil {
	} else {
	}

	bs33, _ := ioutil.ReadAll(resp.Body)
	str := string(bs33)

	results <- HomePageSize{
		Body: str,
		id:   idRequest,
	}
}
}

if method == "STOP" {
			results <- HomePageSize{
			Body: "STOP",
			id:   0,
			}
}

if method != "STOP" && method != "POST" && method != "GET" {

			results <- HomePageSize{
			Body: "NEXT",
			id:   0,
			}
}
}


}

func AddHeadersAll (headers string, req *http.Request) { // Добавление всех headers одним проходом

	//---Parsing-(V)-------------------------------------------------------------------
	p := 0
	l := len(headers)
	s := ""
	s0 := ""
	for i := 0; i <= l-1; i++ {
		if p == 2 {
			p = 1
		}
		c := headers[i]

		if p == 0 {
			if c != ':' {
				s += string(c)
			}
			if c == ':' {
				s0 = s
				p = 2
				s = ""
			}
		}

		if p == 1 {
			if c != ';' {
				s += string(c)
			}
			if c == ';' || i == l-1 {
				req.Header.Set(strings.Trim(s0, " "), strings.Trim(s, " "))
				p = 0
				s = ""
			}
		}
	}

	//-------

}
