package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"

	"github.com/gin-gonic/gin"
	pgxpool "github.com/jackc/pgx/v4/pgxpool"
)

var (
	dbConnString   = "user=postgres dbname=test sslmode=disable" // Replace with your PostgreSQL connection details
	schemaName     = "cashback_may_2023"
	dbMaxIdleConns = 4
	dbMaxConns     = 50
	totalWorker    = 100
	csvFile        = "sample.csv"
	dataHeaders    = []string{
		"no_waybill",
		"tgl_pengiriman",
		"drop_point_outgoing",
		"sprinter_pickup",
		"tempat_tujuan",
		"keterangan",
		"berat_yang_ditagih",
		"cod",
		"biaya_asuransi",
		"biaya_kirim",
		"biaya_lainnya",
		"total_biaya",
		"klien_pengiriman",
		"metode_pembayaran",
		"nama_pengirim",
		"sumber_waybill",
		"paket_retur",
		"waktu_ttd",
		"layanan",
		"diskon",
		"total_biaya_setelah_diskon",
		"agen_tujuan",
		"nik",
		"kode_promo",
		"kat",
	}

	router       = gin.Default()
	errorLogFile = "error.log"
)

type DateParams struct {
	Month string `form:"month"`
	Year  string `form:"year"`
}

// Define a struct to hold the data elements
type DataElement struct {
	NoWaybill               string
	TglPengiriman           time.Time
	DropPointOutgoing       string
	SprinterPickup          string
	TempatTujuan            string
	Keterangan              string
	BeratYangDitagih        float64
	Cod                     int64
	BiayaAsuransi           float64
	BiayaKirim              int64
	BiayaLainnya            int64
	TotalBiaya              float64
	KlienPengiriman         string
	MetodePembayaran        string
	NamaPengirim            string
	SumberWayBill           string
	PaketRetur              string
	WaktuTTD                time.Time
	Layanan                 string
	Diskon                  int64
	TotalBiayaSetelahDiscon int64
	AgenTujuan              string
	Nik                     string
	KodePromo               string
	Kategori                string
}

func main() {
	// Create or open the error log file
	errorLog, err := os.OpenFile(errorLogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer errorLog.Close()

	// Set the logger's output to the error log file
	log.SetOutput(errorLog)

	router.POST("/upload", handleUpload)

	router.Run(":8080")
}

func handleUpload(c *gin.Context) {
	start := time.Now()

	dbPool, err := openDbConnectionPool()
	if err != nil {
		log.Println(err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Failed to connect to the database"})
		return
	}
	defer dbPool.Close()

	file, _, err := c.Request.FormFile("file")
	if err != nil {
		log.Println(err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"message": "Failed to read the uploaded file"})
		return
	}
	defer file.Close()

	var dateParams DateParams
	if err := c.ShouldBindQuery(&dateParams); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid date parameters"})
		return
	}

	csvReader := csv.NewReader(file)

	jobs := make(chan []interface{}, 0)
	wg := new(sync.WaitGroup)

	go dispatchWorkers(dbPool, jobs, wg, &dateParams)
	readCsvFilePerLineThenSendToWorker(csvReader, jobs, wg)

	wg.Wait()

	duration := time.Since(start)

	c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("Data inserted successfully in %d seconds for month %s, year %d", int(math.Ceil(duration.Seconds())), dateParams.Month, dateParams.Year)})
}

// trimBOM trims the UTF-8 byte-order mark (BOM) from the beginning of the reader.
func trimBOM(r io.Reader) io.Reader {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(r)
	if err != nil {
		return r
	}
	b := buf.Bytes()
	if len(b) >= 3 && bytes.Equal(b[:3], []byte{0xEF, 0xBB, 0xBF, 0xef, 0xbc}) {
		return bytes.NewReader(b[3:])
	}
	return bytes.NewReader(b)
}

func openDbConnectionPool() (*pgxpool.Pool, error) {
	log.Println("=> open db connection pool")

	config, err := pgxpool.ParseConfig(dbConnString)
	if err != nil {
		return nil, err
	}

	config.MaxConns = int32(dbMaxConns)
	config.MinConns = int32(dbMaxIdleConns)

	pool, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func openCsvFile() (*csv.Reader, *os.File, error) {
	log.Println("=> open csv file")

	f, err := os.Open(csvFile)
	if err != nil {
		if os.IsNotExist(err) {
			log.Fatal("file csv not found. please import first")
		}

		return nil, nil, err
	}

	defer f.Close()

	// Specify the UTF-8 encoding explicitly
	utf8Decoder := unicode.UTF8.NewDecoder()
	trimmedReader := trimBOM(f)
	reader := csv.NewReader(transform.NewReader(trimmedReader, utf8Decoder))
	// reader := csv.NewReader(f)
	return reader, f, nil
}

func dispatchWorkers(pool *pgxpool.Pool, jobs <-chan []interface{}, wg *sync.WaitGroup, date *DateParams) {
	for workerIndex := 0; workerIndex <= totalWorker; workerIndex++ {
		go func(workerIndex int, pool *pgxpool.Pool, jobs <-chan []interface{}, wg *sync.WaitGroup) {
			counter := 0

			for job := range jobs {
				conn, err := pool.Acquire(context.Background())
				if err != nil {
					log.Println("Worker", workerIndex, "failed to acquire connection:", err)
					continue
				}

				doTheJob(workerIndex, counter, conn, job, date)

				conn.Release()
				wg.Done()
				counter++
			}
		}(workerIndex, pool, jobs, wg)
	}
}

// handle error using panic
func handleError(err error) {
	if err != nil {
		log.Println(err)
	}
}

func readCsvFilePerLineThenSendToWorker(csvReader *csv.Reader, jobs chan<- []interface{}, wg *sync.WaitGroup) {
	isHeader := true

	// Read all records
	csvReader.Comma = ';'

	// records, err := csvReader.ReadAll()
	// handleError(err)

	// log.Println("=> records : ", len(records))

	for {
		row, err := csvReader.Read()

		if isHeader {
			isHeader = false
			continue
		}

		if len(row) == 0 {
			continue
		}

		if err != nil {
			if err == io.EOF {
				err = nil
			}

			// log.Println("\n==========START===============\n row => ", row)
			// log.Println("\nERROR EOF => ", err)
			// log.Println("\n=============END============\n")
			break
		}

		for i, field := range row {
			// Apply field replacement operations to each field
			field = strings.ReplaceAll(field, "\xE2\x80\x8B", "")
			field = strings.ReplaceAll(field, "\xEF\xBB\xBF", "")
			field = regexp.MustCompile(`[^(\x20-\x7F)]*`).ReplaceAllString(field, "")
			field = strings.ReplaceAll(field, "\r\n", "")
			field = strings.ReplaceAll(field, "\n\";", "\";")
			field = strings.ReplaceAll(field, "\"", "")
			field = strings.ReplaceAll(field, ",", ".")
			field = strings.ReplaceAll(field, ";;", ";0;")
			field = strings.ReplaceAll(field, ";", ",")

			// Update the field in the row with the modified value
			row[i] = field
		}

		// Check if the record is empty (contains only semicolons)
		isEmpty := true
		for _, field := range row {
			if strings.TrimSpace(field) != "" {
				isEmpty = false
			}
		}

		if isEmpty {
			break
			// close(jobs)
			// continue
		}

		if len(row) > 1 {
			row = []string{strings.Join(row, ";")}
		}

		// Join the row values using a space separator
		rowData := strings.Join(row, "")
		// rowData = strings.ReplaceAll(rowData, ";;;;;;;;;;;;;;;;;;;;;;;;;", "")
		// rowData = strings.ReplaceAll(rowData, ";;;;;;;;;;;;;;;;;;;;;;;;;;;\r\n", "")
		// Replace consecutive semicolons with ;null; before splitting
		// rowData = strings.ReplaceAll(rowData, "\xE2\x80\x8B", "")
		// rowData = strings.ReplaceAll(rowData, "\xEF\xBB\xBF", "")
		// rowData = regexp.MustCompile(`[^(\x20-\x7F)]*`).ReplaceAllString(rowData, "")
		// rowData = strings.ReplaceAll(rowData, "\r\n", "")
		// rowData = strings.ReplaceAll(rowData, "\n\";", "\";")
		// rowData = strings.ReplaceAll(rowData, "\"", "")
		// rowData = strings.ReplaceAll(rowData, ",", ".")
		// rowData = strings.ReplaceAll(rowData, ";;", ";0;")
		// rowData = strings.ReplaceAll(rowData, ";", ",")

		row = strings.Split(rowData, ";")

		//check if no waybill is not 0 or nil
		// if row[0] == "0" {
		// 	continue
		// }

		//skiped no data
		// if len(row) < 25 {
		// 	continue
		// }

		var element DataElement
		if len(row) >= 25 { // Adjust the index based on your CSV structure
			row = row[:len(row)-2]
			element.NoWaybill = row[0]
			// element.TglPengiriman = row[1]
			// element.WaktuTTD = row[17]
			if row[1] == "" {
				row[1] = "0000-00-00"
			}
			TglPengiriman, err := time.Parse("2006-01-02", row[1])
			if err == nil {
				element.TglPengiriman = TglPengiriman
			} else {
				log.Println("\n==========START===============\n row => ", row)
				log.Println("\n column count : ", len(row))
				log.Println("Error parsing TglPengiriman:", err)
			}
			element.DropPointOutgoing = row[2]
			element.SprinterPickup = row[3]
			element.TempatTujuan = row[4]
			element.Keterangan = row[5]
			// element.BeratYangDitagih = row[6]

			if row[6] == "" {
				row[6] = "0"
			}
			// Convert string to float64 and assign to BeratYangDitagih field
			beratYangDitagih, err := strconv.ParseFloat(row[6], 64)
			if err == nil {
				element.BeratYangDitagih = beratYangDitagih
			} else {
				log.Println("Error parsing BeratYangDitagih:", err)
			}

			if row[7] == "" {
				row[7] = "0"
			}
			// element.Cod = row[7]
			cod, err := strconv.ParseInt(row[7], 10, 64)
			if err == nil {
				element.Cod = cod
			} else {
				log.Println("Error parsing Cod:", err)
			}
			// element.BiayaAsuransi = row[8]
			if row[8] == "" {
				row[8] = "0"
			}

			biaya_asuransi, err := strconv.ParseFloat(row[8], 64)
			if err == nil {
				element.BiayaAsuransi = biaya_asuransi
			} else {
				log.Println("Error parsing BiayaAsuransi:", err)
			}

			// element.BiayaAsuransi = row[9]
			if row[9] == "" {
				row[9] = "0"
			}
			biaya_kirim, err := strconv.ParseInt(row[9], 10, 64)
			if err == nil {
				element.BiayaKirim = biaya_kirim
			} else {
				log.Println("Error parsing BiayaKirim:", err)
			}

			// element.BiayaLainnya = row[10]
			if row[10] == "" {
				row[10] = "0"
			}
			biaya_lainnya, err := strconv.ParseInt(row[10], 10, 64)
			if err == nil {
				element.BiayaLainnya = biaya_lainnya
			} else {
				log.Println("Error parsing BiayaLainnya:", err)
			}

			// element.TotalBiaya = row[11]
			if row[11] == "" {
				row[11] = "0"
			}
			total_biaya, err := strconv.ParseFloat(row[11], 64)
			if err == nil {
				element.TotalBiaya = total_biaya
			} else {
				log.Println("Error parsing TotalBiaya:", err)
			}

			element.KlienPengiriman = row[12]
			element.MetodePembayaran = row[13]
			element.NamaPengirim = row[14]
			element.SumberWayBill = row[15]
			element.PaketRetur = row[16]

			// element.WaktuTTD = row[17]
			if row[17] == "" {
				row[17] = "0000-00-00"
			}
			waktuTTD, err := time.Parse("2006-01-02 15:04:05", row[17])
			if err == nil {
				element.WaktuTTD = waktuTTD
			} else {
				log.Println("Error parsing WaktuTTD:", err)
			}

			element.Layanan = row[18]
			// element.Diskon = row[19]
			if row[19] == "" {
				row[19] = "0"
			}
			diskon, err := strconv.ParseInt(row[19], 10, 64)
			if err == nil {
				element.TotalBiayaSetelahDiscon = diskon
			} else {
				log.Println("Error parsing Diskon:", err)
			}
			// element.TotalBiayaSetelahDiscon = row[20]
			// Convert string to int64 and assign to Diskon field
			if row[20] == "" {
				row[20] = "0"
			}
			total_biaya_setelah_diskon, err := strconv.ParseInt(row[20], 10, 64)
			if err == nil {
				element.TotalBiayaSetelahDiscon = total_biaya_setelah_diskon
			} else {
				log.Println("Error parsing TotalBiayaSetelahDiscon:", err)
			}

			element.AgenTujuan = row[21]
			element.Nik = row[22]
			element.KodePromo = row[23]
			element.Kategori = row[24]
		}

		// rowOrdered := make([]interface{}, 0)
		// for _, each := range element {
		// 	rowOrdered = append(rowOrdered, each)
		// }

		// Populate rowOrdered with struct fields
		rowOrdered := []interface{}{
			element.NoWaybill,
			element.TglPengiriman,
			element.DropPointOutgoing,
			element.SprinterPickup,
			element.TempatTujuan,
			element.Keterangan,
			element.BeratYangDitagih,
			element.Cod,
			element.BiayaAsuransi,
			element.BiayaKirim,
			element.BiayaLainnya,
			element.TotalBiaya,
			element.KlienPengiriman,
			element.MetodePembayaran,
			element.NamaPengirim,
			element.SumberWayBill,
			element.PaketRetur,
			element.WaktuTTD,
			element.Layanan,
			element.Diskon,
			element.TotalBiayaSetelahDiscon,
			element.AgenTujuan,
			element.Nik,
			element.KodePromo,
			element.Kategori,
		}

		wg.Add(1)
		jobs <- rowOrdered
	}
	close(jobs)
}

func doTheJob(workerIndex, counter int, conn *pgxpool.Conn, values []interface{}, date *DateParams) {
	schemaName = fmt.Sprintf("cashback_%s_%s", strings.ToLower(date.Month), strings.ToLower(date.Year))
	query := fmt.Sprintf("INSERT INTO %s.domain (%s) VALUES (%s)",
		schemaName,
		strings.Join(dataHeaders, ","),
		strings.Join(generateQuestionsMark(len(dataHeaders)), ","),
	)

	_, err := conn.Exec(context.Background(), query, values...)
	if err != nil {
		log.Println("\n==========START===============\n Values : ", values)
		log.Println("Worker", workerIndex, "error:", err)
		log.Println("\n=============END============\n")
	}

	if counter%100 == 0 {
		fmt.Println("=> worker", workerIndex, "inserted", counter, "data")
	}
	//  else {
	// 	log.Println("=> worker", workerIndex, "inserted", counter, "data executed")
	// }
}

func generateQuestionsMark(n int) []string {
	s := make([]string, 0)
	for i := 0; i < n; i++ {
		s = append(s, "$"+fmt.Sprint(i+1))
	}
	return s
}

// Helper function to compare byte slices
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, val := range a {
		if val != b[i] {
			return false
		}
	}
	return true
}
