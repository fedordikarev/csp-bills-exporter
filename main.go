package main

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

var AwsExportFields = map[string]int{
	"identity/TimeInterval":   -1,
	"lineItem/UsageEndDate":   -1,
	"lineItem/UsageStartDate": -1,
	"lineItem/BlendedCost":    -1,
	"lineItem/LineItemType":   -1,
}
var AwsOutFieldsOrder = [...]string{
	"lineItem/UsageEndDate",
	"lineItem/BlendedCost",
	"lineItem/LineItemType",
	"lineItem/UsageStartDate",
}

func parseCSV(inFname string, outFname string) {
	timeIntervalsCounter := make(map[string]int, 0)
	startDatesCounter := make(map[string]int, 0)
	endDatesCounter := make(map[string]int, 0)
	f, err := os.Open(inFname)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var r csv.Reader

	if strings.HasSuffix(inFname, ".gz") {
		gr, err := gzip.NewReader(f)
		if err != nil {
			log.Fatal(err)
		}
		defer gr.Close()
		r = *csv.NewReader(gr)
	} else {
		r = *csv.NewReader(f)
	}

	fOut, err := os.Create(outFname)
	if err != nil {
		log.Fatal(err)
	}
	defer fOut.Close()
	w := csv.NewWriter(fOut)
	defer w.Flush()

	headers, _ := r.Read()
	for i, h := range headers {
		if _, ok := AwsExportFields[h]; ok {
			AwsExportFields[h] = i
		}
	}
	outLen := len(AwsOutFieldsOrder)
	out_rec := make([]string, outLen)
	copy(out_rec, AwsOutFieldsOrder[:])
	/*
		for j, h := range AwsOutFieldsOrder {
			out_rec[j] = h
		}
	*/
	w.Write(out_rec)
	// fmt.Println(out_rec)

	if AwsExportFields["identity/TimeInterval"] < 0 {
		log.Fatal("No identity/TimeInterval field found. Bad billing report.")
	}
	idn_idx := AwsExportFields["identity/TimeInterval"]
	today := time.Now().UTC()
	midnight := time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, time.UTC)
	yesterday := midnight.AddDate(0, 0, -1)
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		check_date_str, _, _ := strings.Cut(rec[idn_idx], "/")
		check_date, err := time.Parse(time.RFC3339, check_date_str)
		if err != nil {
			log.Fatal(err)
		}
		if check_date.After(midnight) || check_date.Before(yesterday) || check_date.Equal(midnight) {
			continue
		}
		out_rec := make([]string, outLen)
		for j, h := range AwsOutFieldsOrder {
			if AwsExportFields[h] >= 0 {
				out_rec[j] = rec[AwsExportFields[h]]
			} else {
				out_rec[j] = ""
			}
		}
		// fmt.Println(out_rec)
		w.Write(out_rec)
		if AwsExportFields["identity/TimeInterval"] > -1 {
			timeInterval := rec[AwsExportFields["identity/TimeInterval"]]
			timeIntervalsCounter[timeInterval] += 1
		}
		if AwsExportFields["lineItem/UsageStartDate"] > -1 {
			timeInterval := rec[AwsExportFields["lineItem/UsageStartDate"]]
			startDatesCounter[timeInterval] += 1
		}
		if AwsExportFields["lineItem/UsageEndDate"] > -1 {
			timeInterval := rec[AwsExportFields["lineItem/UsageEndDate"]]
			endDatesCounter[timeInterval] += 1
		}
	}

	// fmt.Printf("%v\n", timeIntervalsCounter)
	fmt.Println("timeIntervalsCounter")
	for k, v := range timeIntervalsCounter {
		fmt.Println(k, ":", v)
	}
	return
	fmt.Println("startDatesCounter")
	for k, v := range startDatesCounter {
		fmt.Println(k, ":", v)
	}
	fmt.Println("endDatesCounter")
	for k, v := range endDatesCounter {
		fmt.Println(k, ":", v)
	}
}

func main() {
	parseCSV(os.Args[1], os.Args[2])
	fmt.Println("Hello!")
}
