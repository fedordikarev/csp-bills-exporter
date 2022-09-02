package main

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

var AwsExportFields = map[string]int{
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
	for j, h := range AwsOutFieldsOrder {
		out_rec[j] = h
	}
	w.Write(out_rec)
	// fmt.Println(out_rec)

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
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
	}
}

func main() {
	parseCSV(os.Args[1], os.Args[2])
	fmt.Println("Hello!")
}
