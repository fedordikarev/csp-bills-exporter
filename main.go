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

var AwsExportFields = map[string]bool{"lineItem/UsageEndDate": true, "lineItem/BlendedCost": true}

func parseCSV(fname string) {
	f, err := os.Open(fname)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var r csv.Reader

	if strings.HasSuffix(fname, ".gz") {
		gr, err := gzip.NewReader(f)
		if err != nil {
			log.Fatal(err)
		}
		defer gr.Close()
		r = *csv.NewReader(gr)
	} else {
		r = *csv.NewReader(f)
	}

	fOut, err := os.Create("out-go.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer fOut.Close()
	w := csv.NewWriter(fOut)
	defer w.Flush()

	headers, _ := r.Read()
	fieldsNum := make([]int, 0)
	for i, h := range headers {
		if AwsExportFields[h] {
			fieldsNum = append(fieldsNum, i)
		}
	}
	outLen := len(fieldsNum)
	out_rec := make([]string, outLen)
	for j, idx := range fieldsNum {
		out_rec[j] = headers[idx]
	}
	w.Write(out_rec)
	// fmt.Println(out_rec)

	// fmt.Println(fieldsNum)
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		out_rec := make([]string, outLen)
		for j, idx := range fieldsNum {
			out_rec[j] = rec[idx]
		}
		// fmt.Println(out_rec)
		w.Write(out_rec)
	}
}

func main() {
	parseCSV("/Users/fe/w/aws-billing/daily-00001.csv.gz")
	fmt.Println("Hello!")
}
