package main

import (
	"bufio"
	"fmt"
	// "github.com/davecgh/go-spew/spew"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"time"
	"sort"
	"math"
	"strconv"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func compare(logdir string, m map[string]map[string]map[string]map[string][]string, start_time string, end_time string) (int, []string, []string, []string, []string, []string, []string, []string, []string, []string) {
	r, _ := regexp.Compile(`^\w+\s(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)Z\s[\w-\/]+\s[\d\.]+:\d+\s[\d\.]+:\d+\s[\d\.]+\s[\d\.]+\s[\d\.]+\s(\d{3})\s\d{3}\s\d+\s\d+\s\"\w+\shttp[s]?:\/\/[\w\.:\d]+(\/[\w\/]+)\?\w+=(Team[\w-]+)`)
	// 1. 2018-02-08T18:18:19
	// 2. .805422 (fractional second)
	// 3. 200 (status)
	// 4. /api/survivors/bulkcommand
	// 5. Team_151090E874F49300_sur-use1a-4_65411

	var no_matches = []string{}
	var no_matches_request = []string{}
	var matches = []string{}
	var no_id = []string{}
	var bad_time = []string{}
	var no_matches_2xx = []string{}
	var no_matches_3xx = []string{}
	var no_matches_4xx = []string{}
	var no_matches_5xx = []string{}
	total_lines := 0
	start, _ := time.Parse(time.RFC3339, start_time)
	end, _ := time.Parse(time.RFC3339, end_time)
	end = end.Add(time.Second)

	files, err := ioutil.ReadDir(logdir)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		file, err := os.OpenFile(logdir+"/"+f.Name(), os.O_RDONLY, os.ModePerm)
		if err != nil {
			log.Fatalf("open file error: %v", err)
			panic(err)
		}
		defer file.Close()

		sc := bufio.NewScanner(file)
		for sc.Scan() {
			line := sc.Text()
			total_lines++

			res := r.FindStringSubmatch(line)
			// time.Sleep(1000 * time.Millisecond)

			if res != nil {
				res1_RFC3339 := res[1]+"Z"
				t, _ := time.Parse(time.RFC3339, res1_RFC3339)
				t2 := t.Add(time.Second)
				if (t.After(start) && t.Before(end)) {
					_, status_match := m[res1_RFC3339][res[4]][res[5]][res[3]]
					_, request_match := m[res1_RFC3339][res[4]][res[5]]
					float_time, _ := strconv.ParseFloat(res[2], 64)
					var next_second_status = false
					var next_second_request = false
					if math.Floor(float_time + 0.5) > 0 {
						next_second := t2.Format(time.RFC3339)
						_, next_second_status = m[next_second][res[4]][res[5]][res[3]]
						_, next_second_request = m[next_second][res[4]][res[5]]
					}

					if !status_match && !next_second_status {
						no_matches = append(no_matches, line)
						if i, _ := strconv.Atoi(res[3]); i < 300 {
							no_matches_2xx = append(no_matches_2xx, line)
						} else if i, _ := strconv.Atoi(res[3]); i < 400 {
							no_matches_3xx = append(no_matches_3xx, line)
							fmt.Printf("%v\n", line)
						} else if i, _ := strconv.Atoi(res[3]); i < 500 {
							no_matches_4xx = append(no_matches_4xx, line)
							fmt.Printf("%v\n", line)
						} else {
							no_matches_5xx = append(no_matches_5xx, line)
							fmt.Printf("%v\n", line)
						}
					} else if !request_match && !next_second_request {
						// fmt.Printf("%v\n", line)
						no_matches_request = append(no_matches, line)
					} else if status_match || next_second_status{
						matches = append(matches, line)
					} else {
						matches = append(matches, line)
					}
				} else {
					bad_time = append(bad_time, line)
				}
			} else {
				no_id = append(no_id, line)
			}
		}
	}

	return total_lines, no_matches, no_matches_request, matches, no_id, bad_time, no_matches_2xx, no_matches_3xx, no_matches_4xx, no_matches_5xx
}

func makeMap(logdir string) (map[string]map[string]map[string]map[string][]string, int, string, string) {

	r, _ := regexp.Compile(`^(\d{4}-\d{2}-\d{2})\t(\d{2}:\d{2}:\d{2}).+\t(\/[\w\/]+)\t(\d{3}).+\t\w+=(Team[\w-]+).+\t(.*==)`)
	// 1. 2018-02-08T18:18:19
	// 2. /api/survivors/bulkcommand
	// 3. 200
	// 4. Team_15115E72DDDEF100_sur-use1d-2_387
	// 5. tR45xclnDMHfn6nzXLWAwJb2j23r27TWy4N0ZeKSoICoJl7d5Q247g==

	m := make(map[string]map[string]map[string]map[string][]string)
	lines := 0

	files, err := ioutil.ReadDir(logdir)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		file, err := os.OpenFile(logdir+"/"+f.Name(), os.O_RDONLY, os.ModePerm)
		if err != nil {
			log.Fatalf("open file error: %v", err)
			panic(err)
		}
		defer file.Close()

		sc := bufio.NewScanner(file)
		for sc.Scan() {
			line := sc.Text()
			lines++
			res := r.FindStringSubmatch(line)
			// fmt.Printf("%v\n", res)
			if res != nil {
				timestamp := res[1]+"T"+res[2]+"Z"
				if m[timestamp] == nil {
					m[timestamp] = make(map[string]map[string]map[string][]string)
				}
				if m[timestamp][res[3]] == nil {
					m[timestamp][res[3]] = make(map[string]map[string][]string)
				}
				if m[timestamp][res[3]][res[5]] == nil {
					m[timestamp][res[3]][res[5]] = make(map[string][]string)
				}
				if m[timestamp][res[3]][res[5]][res[4]] == nil {
					m[timestamp][res[3]][res[5]][res[4]] = []string{}
				}
				m[timestamp][res[3]][res[5]][res[4]] = append(m[timestamp][res[3]][res[5]][res[4]], res[6])
			}
		}
	}

	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	log.Printf("Cloudfront Start: %v", keys[0])
	log.Printf("Cloudfront End: %v", keys[len(keys)-1])

	return m, lines, keys[0], keys[len(keys)-1]
}

func main() {
	var logdir = os.Args[1]
	var alb_logdir = os.Args[2]

	start := time.Now()

	m, cf_lines, start_timestamp, end_timestamp := makeMap(logdir)

	// spew.Dump(m)

	elapsed := time.Since(start)
	log.Printf("Reading Cloudfront logs took %s, %d lines", elapsed, cf_lines)

	alb_lines, no_matches, no_matches_request, matches, no_id, bad_time, no_matches_2xx, no_matches_3xx, no_matches_4xx, no_matches_5xx := compare(alb_logdir, m, start_timestamp, end_timestamp)

	elapsed = time.Since(start)
	log.Printf("Reading ALB logs took %s, %d lines", elapsed, alb_lines)
	log.Printf("%d\t%.2f%%\tno team id", len(no_id), float64(len(no_id)) * float64(100)/float64(alb_lines))
	log.Printf("%d\t%.2f%%\ttimestamp out of range", len(bad_time), (float64(len(bad_time)) * float64(100)/float64(alb_lines)))
	log.Printf("%d\t%.2f%%\tno matches", len(no_matches), (float64(len(no_matches)) * float64(100)/float64(alb_lines)))
	log.Printf("%d\ttotal", len(no_matches))
	log.Printf("%d\ttotal w/different status", len(no_matches_request))
	log.Printf("%d\t\t2xx", len(no_matches_2xx))
	log.Printf("%d\t\t3xx", len(no_matches_3xx))
	log.Printf("%d\t\t4xx", len(no_matches_4xx))
	log.Printf("%d\t\t5xx", len(no_matches_5xx))
	log.Printf("%d\t%.2f%%\tmatches", len(matches), (float64(len(matches)) * float64(100)/float64(alb_lines)))
	log.Printf("total %d", (len(no_id) + len(no_matches) + len(matches)))
}
