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
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func compare(logdir string, m map[string]map[string]map[string]map[string][]string, start_time string, end_time string) (int, []string, []string, []string, []string) {
	r, _ := regexp.Compile(`^\w+\s(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d+)Z\s[\w-\/]+\s[\d\.]+:\d+\s[\d\.]+:\d+\s[\d\.]+\s[\d\.]+\s[\d\.]+\s(\d{3})\s\d{3}\s\d+\s\d+\s\"\w+\shttp[s]?:\/\/[\w\.:\d]+(\/[\w\/]+)\?\w+=(Team[\w-]+)`)
	// 1. 2018-02-08T18:18:19
	// 2. 805422 (ms)
	// 3. 200 (status)
	// 4. /api/survivors/bulkcommand
	// 5. Team_151090E874F49300_sur-use1a-4_65411

	var no_matches = []string{}
	var matches = []string{}
	var no_id = []string{}
	var bad_time = []string{}
	total_lines := 0
	start, _ := time.Parse(time.RFC3339, start_time)
	end, _ := time.Parse(time.RFC3339, end_time)

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
				// log.Printf("time = %v", res1_RFC3339)
				// log.Printf("start = %v", start)
				// log.Printf("t.After(start) = %v", t.After(start))
				// log.Printf("end = %v", end)
				// log.Printf("t.Before(end) = %v", t.Before(end))
				if (t.After(start) && t.Before(end)) {
					// log.Printf("Out of range: %v", res1_RFC3339)
					_, status_match := m[res1_RFC3339][res[4]][res[5]][res[3]]
					_, request_match := m[res1_RFC3339][res[4]][res[5]]
					if !status_match {
						fmt.Printf("%v\n", line)
						no_matches = append(no_matches, line)
					} else if !request_match {
						fmt.Printf("%v\n", line)
						no_matches = append(no_matches, line)
					} else if status_match {
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

	return total_lines, no_matches, matches, no_id, bad_time
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

	log.Printf("Timestamp: %v", keys[0])

	return m, lines, keys[0], keys[len(keys)-1]
}

func main() {
	var logdir = os.Args[1]
	var alb_logdir = os.Args[2]

	start := time.Now()

	m, cf_lines, start_timestamp, end_timestamp := makeMap(logdir)

	// spew.Dump(m)

	elapsed := time.Since(start)
	log.Printf("Reading cf logs took %s, %d lines", elapsed, cf_lines)

	alb_lines, no_matches, matches, no_id, bad_time := compare(alb_logdir, m, start_timestamp, end_timestamp)

	elapsed = time.Since(start)
	log.Printf("Reading alb logs took %s, %d lines, %d no id, %d timestamp out of range, %d no matches, %d matches (%d total)", elapsed, alb_lines, len(no_id), len(bad_time), len(no_matches), len(matches), (len(no_id) + len(no_matches) + len(matches)))

	log.Println("Done\n")

}
