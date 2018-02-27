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
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func compare(logdir string, m map[string]map[string]map[string]map[string]map[string][]string, r *regexp.Regexp) (int, []string, []string, []string) {

	var no_matches = []string{}
	var matches = []string{}
	var no_id = []string{}
	total_lines := 0

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
				// fmt.Printf("%v\n", line)
				_, ok := m[res[1]][res[2]][res[5]][res[6]][res[4]]
				// _, ok := m[res[1]][res[2]][res[5]][res[6]]
				if !ok {
					fmt.Printf("NO MATCH: %v : %v : %v : %v : %v\n\n", res[1], res[2], res[5], res[6], res[4])
					// fmt.Printf("NO MATCH: %v : %v : %v : %v \n\n", res[1], res[2], res[5], res[6])
					no_matches = append(no_matches, line)
				} else {
					fmt.Printf("MATCH: %v : %v : %v : %v : %v\n\n", res[1], res[2], res[5], res[6], res[4])
					// fmt.Printf("MATCH: %v : %v : %v : %v \n\n", res[1], res[2], res[5], res[6])
					matches = append(matches, line)
				}

				// [2018-02-08][18:18:19][/api/survivors/bulkcommand][id=Team_15115E72DDDEF100_sur-use1d-2_387][200]
				// 1. 2018-02-08
				// 2. 18:18:19
				// 3. 805422 (ms)
				// 4. 200 (status)
				// 5. /api/survivors/bulkcommand
				// 6. id=Team_151090E874F49300_sur-use1a-4_65411
			} else {
				no_id = append(no_id, line)
			}
		}
	}

	return total_lines, no_matches, matches, no_id
}

func makeMap(logdir string, r *regexp.Regexp) (map[string]map[string]map[string]map[string]map[string][]string, int) {
	m := make(map[string]map[string]map[string]map[string]map[string][]string)
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
		lines++
		for sc.Scan() {
			line := sc.Text()
			res := r.FindStringSubmatch(line)
			// fmt.Printf("%v\n", res)
			if res != nil {
				if m[res[1]] == nil {
					m[res[1]] = make(map[string]map[string]map[string]map[string][]string)
				}
				if m[res[1]][res[2]] == nil {
					m[res[1]][res[2]] = make(map[string]map[string]map[string][]string)
				}
				if m[res[1]][res[2]][res[3]] == nil {
					m[res[1]][res[2]][res[3]] = make(map[string]map[string][]string)
				}
				if m[res[1]][res[2]][res[3]][res[5]] == nil {
					m[res[1]][res[2]][res[3]][res[5]] = make(map[string][]string)
				}
				if m[res[1]][res[2]][res[3]][res[5]][res[4]] == nil {
					m[res[1]][res[2]][res[3]][res[5]][res[4]] = []string{}
				}
				m[res[1]][res[2]][res[3]][res[5]][res[4]] = append(m[res[1]][res[2]][res[3]][res[5]][res[4]], res[6])
			}
		}
	}

	return m, lines

	// 1. [2018-02-08][18:18:19][/api/survivors/bulkcommand][id=Team_15115E72DDDEF100_sur-use1d-2_387][200][tR45xclnDMHfn6nzXLWAwJb2j23r27TWy4N0ZeKSoICoJl7d5Q247g==]
	// 2. 18:18:19
	// 3. /api/survivors/bulkcommand
	// 4. 200
	// 5. id=Team_15115E72DDDEF100_sur-use1d-2_387
	// 6. tR45xclnDMHfn6nzXLWAwJb2j23r27TWy4N0ZeKSoICoJl7d5Q247g==
}

func main() {
	var logdir = os.Args[1]
	var alb_logdir = os.Args[2]

	// cf := make(map[string]map[string]map[string]map[string]map[string][]string)

	cf_regexp, _ := regexp.Compile(`^(\d{4}-\d{2}-\d{2})\t(\d{2}:\d{2}:\d{2}).+\t(\/[\w\/]+)\t(\d{3}).+\t\w+=(Team[\w-]+).+\t(.*==)`)
	alb_regexp, _ := regexp.Compile(`^\w+\s(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})\.(\d+)Z\s[\w-\/]+\s[\d\.]+:\d+\s[\d\.]+:\d+\s[\d\.]+\s[\d\.]+\s[\d\.]+\s(\d{3})\s\d{3}\s\d+\s\d+\s\"\w+\shttp[s]?:\/\/[\w\.:\d]+(\/[\w\/]+)\?\w+=(Team[\w-]+)`)

	start := time.Now()

	m, cf_lines := makeMap(logdir, cf_regexp)

	// m = makeMap("/tmp/cf_oneline.log", cf)
	//spew.Dump(cf)

	elapsed := time.Since(start)
	log.Printf("Reading cf logs took %s, %d lines", elapsed, cf_lines)

	alb_lines, no_matches, matches, no_id := compare(alb_logdir, m, alb_regexp)

	elapsed = time.Since(start)
	log.Printf("Reading alb logs took %s, %d lines, %d no id, %d no matches, %d matches (%d total)", elapsed, alb_lines, len(no_id), len(no_matches), len(matches), (len(no_id) + len(no_matches) + len(matches)))

	// for k, v := range no_matches {
	// 	fmt.Printf("%d : %s\n", k, v)
	// }

	log.Println("Done\n")

}
