package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"html"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const cvfBaseURL = "https://openaccess.thecvf.com/"

var titleBlockPattern = regexp.MustCompile(`(?is)<dt[^>]*class=["'][^"']*ptitle[^"']*["'][^>]*>`)
var anchorPattern = regexp.MustCompile(`(?is)<a[^>]*href=["']([^"']+)["'][^>]*>(.*?)</a>`)
var authorValuePattern = regexp.MustCompile(`(?is)<input[^>]*name=["']query_author["'][^>]*value=["']([^"']+)["'][^>]*>`)
var pdfPattern = regexp.MustCompile(`(?is)<a[^>]*href=["']([^"']+\.pdf)["'][^>]*>`)
var abstractPattern = regexp.MustCompile(`(?is)<div[^>]*id=["']abstract["'][^>]*>(.*?)</div>`)
var tagPattern = regexp.MustCompile(`(?is)<[^>]+>`)
var tokenPattern = regexp.MustCompile(`[a-z][a-z0-9]{2,}`)
var nonWordPattern = regexp.MustCompile(`[^a-z0-9\s]+`)
var separatorPattern = regexp.MustCompile(`[-_/]+`)
var slugPattern = regexp.MustCompile(`[^a-z0-9]+`)

var stopwords = map[string]struct{}{
	"a": {}, "about": {}, "after": {}, "again": {}, "all": {}, "also": {}, "an": {}, "and": {}, "are": {}, "as": {},
	"at": {}, "based": {}, "be": {}, "been": {}, "between": {}, "by": {}, "can": {}, "for": {}, "from": {},
	"in": {}, "into": {}, "is": {}, "it": {}, "its": {}, "learning": {}, "method": {}, "methods": {}, "model": {},
	"models": {}, "of": {}, "on": {}, "or": {}, "our": {}, "paper": {}, "than": {}, "that": {}, "the": {},
	"their": {}, "there": {}, "these": {}, "this": {}, "those": {}, "through": {}, "to": {}, "toward": {},
	"towards": {}, "using": {}, "we": {}, "where": {}, "which": {}, "with": {}, "without": {},
}

var phrases = []string{
	"3d detection",
	"action recognition",
	"adversarial learning",
	"anomaly detection",
	"contrastive learning",
	"depth estimation",
	"diffusion model",
	"domain adaptation",
	"few shot",
	"foundation model",
	"image generation",
	"image segmentation",
	"instance segmentation",
	"language model",
	"medical image",
	"multi modal",
	"object detection",
	"open vocabulary",
	"pose estimation",
	"prompt learning",
	"representation learning",
	"scene understanding",
	"semantic segmentation",
	"self supervised",
	"video understanding",
	"vision language",
	"visual question answering",
}

type Paper struct {
	SourceID     string   `json:"source_id"`
	Year         int      `json:"year"`
	Title        string   `json:"title"`
	Authors      []string `json:"authors"`
	Institutions []string `json:"institutions"`
	Abstract     string   `json:"abstract"`
	Keywords     []string `json:"keywords"`
	PaperURL     string   `json:"paper_url"`
	PDFURL       string   `json:"pdf_url"`
	CrawledAt    string   `json:"crawled_at"`
}

type Config struct {
	Years         []int
	YearWorkers   int
	DetailWorkers int
	LimitPerYear  int
	FetchDetails  bool
	Watch         bool
	WatchInterval time.Duration
	StateFile     string
	DryRun        bool
	Topic         string
	Brokers       []string
}

func main() {
	cfg := parseConfig()
	ctx := context.Background()

	producer := NewKafkaProducer(cfg.Brokers, cfg.Topic)
	seen := loadSeen(cfg.StateFile)
	cycle := 0

	for {
		cycle++
		log.Printf("crawl_cycle_start cycle=%d years=%v seen=%d", cycle, cfg.Years, len(seen))
		papers := crawlAll(ctx, cfg)
		newCount := 0
		for _, paper := range papers {
			if _, ok := seen[paper.SourceID]; ok {
				continue
			}
			if err := emitPaper(ctx, cfg, producer, paper); err != nil {
				log.Printf("emit_failed source_id=%s error=%v", paper.SourceID, err)
				continue
			}
			seen[paper.SourceID] = struct{}{}
			appendSeen(cfg.StateFile, paper.SourceID)
			newCount++
		}
		log.Printf("crawl_cycle_done cycle=%d discovered=%d new=%d seen=%d", cycle, len(papers), newCount, len(seen))
		if !cfg.Watch {
			return
		}
		time.Sleep(cfg.WatchInterval)
	}
}

func parseConfig() Config {
	yearsText := flag.String("years", "2021,2022,2023,2024,2025,2026", "comma-separated CVPR years")
	yearWorkers := flag.Int("year-workers", 6, "parallel year workers")
	detailWorkers := flag.Int("detail-workers", 4, "parallel detail fetch workers per year")
	limitPerYear := flag.Int("limit-per-year", 0, "limit papers per year, 0 means all")
	fetchDetails := flag.Bool("fetch-details", false, "fetch abstract and PDF from detail pages")
	watch := flag.Bool("watch", false, "keep polling and emit newly discovered papers")
	watchInterval := flag.Duration("watch-interval", time.Hour, "interval between watch cycles")
	stateFile := flag.String("state-file", "", "newline source_id state file")
	dryRun := flag.Bool("dry-run", false, "print JSONL instead of producing to Kafka")
	brokers := flag.String("brokers", env("KAFKA_BROKERS", "localhost:29092"), "comma-separated Kafka brokers")
	topic := flag.String("topic", env("KAFKA_TOPIC", "cvpr.raw.papers"), "Kafka topic")
	flag.Parse()

	return Config{
		Years:         parseYears(*yearsText),
		YearWorkers:   max(1, *yearWorkers),
		DetailWorkers: max(1, *detailWorkers),
		LimitPerYear:  *limitPerYear,
		FetchDetails:  *fetchDetails,
		Watch:         *watch,
		WatchInterval: *watchInterval,
		StateFile:     *stateFile,
		DryRun:        *dryRun,
		Topic:         *topic,
		Brokers:       splitCSV(*brokers),
	}
}

func crawlAll(ctx context.Context, cfg Config) []Paper {
	yearJobs := make(chan int)
	results := make(chan []Paper)
	workers := min(cfg.YearWorkers, len(cfg.Years))
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for year := range yearJobs {
				papers, err := crawlYear(ctx, year, cfg)
				if err != nil {
					log.Printf("year_failed year=%d error=%v", year, err)
					results <- nil
					continue
				}
				log.Printf("year_done year=%d papers=%d", year, len(papers))
				results <- papers
			}
		}()
	}

	go func() {
		for _, year := range cfg.Years {
			yearJobs <- year
		}
		close(yearJobs)
		wg.Wait()
		close(results)
	}()

	var merged []Paper
	for batch := range results {
		merged = append(merged, batch...)
	}
	sort.SliceStable(merged, func(i, j int) bool {
		if merged[i].Year == merged[j].Year {
			return merged[i].Title < merged[j].Title
		}
		return merged[i].Year > merged[j].Year
	})
	return merged
}

func crawlYear(ctx context.Context, year int, cfg Config) ([]Paper, error) {
	listingURL := fmt.Sprintf("%sCVPR%d?day=all", cvfBaseURL, year)
	body, status, err := fetch(ctx, listingURL)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		log.Printf("year_unavailable year=%d url=%s", year, listingURL)
		return nil, nil
	}
	if status >= 400 {
		return nil, fmt.Errorf("listing status %d", status)
	}

	papers := parseListing(body, year, listingURL)
	if cfg.LimitPerYear > 0 && len(papers) > cfg.LimitPerYear {
		papers = papers[:cfg.LimitPerYear]
	}
	if cfg.FetchDetails {
		enrichDetails(ctx, papers, cfg.DetailWorkers)
	}
	return papers, nil
}

func parseListing(body string, year int, baseURL string) []Paper {
	matches := titleBlockPattern.FindAllStringIndex(body, -1)
	papers := make([]Paper, 0, len(matches))
	for i, match := range matches {
		windowEnd := len(body)
		if i+1 < len(matches) {
			windowEnd = matches[i+1][0]
		}
		window := body[match[0]:windowEnd]
		anchor := anchorPattern.FindStringSubmatch(window)
		if len(anchor) != 3 {
			continue
		}
		href := html.UnescapeString(anchor[1])
		title := cleanText(anchor[2])
		if title == "" {
			continue
		}
		authors := parseAuthorValues(window)
		paperURL := resolveURL(baseURL, href)
		pdfURL := ""
		if pdfMatch := pdfPattern.FindStringSubmatch(window); len(pdfMatch) == 2 {
			pdfURL = resolveURL(baseURL, html.UnescapeString(pdfMatch[1]))
		}
		papers = append(papers, normalizePaper(Paper{
			SourceID:     stableSourceID(year, title),
			Year:         year,
			Title:        title,
			Authors:      authors,
			Institutions: []string{},
			Keywords:     extractKeywords(title, 8),
			PaperURL:     paperURL,
			PDFURL:       pdfURL,
			CrawledAt:    time.Now().UTC().Format(time.RFC3339Nano),
		}))
	}
	return papers
}

func parseAuthorValues(block string) []string {
	matches := authorValuePattern.FindAllStringSubmatch(block, -1)
	if len(matches) == 0 {
		return nil
	}
	authors := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) == 2 {
			authors = append(authors, html.UnescapeString(match[1]))
		}
	}
	return uniqueClean(authors)
}

func enrichDetails(ctx context.Context, papers []Paper, workers int) {
	jobs := make(chan int)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				abstract, pdfURL, err := fetchDetail(ctx, papers[index].PaperURL)
				if err != nil {
					log.Printf("detail_failed url=%s error=%v", papers[index].PaperURL, err)
					continue
				}
				papers[index].Abstract = abstract
				if papers[index].PDFURL == "" {
					papers[index].PDFURL = pdfURL
				}
				papers[index].Keywords = extractKeywords(papers[index].Title+" "+abstract, 8)
			}
		}()
	}
	for index := range papers {
		jobs <- index
	}
	close(jobs)
	wg.Wait()
}

func fetchDetail(ctx context.Context, pageURL string) (string, string, error) {
	body, status, err := fetch(ctx, pageURL)
	if err != nil {
		return "", "", err
	}
	if status >= 400 {
		return "", "", fmt.Errorf("detail status %d", status)
	}
	abstract := ""
	if match := abstractPattern.FindStringSubmatch(body); len(match) == 2 {
		abstract = strings.TrimSpace(strings.TrimPrefix(cleanText(match[1]), "Abstract"))
	}
	pdfURL := ""
	if match := pdfPattern.FindStringSubmatch(body); len(match) == 2 {
		pdfURL = resolveURL(pageURL, html.UnescapeString(match[1]))
	}
	return abstract, pdfURL, nil
}

func emitPaper(ctx context.Context, cfg Config, producer *KafkaProducer, paper Paper) error {
	payload, err := json.Marshal(paper)
	if err != nil {
		return err
	}
	if cfg.DryRun {
		fmt.Println(string(payload))
		return nil
	}
	return producer.Produce(ctx, paper.SourceID, payload)
}

func fetch(ctx context.Context, target string) (string, int, error) {
	client := &http.Client{Timeout: 25 * time.Second}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return "", 0, err
		}
		req.Header.Set("User-Agent", "display-pod-cvpr-go-crawler/1.0")
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", resp.StatusCode, err
		}
		return string(data), resp.StatusCode, nil
	}
	return "", 0, lastErr
}

type KafkaProducer struct {
	brokers       []string
	topic         string
	correlationID int32
}

func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	return &KafkaProducer{brokers: brokers, topic: topic}
}

func (p *KafkaProducer) Produce(ctx context.Context, key string, value []byte) error {
	var lastErr error
	for _, broker := range p.brokers {
		if err := p.produceToBroker(ctx, broker, key, value); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return errors.New("no kafka brokers configured")
}

func (p *KafkaProducer) produceToBroker(ctx context.Context, broker string, key string, value []byte) error {
	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))

	correlationID := atomic.AddInt32(&p.correlationID, 1)
	request := buildProduceRequest(p.topic, key, value, correlationID)
	if _, err := conn.Write(request); err != nil {
		return err
	}
	var sizeBuf [4]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return err
	}
	size := binary.BigEndian.Uint32(sizeBuf[:])
	response := make([]byte, size)
	if _, err := io.ReadFull(conn, response); err != nil {
		return err
	}
	if err := parseProduceResponse(response); err != nil {
		return err
	}
	return nil
}

func buildProduceRequest(topic string, key string, value []byte, correlationID int32) []byte {
	var body bytes.Buffer
	writeInt16(&body, 1)
	writeInt32(&body, 10000)
	writeInt32(&body, 1)
	writeString(&body, topic)
	writeInt32(&body, 1)
	writeInt32(&body, 0)
	recordSet := buildMessageSet(key, value)
	writeBytes(&body, recordSet)

	var request bytes.Buffer
	writeInt16(&request, 0)
	writeInt16(&request, 2)
	writeInt32(&request, correlationID)
	writeString(&request, "display-pod-crawler-go")
	request.Write(body.Bytes())

	payload := request.Bytes()
	var framed bytes.Buffer
	writeInt32(&framed, int32(len(payload)))
	framed.Write(payload)
	return framed.Bytes()
}

func buildMessageSet(key string, value []byte) []byte {
	var message bytes.Buffer
	message.WriteByte(1)
	message.WriteByte(0)
	writeInt64(&message, time.Now().UnixMilli())
	writeNullableBytes(&message, []byte(key))
	writeNullableBytes(&message, value)
	crc := crc32.ChecksumIEEE(message.Bytes())

	var messageWithCRC bytes.Buffer
	writeInt32(&messageWithCRC, int32(crc))
	messageWithCRC.Write(message.Bytes())

	var set bytes.Buffer
	writeInt64(&set, 0)
	writeInt32(&set, int32(messageWithCRC.Len()))
	set.Write(messageWithCRC.Bytes())
	return set.Bytes()
}

func parseProduceResponse(response []byte) error {
	reader := bytes.NewReader(response)
	if _, err := readInt32(reader); err != nil {
		return err
	}
	topicCount, err := readInt32(reader)
	if err != nil {
		return err
	}
	for i := int32(0); i < topicCount; i++ {
		if _, err := readString(reader); err != nil {
			return err
		}
		partitionCount, err := readInt32(reader)
		if err != nil {
			return err
		}
		for j := int32(0); j < partitionCount; j++ {
			if _, err := readInt32(reader); err != nil {
				return err
			}
			errorCode, err := readInt16(reader)
			if err != nil {
				return err
			}
			if errorCode != 0 {
				return fmt.Errorf("kafka produce error code %d", errorCode)
			}
			if _, err := readInt64(reader); err != nil {
				return err
			}
			if _, err := readInt64(reader); err != nil {
				return err
			}
		}
	}
	return nil
}

func normalizePaper(paper Paper) Paper {
	paper.Title = cleanText(paper.Title)
	paper.Abstract = cleanText(paper.Abstract)
	paper.Authors = uniqueClean(paper.Authors)
	paper.Keywords = uniqueClean(paper.Keywords)
	if len(paper.Keywords) == 0 {
		paper.Keywords = extractKeywords(paper.Title+" "+paper.Abstract, 8)
	}
	if paper.CrawledAt == "" {
		paper.CrawledAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	return paper
}

func extractKeywords(text string, limit int) []string {
	normalized := strings.ToLower(separatorPattern.ReplaceAllString(text, " "))
	normalized = nonWordPattern.ReplaceAllString(normalized, " ")
	scores := map[string]int{}
	for _, phrase := range phrases {
		if strings.Contains(normalized, phrase) {
			scores[phrase] += 4
		}
	}
	for _, token := range tokenPattern.FindAllString(normalized, -1) {
		if _, ok := stopwords[token]; ok {
			continue
		}
		scores[token]++
	}
	type pair struct {
		key   string
		score int
	}
	pairs := make([]pair, 0, len(scores))
	for key, score := range scores {
		pairs = append(pairs, pair{key: key, score: score})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].score == pairs[j].score {
			return pairs[i].key < pairs[j].key
		}
		return pairs[i].score > pairs[j].score
	})
	if len(pairs) > limit {
		pairs = pairs[:limit]
	}
	keywords := make([]string, 0, len(pairs))
	for _, item := range pairs {
		keywords = append(keywords, item.key)
	}
	return keywords
}

func stableSourceID(year int, title string) string {
	slug := strings.Trim(slugPattern.ReplaceAllString(strings.ToLower(title), "-"), "-")
	if len(slug) > 80 {
		slug = slug[:80]
	}
	sum := sha1.Sum([]byte(fmt.Sprintf("%d:%s", year, title)))
	return fmt.Sprintf("cvpr-%d-%s-%s", year, slug, hex.EncodeToString(sum[:])[:10])
}

func parseAuthors(value string) []string {
	value = regexp.MustCompile(`(?i)\s+and\s+`).ReplaceAllString(value, ", ")
	return uniqueClean(strings.Split(value, ","))
}

func uniqueClean(values []string) []string {
	seen := map[string]struct{}{}
	var cleaned []string
	for _, value := range values {
		value = cleanText(value)
		key := strings.ToLower(value)
		if value == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		cleaned = append(cleaned, value)
	}
	return cleaned
}

func cleanText(value string) string {
	value = tagPattern.ReplaceAllString(value, " ")
	value = html.UnescapeString(value)
	return strings.Join(strings.Fields(value), " ")
}

func resolveURL(base string, ref string) string {
	baseURL, err := url.Parse(base)
	if err != nil {
		return ref
	}
	refURL, err := url.Parse(ref)
	if err != nil {
		return ref
	}
	return baseURL.ResolveReference(refURL).String()
}

func loadSeen(path string) map[string]struct{} {
	seen := map[string]struct{}{}
	if path == "" {
		return seen
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return seen
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			seen[line] = struct{}{}
		}
	}
	return seen
}

func appendSeen(path string, sourceID string) {
	if path == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		log.Printf("state_dir_failed path=%s error=%v", path, err)
		return
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Printf("state_open_failed path=%s error=%v", path, err)
		return
	}
	defer file.Close()
	if _, err := fmt.Fprintln(file, sourceID); err != nil {
		log.Printf("state_write_failed path=%s error=%v", path, err)
	}
}

func parseYears(value string) []int {
	parts := splitCSV(value)
	years := make([]int, 0, len(parts))
	for _, part := range parts {
		year, err := strconv.Atoi(part)
		if err == nil {
			years = append(years, year)
		}
	}
	return years
}

func splitCSV(value string) []string {
	var values []string
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			values = append(values, part)
		}
	}
	return values
}

func env(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func writeInt16(w io.Writer, value int16) { _ = binary.Write(w, binary.BigEndian, value) }
func writeInt32(w io.Writer, value int32) { _ = binary.Write(w, binary.BigEndian, value) }
func writeInt64(w io.Writer, value int64) { _ = binary.Write(w, binary.BigEndian, value) }

func writeString(w io.Writer, value string) {
	writeInt16(w, int16(len(value)))
	_, _ = w.Write([]byte(value))
}

func writeBytes(w io.Writer, value []byte) {
	writeInt32(w, int32(len(value)))
	_, _ = w.Write(value)
}

func writeNullableBytes(w io.Writer, value []byte) {
	if value == nil {
		writeInt32(w, -1)
		return
	}
	writeBytes(w, value)
}

func readInt16(r io.Reader) (int16, error) {
	var value int16
	err := binary.Read(r, binary.BigEndian, &value)
	return value, err
}

func readInt32(r io.Reader) (int32, error) {
	var value int32
	err := binary.Read(r, binary.BigEndian, &value)
	return value, err
}

func readInt64(r io.Reader) (int64, error) {
	var value int64
	err := binary.Read(r, binary.BigEndian, &value)
	return value, err
}

func readString(r io.Reader) (string, error) {
	length, err := readInt16(r)
	if err != nil {
		return "", err
	}
	if length < 0 {
		return "", nil
	}
	data := make([]byte, length)
	_, err = io.ReadFull(r, data)
	return string(data), err
}
