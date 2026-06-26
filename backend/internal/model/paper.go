package model

import "time"

type Paper struct {
	ID        int64     `json:"id"`
	SourceID  string    `json:"source_id"`
	Year      int       `json:"year"`
	Title     string    `json:"title"`
	Abstract  string    `json:"abstract"`
	PaperURL  string    `json:"paper_url"`
	PDFURL    string    `json:"pdf_url"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type CountPoint struct {
	Name  string `json:"name"`
	Value int64  `json:"value"`
}

type YearPoint struct {
	Year  int   `json:"year"`
	Count int64 `json:"count"`
}

type PaperList struct {
	Items  []Paper `json:"items"`
	Total  int64   `json:"total"`
	Limit  int     `json:"limit"`
	Offset int     `json:"offset"`
}

type Summary struct {
	Papers       int64 `json:"papers"`
	Years        int64 `json:"years"`
	Keywords     int64 `json:"keywords"`
	Authors      int64 `json:"authors"`
	Institutions int64 `json:"institutions"`
}
