package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"display-pod/backend/internal/model"
)

type PaperRepository struct {
	db *sql.DB
}

var keywordStopwords = map[string]struct{}{
	"a": {}, "about": {}, "after": {}, "again": {}, "all": {}, "also": {}, "an": {}, "and": {}, "are": {}, "as": {},
	"at": {}, "based": {}, "be": {}, "been": {}, "between": {}, "by": {}, "can": {}, "for": {}, "from": {},
	"in": {}, "into": {}, "is": {}, "it": {}, "its": {}, "learning": {}, "method": {}, "methods": {}, "model": {},
	"models": {}, "of": {}, "on": {}, "or": {}, "our": {}, "paper": {}, "than": {}, "that": {}, "the": {},
	"their": {}, "there": {}, "these": {}, "this": {}, "those": {}, "through": {}, "to": {}, "toward": {},
	"towards": {}, "using": {}, "we": {}, "where": {}, "which": {}, "with": {}, "without": {},
}

func NewPaperRepository(db *sql.DB) *PaperRepository {
	return &PaperRepository{db: db}
}

func (r *PaperRepository) List(ctx context.Context, query string, year int, limit int, offset int) (model.PaperList, error) {
	where := []string{"1=1"}
	args := []any{}
	if query != "" {
		where = append(where, "(title LIKE ? OR abstract LIKE ?)")
		like := "%" + query + "%"
		args = append(args, like, like)
	}
	if year > 0 {
		where = append(where, "year = ?")
		args = append(args, year)
	}
	whereSQL := strings.Join(where, " AND ")

	var total int64
	if err := r.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM papers WHERE %s`, whereSQL), args...).Scan(&total); err != nil {
		return model.PaperList{}, err
	}

	sqlText := fmt.Sprintf(`SELECT id, source_id, year, title, abstract, paper_url, pdf_url, created_at, updated_at
		FROM papers WHERE %s ORDER BY year DESC, id DESC LIMIT ? OFFSET ?`, whereSQL)
	queryArgs := append(append([]any{}, args...), limit, offset)
	rows, err := r.db.QueryContext(ctx, sqlText, queryArgs...)
	if err != nil {
		return model.PaperList{}, err
	}
	defer rows.Close()

	var papers []model.Paper
	for rows.Next() {
		var paper model.Paper
		if err := rows.Scan(&paper.ID, &paper.SourceID, &paper.Year, &paper.Title, &paper.Abstract, &paper.PaperURL, &paper.PDFURL, &paper.CreatedAt, &paper.UpdatedAt); err != nil {
			return model.PaperList{}, err
		}
		papers = append(papers, paper)
	}
	if err := rows.Err(); err != nil {
		return model.PaperList{}, err
	}
	return model.PaperList{Items: papers, Total: total, Limit: limit, Offset: offset}, nil
}

func (r *PaperRepository) YearlyStats(ctx context.Context) ([]model.YearPoint, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT year, paper_count FROM stats_yearly ORDER BY year`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var points []model.YearPoint
	for rows.Next() {
		var point model.YearPoint
		if err := rows.Scan(&point.Year, &point.Count); err != nil {
			return nil, err
		}
		points = append(points, point)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(points) > 0 {
		return points, nil
	}
	return r.yearlyStatsFromDetails(ctx)
}

func (r *PaperRepository) KeywordStats(ctx context.Context, limit int) ([]model.CountPoint, error) {
	scanLimit := max(limit*5, limit)
	points, err := r.countStats(ctx, `SELECT keyword, paper_count FROM stats_keywords ORDER BY paper_count DESC, keyword LIMIT ?`, scanLimit)
	if err != nil {
		return nil, err
	}
	points = filterKeywordStopwords(points, limit)
	if len(points) > 0 {
		return points, nil
	}
	points, err = r.countStats(ctx, `SELECT keyword, COUNT(*) FROM paper_keywords GROUP BY keyword ORDER BY COUNT(*) DESC, keyword LIMIT ?`, scanLimit)
	if err != nil {
		return nil, err
	}
	return filterKeywordStopwords(points, limit), nil
}

func (r *PaperRepository) AuthorStats(ctx context.Context, limit int) ([]model.CountPoint, error) {
	points, err := r.countStats(ctx, `SELECT author_name, paper_count FROM stats_authors ORDER BY paper_count DESC, author_name LIMIT ?`, limit)
	if err != nil || len(points) > 0 {
		return points, err
	}
	return r.countStats(ctx, `SELECT author_name, COUNT(*) FROM paper_authors GROUP BY author_name ORDER BY COUNT(*) DESC, author_name LIMIT ?`, limit)
}

func (r *PaperRepository) InstitutionStats(ctx context.Context, limit int) ([]model.CountPoint, error) {
	points, err := r.countStats(ctx, `SELECT institution_name, paper_count FROM stats_institutions ORDER BY paper_count DESC, institution_name LIMIT ?`, limit)
	if err != nil || len(points) > 0 {
		return points, err
	}
	return r.countStats(ctx, `SELECT institution_name, COUNT(*) FROM paper_institutions GROUP BY institution_name ORDER BY COUNT(*) DESC, institution_name LIMIT ?`, limit)
}

func (r *PaperRepository) Years(ctx context.Context) ([]int, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT DISTINCT year FROM papers ORDER BY year DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var years []int
	for rows.Next() {
		var year int
		if err := rows.Scan(&year); err != nil {
			return nil, err
		}
		years = append(years, year)
	}
	return years, rows.Err()
}

func (r *PaperRepository) Summary(ctx context.Context) (model.Summary, error) {
	var summary model.Summary
	queries := []struct {
		target *int64
		sql    string
	}{
		{&summary.Papers, `SELECT COUNT(*) FROM papers`},
		{&summary.Years, `SELECT COUNT(DISTINCT year) FROM papers`},
		{&summary.Keywords, `SELECT COUNT(DISTINCT keyword) FROM paper_keywords WHERE LOWER(keyword) NOT IN (` + keywordStopwordPlaceholders() + `)`},
		{&summary.Authors, `SELECT COUNT(DISTINCT author_name) FROM paper_authors`},
		{&summary.Institutions, `SELECT COUNT(DISTINCT institution_name) FROM paper_institutions`},
	}
	for _, query := range queries {
		if err := r.db.QueryRowContext(ctx, query.sql).Scan(query.target); err != nil {
			return model.Summary{}, err
		}
	}
	return summary, nil
}

func filterKeywordStopwords(points []model.CountPoint, limit int) []model.CountPoint {
	filtered := make([]model.CountPoint, 0, min(len(points), limit))
	for _, point := range points {
		if _, ok := keywordStopwords[strings.ToLower(point.Name)]; ok {
			continue
		}
		filtered = append(filtered, point)
		if len(filtered) >= limit {
			break
		}
	}
	return filtered
}

func keywordStopwordPlaceholders() string {
	parts := make([]string, 0, len(keywordStopwords))
	for word := range keywordStopwords {
		parts = append(parts, "'"+word+"'")
	}
	return strings.Join(parts, ",")
}

func (r *PaperRepository) UpdatedAfter(ctx context.Context, after time.Time, limit int) ([]model.Paper, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT id, source_id, year, title, abstract, paper_url, pdf_url, created_at, updated_at
		FROM papers WHERE updated_at > ? ORDER BY updated_at ASC LIMIT ?`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var papers []model.Paper
	for rows.Next() {
		var paper model.Paper
		if err := rows.Scan(&paper.ID, &paper.SourceID, &paper.Year, &paper.Title, &paper.Abstract, &paper.PaperURL, &paper.PDFURL, &paper.CreatedAt, &paper.UpdatedAt); err != nil {
			return nil, err
		}
		papers = append(papers, paper)
	}
	return papers, rows.Err()
}

func (r *PaperRepository) countStats(ctx context.Context, sqlText string, limit int) ([]model.CountPoint, error) {
	rows, err := r.db.QueryContext(ctx, sqlText, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var points []model.CountPoint
	for rows.Next() {
		var point model.CountPoint
		if err := rows.Scan(&point.Name, &point.Value); err != nil {
			return nil, err
		}
		points = append(points, point)
	}
	return points, rows.Err()
}

func (r *PaperRepository) yearlyStatsFromDetails(ctx context.Context) ([]model.YearPoint, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT year, COUNT(*) FROM papers GROUP BY year ORDER BY year`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var points []model.YearPoint
	for rows.Next() {
		var point model.YearPoint
		if err := rows.Scan(&point.Year, &point.Count); err != nil {
			return nil, err
		}
		points = append(points, point)
	}
	return points, rows.Err()
}
