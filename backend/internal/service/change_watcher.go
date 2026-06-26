package service

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"display-pod/backend/internal/config"
	"display-pod/backend/internal/model"
	"display-pod/backend/internal/repository"
	"display-pod/backend/internal/ws"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
)

type ChangeWatcher struct {
	cfg    config.Config
	papers *repository.PaperRepository
	hub    *ws.Hub
}

func NewChangeWatcher(cfg config.Config, papers *repository.PaperRepository, hub *ws.Hub) *ChangeWatcher {
	return &ChangeWatcher{cfg: cfg, papers: papers, hub: hub}
}

func (w *ChangeWatcher) Run(ctx context.Context) {
	if w.cfg.BinlogEnabled {
		if err := w.runBinlog(ctx); err != nil && ctx.Err() == nil {
			log.Printf("binlog watcher unavailable, falling back to polling: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
	w.runPolling(ctx, 3*time.Second)
}

func (w *ChangeWatcher) runPolling(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lastSeen := time.Now().Add(-24 * time.Hour)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			updated, err := w.papers.UpdatedAfter(ctx, lastSeen, 50)
			if err != nil {
				log.Printf("watch paper changes: %v", err)
				continue
			}
			for _, paper := range updated {
				if paper.UpdatedAt.After(lastSeen) {
					lastSeen = paper.UpdatedAt
				}
				w.hub.Broadcast(ws.Event{Type: "paper.updated", Payload: paper})
			}
		}
	}
}

func (w *ChangeWatcher) runBinlog(ctx context.Context) error {
	positionDB, err := sql.Open("mysql", w.binlogDSN())
	if err != nil {
		return err
	}
	defer positionDB.Close()

	file, pos, err := currentBinlogPosition(ctx, positionDB)
	if err != nil {
		return err
	}

	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: w.cfg.BinlogServerID,
		Flavor:   "mysql",
		Host:     w.cfg.BinlogHost,
		Port:     w.cfg.BinlogPort,
		User:     w.cfg.BinlogUser,
		Password: w.cfg.BinlogPassword,
	})
	defer syncer.Close()

	streamer, err := syncer.StartSync(mysql.Position{Name: file, Pos: pos})
	if err != nil {
		return err
	}
	log.Printf("binlog watcher started at %s:%d", file, pos)

	for {
		event, err := streamer.GetEvent(ctx)
		if err != nil {
			return err
		}
		rows, ok := event.Event.(*replication.RowsEvent)
		if !ok || !w.isPaperRowsEvent(rows) {
			continue
		}
		w.publishRowsEvent(event.Header.EventType, rows)
	}
}

func (w *ChangeWatcher) isPaperRowsEvent(event *replication.RowsEvent) bool {
	return string(event.Table.Schema) == w.cfg.MySQLDatabase && string(event.Table.Table) == "papers"
}

func (w *ChangeWatcher) publishRowsEvent(eventType replication.EventType, event *replication.RowsEvent) {
	for index, row := range event.Rows {
		if isDeleteRowsEvent(eventType) {
			continue
		}
		if isUpdateRowsEvent(eventType) && index%2 == 0 {
			continue
		}
		paper, ok := paperFromBinlogRow(row)
		if !ok {
			continue
		}
		w.hub.Broadcast(ws.Event{Type: "paper.updated", Payload: paper})
	}
}

func isUpdateRowsEvent(eventType replication.EventType) bool {
	switch eventType {
	case replication.UPDATE_ROWS_EVENTv0,
		replication.UPDATE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv2,
		replication.PARTIAL_UPDATE_ROWS_EVENT,
		replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1:
		return true
	default:
		return false
	}
}

func isDeleteRowsEvent(eventType replication.EventType) bool {
	switch eventType {
	case replication.DELETE_ROWS_EVENTv0,
		replication.DELETE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv2,
		replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
		return true
	default:
		return false
	}
}

func (w *ChangeWatcher) binlogDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=true&loc=Local",
		w.cfg.BinlogUser,
		w.cfg.BinlogPassword,
		w.cfg.BinlogHost,
		w.cfg.BinlogPort,
	)
}

func currentBinlogPosition(ctx context.Context, db *sql.DB) (string, uint32, error) {
	file, pos, err := scanBinlogPosition(ctx, db, "SHOW BINARY LOG STATUS")
	if err == nil {
		return file, pos, nil
	}
	return scanBinlogPosition(ctx, db, "SHOW MASTER STATUS")
}

func scanBinlogPosition(ctx context.Context, db *sql.DB, query string) (string, uint32, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return "", 0, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return "", 0, err
	}
	values := make([]sql.NullString, len(columns))
	dest := make([]any, len(columns))
	for i := range values {
		dest[i] = &values[i]
	}
	if !rows.Next() {
		return "", 0, sql.ErrNoRows
	}
	if err := rows.Scan(dest...); err != nil {
		return "", 0, err
	}
	if len(values) < 2 {
		return "", 0, fmt.Errorf("unexpected binlog status columns: %v", columns)
	}
	var pos uint64
	if _, err := fmt.Sscanf(values[1].String, "%d", &pos); err != nil {
		return "", 0, err
	}
	return values[0].String, uint32(pos), nil
}

func paperFromBinlogRow(row []any) (model.Paper, bool) {
	if len(row) < 10 {
		return model.Paper{}, false
	}
	return model.Paper{
		ID:        asInt64(row[0]),
		SourceID:  asString(row[1]),
		Year:      int(asInt64(row[2])),
		Title:     asString(row[3]),
		Abstract:  asString(row[4]),
		PaperURL:  asString(row[5]),
		PDFURL:    asString(row[6]),
		CreatedAt: asTime(row[8]),
		UpdatedAt: asTime(row[9]),
	}, true
}

func asString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(typed)
	}
}

func asInt64(value any) int64 {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int32:
		return int64(typed)
	case int64:
		return typed
	case uint:
		return int64(typed)
	case uint32:
		return int64(typed)
	case uint64:
		return int64(typed)
	default:
		return 0
	}
}

func asTime(value any) time.Time {
	if typed, ok := value.(time.Time); ok {
		return typed
	}
	return time.Now()
}
