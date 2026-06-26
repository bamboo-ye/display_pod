.PHONY: up down logs sample crawl-sample crawl-all watch-openaccess crawler-test rebuild-stats demo-db seed-demo dev-backend dev-frontend

up:
	docker compose -f docker-compose.yml -f docker-compose.local.yml up --build

down:
	docker compose -f docker-compose.yml -f docker-compose.local.yml down

logs:
	docker compose -f docker-compose.yml -f docker-compose.local.yml logs -f

sample:
	docker compose -f docker-compose.yml -f docker-compose.local.yml --profile tools run --rm crawler

crawl-sample:
	docker compose -f docker-compose.yml -f docker-compose.local.yml --profile tools run --rm crawler --years 2024 --limit-per-year 2 --dry-run

crawl-all:
	docker compose -f docker-compose.yml -f docker-compose.local.yml --profile tools run --rm crawler --years 2021,2022,2023,2024,2025,2026 --year-workers 6

watch-openaccess:
	docker compose -f docker-compose.yml -f docker-compose.local.yml --profile crawler up -d --build crawler-watch

crawler-test:
	cd crawler && python -m unittest discover -p 'test_*.py'

rebuild-stats:
	docker compose -f docker-compose.yml -f docker-compose.local.yml exec -T mysql mysql -uroot -proot_pass cvpr_display < deploy/mysql/rebuild_stats.sql

demo-db:
	docker compose -f docker-compose.yml -f docker-compose.local.yml up -d mysql

seed-demo:
	docker compose -f docker-compose.yml -f docker-compose.local.yml exec -T mysql mysql -uroot -proot_pass cvpr_display < deploy/mysql/seed_demo.sql

dev-backend:
	cd backend && set -a && . ../.env && set +a && HTTP_ADDR=':8080' FRONTEND_ORIGIN='http://localhost:5173' go run ./cmd/server

dev-frontend:
	cd frontend && npm install && npm run dev
