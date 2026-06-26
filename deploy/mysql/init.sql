CREATE TABLE IF NOT EXISTS papers (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  source_id VARCHAR(128) NOT NULL UNIQUE,
  year INT NOT NULL,
  title VARCHAR(512) NOT NULL,
  abstract TEXT,
  paper_url VARCHAR(1024),
  pdf_url VARCHAR(1024),
  crawled_at TIMESTAMP NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FULLTEXT KEY ft_papers_title_abstract (title, abstract),
  KEY idx_papers_year (year),
  KEY idx_papers_updated_at (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS paper_authors (
  paper_source_id VARCHAR(128) NOT NULL,
  author_name VARCHAR(255) NOT NULL,
  PRIMARY KEY (paper_source_id, author_name),
  KEY idx_author_name (author_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS paper_institutions (
  paper_source_id VARCHAR(128) NOT NULL,
  institution_name VARCHAR(255) NOT NULL,
  PRIMARY KEY (paper_source_id, institution_name),
  KEY idx_institution_name (institution_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS paper_keywords (
  paper_source_id VARCHAR(128) NOT NULL,
  keyword VARCHAR(128) NOT NULL,
  PRIMARY KEY (paper_source_id, keyword),
  KEY idx_keyword (keyword)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS paper_ingest_events (
  paper_source_id VARCHAR(128) PRIMARY KEY,
  first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stats_yearly (
  year INT PRIMARY KEY,
  paper_count BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stats_keywords (
  keyword VARCHAR(128) PRIMARY KEY,
  paper_count BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stats_authors (
  author_name VARCHAR(255) PRIMARY KEY,
  paper_count BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stats_institutions (
  institution_name VARCHAR(255) PRIMARY KEY,
  paper_count BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS 'replicator'@'%' IDENTIFIED BY 'replicator_pass';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'replicator'@'%';
FLUSH PRIVILEGES;
