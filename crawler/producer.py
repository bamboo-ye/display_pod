import argparse
import hashlib
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Iterator

import requests
from bs4 import BeautifulSoup

try:
    from confluent_kafka import Producer
except ImportError:
    Producer = None


CVF_BASE_URL = "https://openaccess.thecvf.com/"

STOPWORDS = {
    "about",
    "after",
    "again",
    "and",
    "based",
    "between",
    "for",
    "from",
    "into",
    "our",
    "learning",
    "method",
    "model",
    "models",
    "paper",
    "using",
    "with",
    "without",
    "this",
    "that",
    "their",
    "there",
    "these",
    "the",
    "those",
    "through",
    "towards",
    "toward",
    "where",
    "which",
}

PHRASES = [
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
]

SAMPLE_PAPERS = [
    {
        "year": 2024,
        "title": "Vision Foundation Models for Open-Vocabulary Scene Understanding",
        "authors": ["Alice Zhang", "Bob Wang"],
        "institutions": ["Example University", "Display Lab"],
        "abstract": "We study open-vocabulary scene understanding with large vision foundation models.",
        "keywords": ["foundation model", "open vocabulary", "scene understanding"],
        "paper_url": "https://openaccess.thecvf.com/",
        "pdf_url": "https://openaccess.thecvf.com/",
    },
    {
        "year": 2023,
        "title": "Efficient Video Representation Learning with Temporal Prompts",
        "authors": ["Chen Li", "Dana Kim"],
        "institutions": ["Institute of AI"],
        "abstract": "This paper presents a temporal prompt strategy for efficient video representation learning.",
        "keywords": ["video", "representation learning", "prompt learning"],
        "paper_url": "https://openaccess.thecvf.com/",
        "pdf_url": "https://openaccess.thecvf.com/",
    },
]


def stable_source_id(year: int, title: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:80]
    digest = hashlib.sha1(f"{year}:{title}".encode("utf-8")).hexdigest()[:10]
    return f"cvpr-{year}-{slug}-{digest}"


def normalize_text(value: str) -> str:
    return " ".join(str(value or "").split())


def normalize_paper(paper: dict) -> dict:
    paper = dict(paper)
    paper["year"] = int(paper["year"])
    paper["title"] = normalize_text(paper.get("title", ""))
    paper["abstract"] = normalize_text(paper.get("abstract", ""))
    paper["authors"] = unique_clean(paper.get("authors", []))
    paper["institutions"] = unique_clean(paper.get("institutions", []))
    keywords = unique_clean([x.lower() for x in paper.get("keywords", [])])
    if not keywords:
        keywords = extract_keywords(f"{paper['title']} {paper['abstract']}")
    paper["keywords"] = keywords
    paper["source_id"] = paper.get("source_id") or stable_source_id(paper["year"], paper["title"])
    paper["paper_url"] = paper.get("paper_url", "")
    paper["pdf_url"] = paper.get("pdf_url", "")
    paper["crawled_at"] = paper.get("crawled_at") or datetime.now(timezone.utc).isoformat()
    return paper


def unique_clean(values: Iterable[str]) -> list[str]:
    seen = set()
    cleaned = []
    for value in values or []:
        value = normalize_text(value)
        if value and value.lower() not in seen:
            seen.add(value.lower())
            cleaned.append(value)
    return cleaned


def extract_keywords(text: str, limit: int = 8) -> list[str]:
    normalized = re.sub(r"[-_/]+", " ", text.lower())
    normalized = re.sub(r"[^a-z0-9\s]+", " ", normalized)

    scored: dict[str, int] = {}
    for phrase in PHRASES:
        if phrase in normalized:
            scored[phrase] = scored.get(phrase, 0) + 4

    for token in re.findall(r"[a-z][a-z0-9]{2,}", normalized):
        if token in STOPWORDS:
            continue
        scored[token] = scored.get(token, 0) + 1

    return [
        keyword
        for keyword, _ in sorted(scored.items(), key=lambda item: (-item[1], item[0]))[:limit]
    ]


def sample_papers() -> Iterator[dict]:
    for paper in SAMPLE_PAPERS:
        yield normalize_paper(paper)


def parse_authors(value: str) -> list[str]:
    value = normalize_text(value)
    value = re.sub(r"\s+and\s+", ", ", value)
    return unique_clean(value.split(","))


def parse_openaccess_listing(
    html: str,
    year: int,
    base_url: str,
    detail_loader: Callable[[str], str] | None = None,
    limit: int = 0,
) -> Iterator[dict]:
    soup = BeautifulSoup(html, "html.parser")
    count = 0
    for title_node in soup.select("dt.ptitle a"):
        title = title_node.get_text(" ", strip=True)
        if not title:
            continue

        detail_url = requests.compat.urljoin(base_url, title_node.get("href", ""))
        container = title_node.find_parent("dt")
        authors_node = container.find_next_sibling("dd") if container else None
        authors = parse_authors(authors_node.get_text(" ", strip=True)) if authors_node else []

        abstract = ""
        pdf_url = find_pdf_url(container, base_url)
        if detail_loader:
            try:
                detail_html = detail_loader(detail_url)
                abstract, detail_pdf_url = parse_detail_page(detail_html, detail_url)
                pdf_url = pdf_url or detail_pdf_url
            except Exception as error:
                print(f"detail_failed url={detail_url} error={error}", file=sys.stderr)

        yield normalize_paper(
            {
                "year": year,
                "title": title,
                "authors": authors,
                "institutions": [],
                "abstract": abstract,
                "keywords": [],
                "paper_url": detail_url,
                "pdf_url": pdf_url,
            }
        )
        count += 1
        if limit and count >= limit:
            return


def find_pdf_url(anchor_context, base_url: str) -> str:
    if not anchor_context:
        return ""
    for link in anchor_context.find_all_next("a", limit=8):
        href = link.get("href", "")
        text = link.get_text(" ", strip=True).lower()
        if href.lower().endswith(".pdf") or text == "pdf":
            return requests.compat.urljoin(base_url, href)
        if link.find_parent("dt") and link.find_parent("dt") is not anchor_context:
            break
    return ""


def parse_detail_page(html: str, page_url: str) -> tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    abstract = ""
    abstract_node = soup.select_one("#abstract")
    if abstract_node:
        abstract = abstract_node.get_text(" ", strip=True)
        abstract = re.sub(r"^abstract\s*", "", abstract, flags=re.I)

    pdf_url = ""
    for link in soup.select("a[href]"):
        href = link.get("href", "")
        text = link.get_text(" ", strip=True).lower()
        if href.lower().endswith(".pdf") or text == "pdf":
            pdf_url = requests.compat.urljoin(page_url, href)
            break
    return normalize_text(abstract), pdf_url


def scrape_cvpr_openaccess(
    year: int,
    fetch_details: bool = False,
    limit: int = 0,
    delay: float = 0.2,
    detail_workers: int = 4,
) -> Iterator[dict]:
    listing_url = f"{CVF_BASE_URL}CVPR{year}?day=all"
    session = requests.Session()
    response = session.get(listing_url, timeout=30)
    if response.status_code == 404:
        print(f"year_unavailable year={year} url={listing_url}", file=sys.stderr)
        return
    response.raise_for_status()

    def load_detail(url: str) -> str:
        time.sleep(delay)
        last_error = None
        for attempt in range(2):
            try:
                detail_response = session.get(url, timeout=20)
                detail_response.raise_for_status()
                return detail_response.text
            except requests.RequestException as error:
                last_error = error
                time.sleep(1 + attempt)
        raise last_error

    papers = list(parse_openaccess_listing(
        response.text,
        year,
        listing_url,
        detail_loader=None,
        limit=limit,
    ))
    if not fetch_details:
        yield from papers
        return

    def enrich_detail(index_and_paper: tuple[int, dict]) -> tuple[int, dict]:
        index, paper = index_and_paper
        try:
            detail_html = load_detail(paper["paper_url"])
            abstract, detail_pdf_url = parse_detail_page(detail_html, paper["paper_url"])
            enriched = dict(paper)
            enriched["abstract"] = abstract
            enriched["pdf_url"] = enriched.get("pdf_url") or detail_pdf_url
            enriched["keywords"] = extract_keywords(f"{enriched['title']} {abstract}")
            return index, normalize_paper(enriched)
        except Exception as error:
            print(f"detail_failed url={paper['paper_url']} error={error}", file=sys.stderr)
            return index, paper

    max_workers = max(1, detail_workers)
    ordered = list(papers)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(enrich_detail, item) for item in enumerate(papers)]
        for future in as_completed(futures):
            index, paper = future.result()
            ordered[index] = paper
    yield from ordered


def load_json_papers(path: Path) -> Iterator[dict]:
    text = path.read_text(encoding="utf-8")
    stripped = text.lstrip()
    if stripped.startswith("["):
        data = json.loads(text)
        for paper in data:
            yield normalize_paper(paper)
        return

    for line in text.splitlines():
        if line.strip():
            yield normalize_paper(json.loads(line))


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"delivery failed: {err}", file=sys.stderr)
    else:
        print(f"sent {msg.topic()}[{msg.partition()}] offset={msg.offset()}")


def publish(
    papers: Iterable[dict],
    brokers: str,
    topic: str,
    dry_run: bool = False,
    pretty: bool = False,
    publish_delay: float = 0.0,
    flush_every: int = 100,
) -> int:
    if dry_run:
        count = 0
        for paper in papers:
            print(json.dumps(paper, ensure_ascii=False, indent=2 if pretty else None))
            count += 1
        return count

    if Producer is None:
        raise RuntimeError("confluent-kafka is not installed; use --dry-run or install requirements.txt")

    producer = Producer({"bootstrap.servers": brokers})
    count = 0
    for paper in papers:
        payload = json.dumps(paper, ensure_ascii=False).encode("utf-8")
        producer.produce(topic, key=paper["source_id"], value=payload, callback=delivery_report)
        producer.poll(0)
        count += 1
        if flush_every and count % flush_every == 0:
            producer.flush()
        if publish_delay:
            time.sleep(publish_delay)
    producer.flush()
    return count


def scrape_year_safely(year: int, args: argparse.Namespace) -> list[dict]:
    try:
        papers = list(scrape_cvpr_openaccess(
            year,
            fetch_details=args.fetch_details,
            limit=args.limit_per_year,
            delay=args.delay,
            detail_workers=args.detail_workers,
        ))
        print(f"year_done year={year} papers={len(papers)}", file=sys.stderr)
        return papers
    except requests.RequestException as error:
        print(f"year_failed year={year} error={error}", file=sys.stderr)
        return []


def parallel_year_stream(args: argparse.Namespace) -> Iterator[dict]:
    max_workers = max(1, min(args.year_workers, len(args.years) or 1))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(scrape_year_safely, year, args) for year in args.years]
        for future in as_completed(futures):
            yield from future.result()


def load_seen_ids(path: str) -> set[str]:
    if not path:
        return set()
    state_path = Path(path)
    if not state_path.exists():
        return set()
    return {line.strip() for line in state_path.read_text(encoding="utf-8").splitlines() if line.strip()}


def append_seen_id(path: str, source_id: str) -> None:
    if not path:
        return
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with state_path.open("a", encoding="utf-8") as output:
        output.write(f"{source_id}\n")


def watch_paper_stream(args: argparse.Namespace) -> Iterator[dict]:
    seen = load_seen_ids(args.state_file)
    cycle = 0
    while True:
        cycle += 1
        published = 0
        print(f"watch_cycle_start cycle={cycle} years={args.years} seen={len(seen)}", file=sys.stderr)
        for paper in parallel_year_stream(args):
            source_id = paper["source_id"]
            if source_id in seen:
                continue
            seen.add(source_id)
            append_seen_id(args.state_file, source_id)
            published += 1
            yield paper
        print(f"watch_cycle_done cycle={cycle} new={published} seen={len(seen)}", file=sys.stderr)
        time.sleep(args.watch_interval)


def build_paper_stream(args: argparse.Namespace) -> Iterable[dict]:
    if args.sample:
        return sample_papers()
    if args.input_json:
        return load_json_papers(Path(args.input_json))
    if args.watch:
        return watch_paper_stream(args)
    return parallel_year_stream(args)


def main() -> None:
    parser = argparse.ArgumentParser(description="CVPR paper Kafka producer")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--sample", action="store_true", help="publish bundled demo papers")
    source.add_argument("--input-json", help="load papers from a JSON array or JSONL file")
    parser.add_argument("--years", nargs="*", type=int, default=[2024, 2023, 2022])
    parser.add_argument("--fetch-details", action="store_true", help="fetch each paper page for abstract and PDF URL")
    parser.add_argument("--limit-per-year", type=int, default=0, help="limit papers per year, 0 means no limit")
    parser.add_argument("--delay", type=float, default=0.2, help="delay between detail page requests")
    parser.add_argument("--year-workers", type=int, default=4, help="parallel year listing workers")
    parser.add_argument("--detail-workers", type=int, default=4, help="parallel detail page workers per year")
    parser.add_argument("--publish-delay", type=float, default=0.0, help="delay between Kafka messages")
    parser.add_argument("--watch", action="store_true", help="keep polling OpenAccess and publish newly discovered papers")
    parser.add_argument("--watch-interval", type=float, default=3600, help="seconds between watch polling cycles")
    parser.add_argument("--state-file", default="", help="optional newline source_id state file for watch mode")
    parser.add_argument("--dry-run", action="store_true", help="print JSON instead of sending to Kafka")
    parser.add_argument("--pretty", action="store_true", help="pretty-print JSON for dry-run")
    args = parser.parse_args()

    brokers = os.getenv("KAFKA_BROKERS", "localhost:29092")
    topic = os.getenv("KAFKA_TOPIC", "cvpr.raw.papers")
    count = publish(
        build_paper_stream(args),
        brokers,
        topic,
        dry_run=args.dry_run,
        pretty=args.pretty,
        publish_delay=args.publish_delay,
    )
    print(f"processed={count}")


if __name__ == "__main__":
    main()
