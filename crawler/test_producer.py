import json
import unittest

from producer import (
    extract_keywords,
    normalize_paper,
    parse_detail_page,
    parse_openaccess_listing,
)


LISTING_HTML = """
<html>
  <body>
    <dl>
      <dt class="ptitle"><a href="content/CVPR2024/html/example.html">Open-Vocabulary Scene Understanding</a></dt>
      <dd>Alice Zhang, Bob Wang</dd>
      <dd><a href="content/CVPR2024/papers/example.pdf">pdf</a></dd>
    </dl>
  </body>
</html>
"""

DETAIL_HTML = """
<html>
  <body>
    <div id="abstract">Abstract We present a vision-language foundation model for object detection.</div>
    <a href="../papers/example-detail.pdf">pdf</a>
  </body>
</html>
"""


class ProducerParsingTest(unittest.TestCase):
    def test_parse_openaccess_listing(self):
        papers = list(
            parse_openaccess_listing(
                LISTING_HTML,
                2024,
                "https://openaccess.thecvf.com/CVPR2024?day=all",
                limit=1,
            )
        )

        self.assertEqual(len(papers), 1)
        self.assertEqual(papers[0]["year"], 2024)
        self.assertEqual(papers[0]["title"], "Open-Vocabulary Scene Understanding")
        self.assertEqual(papers[0]["authors"], ["Alice Zhang", "Bob Wang"])
        self.assertTrue(papers[0]["source_id"].startswith("cvpr-2024-open-vocabulary"))
        self.assertEqual(
            papers[0]["paper_url"],
            "https://openaccess.thecvf.com/content/CVPR2024/html/example.html",
        )

    def test_parse_detail_page(self):
        abstract, pdf_url = parse_detail_page(
            DETAIL_HTML,
            "https://openaccess.thecvf.com/content/CVPR2024/html/example.html",
        )

        self.assertEqual(
            abstract,
            "We present a vision-language foundation model for object detection.",
        )
        self.assertEqual(
            pdf_url,
            "https://openaccess.thecvf.com/content/CVPR2024/papers/example-detail.pdf",
        )

    def test_normalize_paper_extracts_keywords(self):
        paper = normalize_paper(
            {
                "year": 2024,
                "title": "Vision-Language Object Detection",
                "authors": ["Alice Zhang", "Alice Zhang"],
                "institutions": [],
                "abstract": "A foundation model for open vocabulary object detection.",
            }
        )

        self.assertEqual(paper["authors"], ["Alice Zhang"])
        self.assertIn("object detection", paper["keywords"])
        self.assertIn("foundation model", paper["keywords"])
        json.dumps(paper)

    def test_extract_keywords_is_deterministic(self):
        first = extract_keywords("Vision language model for semantic segmentation")
        second = extract_keywords("Vision language model for semantic segmentation")

        self.assertEqual(first, second)
        self.assertIn("semantic segmentation", first)


if __name__ == "__main__":
    unittest.main()
