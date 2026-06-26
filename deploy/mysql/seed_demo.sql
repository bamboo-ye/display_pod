INSERT INTO papers (source_id, year, title, abstract, paper_url, pdf_url, crawled_at) VALUES
('cvpr-2024-vision-foundation-open-vocabulary-demo', 2024, 'Vision Foundation Models for Open-Vocabulary Scene Understanding', 'We study open-vocabulary scene understanding with large vision foundation models and evaluate transfer across detection and segmentation tasks.', 'https://openaccess.thecvf.com/', 'https://openaccess.thecvf.com/', UTC_TIMESTAMP()),
('cvpr-2024-diffusion-video-generation-demo', 2024, 'Controllable Diffusion Models for Long-Form Video Generation', 'A controllable diffusion model is introduced for long-form video generation with temporal consistency and text-guided editing.', 'https://openaccess.thecvf.com/', 'https://openaccess.thecvf.com/', UTC_TIMESTAMP()),
('cvpr-2023-efficient-video-prompt-demo', 2023, 'Efficient Video Representation Learning with Temporal Prompts', 'This paper presents a temporal prompt strategy for efficient video representation learning under limited annotation budgets.', 'https://openaccess.thecvf.com/', 'https://openaccess.thecvf.com/', UTC_TIMESTAMP()),
('cvpr-2023-3d-detection-autonomous-demo', 2023, 'Robust 3D Object Detection for Autonomous Driving', 'We improve 3D object detection robustness with multi-sensor fusion and uncertainty-aware proposal refinement.', 'https://openaccess.thecvf.com/', 'https://openaccess.thecvf.com/', UTC_TIMESTAMP()),
('cvpr-2022-self-supervised-segmentation-demo', 2022, 'Self-Supervised Semantic Segmentation with Masked Visual Tokens', 'Masked visual token learning enables semantic segmentation models to learn dense visual representations without category-level annotation.', 'https://openaccess.thecvf.com/', 'https://openaccess.thecvf.com/', UTC_TIMESTAMP()),
('cvpr-2022-medical-image-foundation-demo', 2022, 'Medical Image Foundation Models with Cross-Domain Adaptation', 'A foundation model for medical image analysis is adapted across modalities with contrastive learning and domain adaptation.', 'https://openaccess.thecvf.com/', 'https://openaccess.thecvf.com/', UTC_TIMESTAMP()),
('cvpr-2021-pose-estimation-demo', 2021, 'Human Pose Estimation via Graph-Aware Visual Transformers', 'Graph-aware visual transformers improve human pose estimation by modeling long-range keypoint interactions.', 'https://openaccess.thecvf.com/', 'https://openaccess.thecvf.com/', UTC_TIMESTAMP()),
('cvpr-2021-few-shot-detection-demo', 2021, 'Few-Shot Object Detection with Semantic Calibration', 'Few-shot object detection benefits from semantic calibration between base and novel categories.', 'https://openaccess.thecvf.com/', 'https://openaccess.thecvf.com/', UTC_TIMESTAMP())
ON DUPLICATE KEY UPDATE
  year=VALUES(year),
  title=VALUES(title),
  abstract=VALUES(abstract),
  paper_url=VALUES(paper_url),
  pdf_url=VALUES(pdf_url),
  crawled_at=VALUES(crawled_at);

INSERT IGNORE INTO paper_authors (paper_source_id, author_name) VALUES
('cvpr-2024-vision-foundation-open-vocabulary-demo', 'Alice Zhang'),
('cvpr-2024-vision-foundation-open-vocabulary-demo', 'Bob Wang'),
('cvpr-2024-diffusion-video-generation-demo', 'Chen Li'),
('cvpr-2024-diffusion-video-generation-demo', 'Dana Kim'),
('cvpr-2023-efficient-video-prompt-demo', 'Evan Zhou'),
('cvpr-2023-efficient-video-prompt-demo', 'Fiona Chen'),
('cvpr-2023-3d-detection-autonomous-demo', 'Grace Liu'),
('cvpr-2023-3d-detection-autonomous-demo', 'Hao Sun'),
('cvpr-2022-self-supervised-segmentation-demo', 'Iris Huang'),
('cvpr-2022-self-supervised-segmentation-demo', 'Jie Wu'),
('cvpr-2022-medical-image-foundation-demo', 'Kai Zhao'),
('cvpr-2022-medical-image-foundation-demo', 'Lina Xu'),
('cvpr-2021-pose-estimation-demo', 'Ming Tang'),
('cvpr-2021-pose-estimation-demo', 'Nora Yu'),
('cvpr-2021-few-shot-detection-demo', 'Oscar Lin'),
('cvpr-2021-few-shot-detection-demo', 'Priya Rao');

INSERT IGNORE INTO paper_institutions (paper_source_id, institution_name) VALUES
('cvpr-2024-vision-foundation-open-vocabulary-demo', 'Example University'),
('cvpr-2024-vision-foundation-open-vocabulary-demo', 'Display Lab'),
('cvpr-2024-diffusion-video-generation-demo', 'Institute of AI'),
('cvpr-2023-efficient-video-prompt-demo', 'Vision Systems Lab'),
('cvpr-2023-3d-detection-autonomous-demo', 'Autonomous Intelligence Center'),
('cvpr-2022-self-supervised-segmentation-demo', 'Machine Perception Lab'),
('cvpr-2022-medical-image-foundation-demo', 'Medical AI Institute'),
('cvpr-2021-pose-estimation-demo', 'Human-Centric Vision Lab'),
('cvpr-2021-few-shot-detection-demo', 'Robust Learning Group');

INSERT IGNORE INTO paper_keywords (paper_source_id, keyword) VALUES
('cvpr-2024-vision-foundation-open-vocabulary-demo', 'foundation model'),
('cvpr-2024-vision-foundation-open-vocabulary-demo', 'open vocabulary'),
('cvpr-2024-vision-foundation-open-vocabulary-demo', 'scene understanding'),
('cvpr-2024-vision-foundation-open-vocabulary-demo', 'semantic segmentation'),
('cvpr-2024-diffusion-video-generation-demo', 'diffusion model'),
('cvpr-2024-diffusion-video-generation-demo', 'video understanding'),
('cvpr-2024-diffusion-video-generation-demo', 'image generation'),
('cvpr-2023-efficient-video-prompt-demo', 'video understanding'),
('cvpr-2023-efficient-video-prompt-demo', 'representation learning'),
('cvpr-2023-efficient-video-prompt-demo', 'prompt learning'),
('cvpr-2023-3d-detection-autonomous-demo', '3d detection'),
('cvpr-2023-3d-detection-autonomous-demo', 'object detection'),
('cvpr-2023-3d-detection-autonomous-demo', 'autonomous driving'),
('cvpr-2022-self-supervised-segmentation-demo', 'self supervised'),
('cvpr-2022-self-supervised-segmentation-demo', 'semantic segmentation'),
('cvpr-2022-self-supervised-segmentation-demo', 'representation learning'),
('cvpr-2022-medical-image-foundation-demo', 'medical image'),
('cvpr-2022-medical-image-foundation-demo', 'foundation model'),
('cvpr-2022-medical-image-foundation-demo', 'domain adaptation'),
('cvpr-2021-pose-estimation-demo', 'pose estimation'),
('cvpr-2021-pose-estimation-demo', 'visual transformer'),
('cvpr-2021-few-shot-detection-demo', 'few shot'),
('cvpr-2021-few-shot-detection-demo', 'object detection');

TRUNCATE TABLE paper_ingest_events;
INSERT INTO paper_ingest_events (paper_source_id)
SELECT source_id FROM papers;

TRUNCATE TABLE stats_yearly;
TRUNCATE TABLE stats_keywords;
TRUNCATE TABLE stats_authors;
TRUNCATE TABLE stats_institutions;

INSERT INTO stats_yearly (year, paper_count)
SELECT year, COUNT(*)
FROM papers
GROUP BY year;

INSERT INTO stats_keywords (keyword, paper_count)
SELECT keyword, COUNT(*)
FROM paper_keywords
GROUP BY keyword;

INSERT INTO stats_authors (author_name, paper_count)
SELECT author_name, COUNT(*)
FROM paper_authors
GROUP BY author_name;

INSERT INTO stats_institutions (institution_name, paper_count)
SELECT institution_name, COUNT(*)
FROM paper_institutions
GROUP BY institution_name;
