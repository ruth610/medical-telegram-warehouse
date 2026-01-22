"""
YOLO Object Detection Script

This script runs YOLOv8 object detection on downloaded images
and categorizes them based on detected objects.
"""

import os
import csv
import logging
from pathlib import Path
from typing import List, Dict, Optional
from ultralytics import YOLO
import cv2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/yolo_detect.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Directories
IMAGES_DIR = Path("data/raw/images")
OUTPUT_DIR = Path("data/processed")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# YOLO class names we care about
PERSON_CLASS = 0  # person
BOTTLE_CLASS = 39  # bottle
CUP_CLASS = 41  # cup
BOWL_CLASS = 40  # bowl
CONTAINER_CLASSES = [39, 40, 41]  # bottle, bowl, cup


class YOLODetector:
    """YOLO object detector for image analysis"""
    
    def __init__(self, model_path: str = "yolov8n.pt"):
        """
        Initialize YOLO detector
        
        Args:
            model_path: Path to YOLO model file
        """
        logger.info(f"Loading YOLO model: {model_path}")
        self.model = YOLO(model_path)
        logger.info("YOLO model loaded successfully")
        
    def detect_objects(self, image_path: Path) -> List[Dict]:
        """
        Detect objects in an image
        
        Args:
            image_path: Path to the image file
            
        Returns:
            List of detected objects with class and confidence
        """
        try:
            results = self.model(str(image_path))
            detections = []
            
            for result in results:
                boxes = result.boxes
                for box in boxes:
                    class_id = int(box.cls[0])
                    confidence = float(box.conf[0])
                    class_name = self.model.names[class_id]
                    
                    detections.append({
                        "class_id": class_id,
                        "class_name": class_name,
                        "confidence": confidence
                    })
                    
            return detections
            
        except Exception as e:
            logger.error(f"Error detecting objects in {image_path}: {e}")
            return []
            
    def categorize_image(self, detections: List[Dict]) -> str:
        """
        Categorize image based on detected objects
        
        Categories:
        - promotional: Contains person + product (bottle/container)
        - product_display: Contains bottle/container, no person
        - lifestyle: Contains person, no product
        - other: Neither detected
        
        Args:
            detections: List of detected objects
            
        Returns:
            Category string
        """
        has_person = any(d["class_id"] == PERSON_CLASS for d in detections)
        has_product = any(d["class_id"] in CONTAINER_CLASSES for d in detections)
        
        if has_person and has_product:
            return "promotional"
        elif has_product and not has_person:
            return "product_display"
        elif has_person and not has_product:
            return "lifestyle"
        else:
            return "other"
            
    def process_image(self, image_path: Path, message_id: int) -> Optional[Dict]:
        """
        Process a single image and return detection results
        
        Args:
            image_path: Path to the image
            message_id: Message ID associated with the image
            
        Returns:
            Dictionary with detection results or None
        """
        if not image_path.exists():
            logger.warning(f"Image not found: {image_path}")
            return None
            
        detections = self.detect_objects(image_path)
        
        if not detections:
            return {
                "message_id": message_id,
                "image_path": str(image_path),
                "detected_class": None,
                "confidence_score": 0.0,
                "image_category": "other"
            }
        
        # Get the highest confidence detection
        best_detection = max(detections, key=lambda x: x["confidence"])
        category = self.categorize_image(detections)
        
        return {
            "message_id": message_id,
            "image_path": str(image_path),
            "detected_class": best_detection["class_name"],
            "confidence_score": best_detection["confidence"],
            "image_category": category,
            "all_detections": detections
        }
        
    def process_all_images(self) -> List[Dict]:
        """
        Process all images in the images directory
        
        Returns:
            List of detection results
        """
        if not IMAGES_DIR.exists():
            logger.warning(f"Images directory {IMAGES_DIR} does not exist")
            return []
            
        results = []
        image_files = list(IMAGES_DIR.rglob("*.jpg")) + list(IMAGES_DIR.rglob("*.png"))
        logger.info(f"Found {len(image_files)} images to process")
        
        for image_path in image_files:
            # Extract message_id from filename (format: {message_id}.jpg)
            try:
                message_id = int(image_path.stem)
            except ValueError:
                logger.warning(f"Could not extract message_id from {image_path}")
                continue
                
            result = self.process_image(image_path, message_id)
            if result:
                results.append(result)
                logger.info(f"Processed {image_path}: {result['image_category']}")
                
        logger.info(f"Processed {len(results)} images")
        return results
        
    def save_results(self, results: List[Dict], output_file: str = "yolo_detections.csv"):
        """
        Save detection results to CSV
        
        Args:
            results: List of detection results
            output_file: Output CSV filename
        """
        if not results:
            logger.warning("No results to save")
            return
            
        output_path = OUTPUT_DIR / output_file
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                "message_id",
                "image_path",
                "detected_class",
                "confidence_score",
                "image_category"
            ])
            writer.writeheader()
            
            for result in results:
                writer.writerow({
                    "message_id": result["message_id"],
                    "image_path": result["image_path"],
                    "detected_class": result["detected_class"],
                    "confidence_score": result["confidence_score"],
                    "image_category": result["image_category"]
                })
                
        logger.info(f"Saved {len(results)} detection results to {output_path}")


def load_yolo_results_to_postgres(csv_path: Path):
    """
    Load YOLO results CSV into PostgreSQL
    
    Args:
        csv_path: Path to the CSV file
    """
    import psycopg2
    from psycopg2.extras import execute_values
    
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            user=os.getenv("POSTGRES_USER", "warehouse_user"),
            password=os.getenv("POSTGRES_PASSWORD", "warehouse_pass"),
            database=os.getenv("POSTGRES_DB", "medical_warehouse")
        )
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw.yolo_detections (
                detection_id SERIAL PRIMARY KEY,
                message_id BIGINT NOT NULL,
                image_path VARCHAR(500),
                detected_class VARCHAR(100),
                confidence_score FLOAT,
                image_category VARCHAR(50),
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Read CSV and insert data
        import csv
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data = []
            for row in reader:
                data.append((
                    int(row["message_id"]),
                    row["image_path"],
                    row["detected_class"] if row["detected_class"] else None,
                    float(row["confidence_score"]) if row["confidence_score"] else None,
                    row["image_category"]
                ))
        
        if data:
            insert_query = """
            INSERT INTO raw.yolo_detections 
            (message_id, image_path, detected_class, confidence_score, image_category)
            VALUES %s
            ON CONFLICT DO NOTHING;
            """
            execute_values(cursor, insert_query, data)
            conn.commit()
            logger.info(f"Loaded {len(data)} detection results to PostgreSQL")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error loading results to PostgreSQL: {e}")


def main():
    """Main function"""
    detector = YOLODetector()
    results = detector.process_all_images()
    detector.save_results(results)
    
    # Load to PostgreSQL
    csv_path = OUTPUT_DIR / "yolo_detections.csv"
    if csv_path.exists():
        load_yolo_results_to_postgres(csv_path)


if __name__ == "__main__":
    main()
