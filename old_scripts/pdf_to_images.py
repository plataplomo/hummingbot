#!/usr/bin/env python3
"""
PDF to Images Converter

This script converts PDF files to images (one per page) to better preserve and visualize
mathematical formulas and document layout.

Usage:
    python pdf_to_images.py [--source SOURCE_DIR] [--dest DEST_DIR] [--dpi DPI]

The script will process all PDF files in the source directory and save the image files
in the destination directory in organized folders.
"""

import os
import argparse
import fitz  # PyMuPDF
import logging
from pathlib import Path
from PIL import Image
import io
import concurrent.futures
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def convert_page_to_image(pdf_document, page_num, dpi=300):
    """
    Convert a single PDF page to an image.
    
    Args:
        pdf_document: PyMuPDF document
        page_num (int): Page number to convert
        dpi (int): Resolution in dots per inch
    
    Returns:
        PIL.Image: The converted image
    """
    page = pdf_document.load_page(page_num)
    
    # Calculate zoom factor based on desired DPI
    zoom = dpi / 72.0  # PDF uses 72 DPI by default
    
    # Create a matrix for zooming
    matrix = fitz.Matrix(zoom, zoom)
    
    # Render page to pixmap
    pixmap = page.get_pixmap(matrix=matrix, alpha=False)
    
    # Convert pixmap to PIL Image
    img = Image.frombytes("RGB", [pixmap.width, pixmap.height], pixmap.samples)
    
    return img

def convert_pdf_to_images(pdf_path, output_dir, dpi=300):
    """
    Convert a PDF file to a series of images.
    
    Args:
        pdf_path (str): Path to the PDF file
        output_dir (str): Directory to save the images
        dpi (int): Resolution in dots per inch
    
    Returns:
        list: Paths to the created image files
    """
    pdf_filename = os.path.basename(pdf_path)
    pdf_name = os.path.splitext(pdf_filename)[0]
    
    # Create a dedicated directory for this PDF
    pdf_output_dir = os.path.join(output_dir, pdf_name)
    os.makedirs(pdf_output_dir, exist_ok=True)
    
    image_paths = []
    
    try:
        # Open the PDF file
        pdf_document = fitz.open(pdf_path)
        total_pages = len(pdf_document)
        
        logger.info(f"Converting {pdf_filename} ({total_pages} pages) to images at {dpi} DPI")
        
        # Process each page
        for page_num in range(total_pages):
            try:
                # Convert page to image
                img = convert_page_to_image(pdf_document, page_num, dpi)
                
                # Save the image
                image_path = os.path.join(pdf_output_dir, f"page_{page_num+1:03d}.png")
                img.save(image_path, "PNG")
                image_paths.append(image_path)
                
                # Log progress for every 5 pages or last page
                if (page_num + 1) % 5 == 0 or page_num + 1 == total_pages:
                    logger.info(f"Converted page {page_num+1}/{total_pages} of {pdf_filename}")
                
            except Exception as e:
                logger.error(f"Error converting page {page_num+1} of {pdf_filename}: {e}")
        
        # Create a contact sheet of first 5 pages for quick preview
        if total_pages > 0:
            # Calculate dimensions for the contact sheet
            preview_pages = min(5, total_pages)
            max_width = 0
            max_height = 0
            preview_images = []
            
            for page_num in range(preview_pages):
                img = convert_page_to_image(pdf_document, page_num, dpi=150)  # Lower DPI for preview
                preview_images.append(img)
                max_width = max(max_width, img.width)
                max_height = max(max_height, img.height)
            
            # Create contact sheet
            contact_sheet = Image.new('RGB', (max_width, max_height * preview_pages))
            
            # Paste images into contact sheet
            for i, img in enumerate(preview_images):
                contact_sheet.paste(img, (0, i * max_height))
            
            # Save contact sheet
            contact_sheet_path = os.path.join(output_dir, f"{pdf_name}_preview.jpg")
            contact_sheet.save(contact_sheet_path, "JPEG", quality=85)
            logger.info(f"Created preview contact sheet: {contact_sheet_path}")
        
    except Exception as e:
        logger.error(f"Error processing {pdf_filename}: {e}")
    
    return image_paths

def process_pdfs_in_parallel(pdf_files, output_dir, dpi, max_workers=4):
    """
    Process multiple PDF files in parallel.
    
    Args:
        pdf_files (list): List of PDF file paths
        output_dir (str): Directory to save the images
        dpi (int): Resolution in dots per inch
        max_workers (int): Maximum number of worker processes
    """
    # Adjust max_workers based on available CPU cores
    max_workers = min(max_workers, os.cpu_count() or 1)
    logger.info(f"Processing {len(pdf_files)} PDF files with {max_workers} worker processes")
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(convert_pdf_to_images, str(pdf_path), output_dir, dpi)
            for pdf_path in pdf_files
        ]
        
        # Wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"An error occurred during parallel processing: {e}")

def create_index_html(output_dir):
    """
    Create an HTML index file that lists all PDFs and their preview images.
    
    Args:
        output_dir (str): Directory with the converted images
    """
    index_path = os.path.join(output_dir, "index.html")
    preview_files = list(Path(output_dir).glob("*_preview.jpg"))
    
    html_content = [
        "<!DOCTYPE html>",
        "<html>",
        "<head>",
        "    <title>PDF Previews - CyberDeltaEngine</title>",
        "    <style>",
        "        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }",
        "        h1 { color: #333; }",
        "        .preview-container { display: flex; flex-wrap: wrap; }",
        "        .preview-card { margin: 10px; background-color: white; box-shadow: 0 2px 5px rgba(0,0,0,0.1); padding: 15px; border-radius: 5px; }",
        "        .preview-card h2 { margin-top: 0; font-size: 1.2em; }",
        "        .preview-card img { max-width: 200px; max-height: 300px; display: block; margin-bottom: 10px; }",
        "        .preview-card a { display: inline-block; margin-top: 10px; color: #0066cc; text-decoration: none; }",
        "        .preview-card a:hover { text-decoration: underline; }",
        "    </style>",
        "</head>",
        "<body>",
        "    <h1>PDF Previews</h1>",
        "    <div class='preview-container'>"
    ]
    
    for preview_file in sorted(preview_files):
        pdf_name = os.path.basename(preview_file).replace("_preview.jpg", "")
        folder_path = os.path.join(output_dir, pdf_name)
        
        if os.path.isdir(folder_path):
            html_content.extend([
                f"        <div class='preview-card'>",
                f"            <h2>{pdf_name}</h2>",
                f"            <a href='{pdf_name}/'><img src='{os.path.basename(preview_file)}' alt='{pdf_name} preview'></a>",
                f"            <a href='{pdf_name}/'>View all pages</a>",
                f"        </div>"
            ])
    
    html_content.extend([
        "    </div>",
        "</body>",
        "</html>"
    ])
    
    with open(index_path, "w") as f:
        f.write("\n".join(html_content))
    
    logger.info(f"Created index HTML file: {index_path}")

def create_pdf_folder_index(pdf_folder):
    """
    Create an HTML index file for a specific PDF folder.
    
    Args:
        pdf_folder (str): Path to the folder with page images
    """
    folder_name = os.path.basename(pdf_folder)
    index_path = os.path.join(pdf_folder, "index.html")
    image_files = sorted(list(Path(pdf_folder).glob("*.png")))
    
    html_content = [
        "<!DOCTYPE html>",
        "<html>",
        "<head>",
        f"    <title>{folder_name} - Page Images</title>",
        "    <style>",
        "        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }",
        "        h1 { color: #333; }",
        "        .image-container { display: flex; flex-direction: column; align-items: center; }",
        "        .page-image { margin: 20px 0; background-color: white; box-shadow: 0 2px 5px rgba(0,0,0,0.1); padding: 15px; border-radius: 5px; }",
        "        .page-image img { max-width: 100%; height: auto; }",
        "        .page-image p { margin: 10px 0 0 0; font-weight: bold; }",
        "        .navigation { position: fixed; bottom: 20px; right: 20px; background-color: white; padding: 10px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.2); }",
        "        .navigation a { margin: 0 5px; }",
        "    </style>",
        "</head>",
        "<body>",
        f"    <h1>{folder_name}</h1>",
        "    <a href='../index.html'>Back to index</a>",
        "    <div class='image-container'>"
    ]
    
    for image_file in image_files:
        page_num = os.path.basename(image_file).replace("page_", "").replace(".png", "")
        html_content.extend([
            f"        <div class='page-image' id='page{page_num}'>",
            f"            <p>Page {page_num}</p>",
            f"            <img src='{os.path.basename(image_file)}' alt='Page {page_num}'>",
            f"        </div>"
        ])
    
    html_content.extend([
        "    </div>",
        "    <div class='navigation'>",
        "        <a href='#top'>Top</a>",
    ])
    
    # Add quick navigation links for every 5 pages
    for i in range(1, len(image_files) + 1, 5):
        page_num = f"{i:03d}"
        html_content.append(f"        <a href='#page{page_num}'>Page {i}</a>")
    
    html_content.extend([
        "    </div>",
        "</body>",
        "</html>"
    ])
    
    with open(index_path, "w") as f:
        f.write("\n".join(html_content))
    
    logger.info(f"Created folder index HTML file: {index_path}")

def main():
    """Main function to parse arguments and convert PDFs to images."""
    parser = argparse.ArgumentParser(description='Convert PDF files to images.')
    parser.add_argument('--source', 
                        default='/home/demute/code/CyberDeltaEngine/.ai_workflow/study',
                        help='Source directory containing PDF files')
    parser.add_argument('--dest', 
                        default='/home/demute/code/CyberDeltaEngine/.ai_workflow/study/images',
                        help='Destination directory for images')
    parser.add_argument('--dpi', type=int, default=300,
                        help='Resolution in dots per inch (default: 300)')
    
    args = parser.parse_args()
    
    # Check dependencies
    try:
        import fitz
    except ImportError:
        logger.error("PyMuPDF (fitz) is not installed. Please install it with: pip install PyMuPDF")
        sys.exit(1)
    
    try:
        from PIL import Image
    except ImportError:
        logger.error("Pillow is not installed. Please install it with: pip install Pillow")
        sys.exit(1)
    
    # Ensure destination directory exists
    os.makedirs(args.dest, exist_ok=True)
    
    # Find all PDF files in the source directory
    pdf_files = list(Path(args.source).glob('*.pdf'))
    
    if not pdf_files:
        logger.warning(f"No PDF files found in {args.source}")
        return
    
    logger.info(f"Found {len(pdf_files)} PDF files to convert")
    
    # Convert PDFs to images
    process_pdfs_in_parallel(pdf_files, args.dest, args.dpi)
    
    # Create index HTML files
    create_index_html(args.dest)
    
    # Create individual folder index files
    for pdf_file in pdf_files:
        pdf_name = os.path.splitext(os.path.basename(pdf_file))[0]
        pdf_folder = os.path.join(args.dest, pdf_name)
        if os.path.isdir(pdf_folder):
            create_pdf_folder_index(pdf_folder)
    
    logger.info("Conversion completed!")
    logger.info(f"Open {os.path.join(args.dest, 'index.html')} in a web browser to view the results")

if __name__ == '__main__':
    main() 