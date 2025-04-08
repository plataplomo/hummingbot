#!/usr/bin/env python3
"""
PDF to Markdown Converter

This script converts PDF files to Markdown format, preserving as much structure as possible.
It uses the PyPDF2 library to extract text content from PDFs and formats it as Markdown.

Usage:
    python pdf_to_md_converter.py

The script will process all PDF files in the source directory and save the Markdown files
in the destination directory with the same filename but with a .md extension.
"""

import os
import re
import subprocess
import argparse
from pathlib import Path


def extract_text_from_pdf(pdf_path, start_page=None, end_page=None):
    """
    Extract text from a PDF file using pdftotext command line tool.
    
    Args:
        pdf_path (str): Path to the PDF file
        start_page (int, optional): First page to extract
        end_page (int, optional): Last page to extract
    
    Returns:
        str: Extracted text from the PDF
    """
    cmd = ['pdftotext']
    
    # Add page range options if specified
    if start_page is not None:
        cmd.extend(['-f', str(start_page)])
    if end_page is not None:
        cmd.extend(['-l', str(end_page)])
    
    # Add layout preservation option
    cmd.append('-layout')
    
    # Add input and output files
    cmd.extend([pdf_path, '-'])
    
    try:
        # Run the command and capture the output
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error extracting text from {pdf_path}: {e}")
        return ""


def clean_text(text):
    """
    Clean and format the extracted text.
    
    Args:
        text (str): Raw text extracted from PDF
    
    Returns:
        str: Cleaned and formatted text
    """
    # Replace multiple newlines with a single newline
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    # Fix common formatting issues
    text = re.sub(r'\f', '\n\n---\n\n', text)  # Form feeds as section separators
    
    # Handle headers (lines that are likely to be headers)
    lines = text.split('\n')
    processed_lines = []
    
    for i, line in enumerate(lines):
        line = line.rstrip()
        
        # Skip empty lines
        if not line.strip():
            processed_lines.append('')
            continue
            
        # Detect potential headers (short lines, all caps, ending with numbers)
        if len(line) < 50 and (line.isupper() or re.match(r'^[0-9]+\.\s+', line)):
            # Determine header level based on indentation or numbering
            if re.match(r'^[0-9]+\.\s+', line):
                header_level = len(re.match(r'^([0-9]+\.)+\s+', line).group(0).split('.')) if re.match(r'^([0-9]+\.)+\s+', line) else 1
                line = re.sub(r'^[0-9]+\.\s+', '', line)
                processed_lines.append('#' * header_level + ' ' + line)
            else:
                processed_lines.append('## ' + line)
        else:
            processed_lines.append(line)
    
    return '\n'.join(processed_lines)


def format_as_markdown(text, pdf_path):
    """
    Format the cleaned text as Markdown.
    
    Args:
        text (str): Cleaned text
        pdf_path (str): Path to the original PDF file
    
    Returns:
        str: Text formatted as Markdown
    """
    # Extract filename without extension
    filename = os.path.basename(pdf_path)
    
    # Extract title (use filename if no better title is found)
    title = filename.replace('.pdf', '')
    
    # Try to find a better title in the first few lines
    lines = text.split('\n')
    for i in range(min(10, len(lines))):
        if lines[i].strip() and len(lines[i].strip()) < 100:
            title = lines[i].strip()
            break
    
    # Construct the Markdown header
    header = f"""# {title}

> Extracted from: {filename}

"""
    
    # Add the content
    markdown_text = header + text
    
    # Replace consecutive blank lines with a single blank line
    markdown_text = re.sub(r'\n{3,}', '\n\n', markdown_text)
    
    return markdown_text


def convert_pdf_to_md(pdf_path, output_dir):
    """
    Convert a PDF file to Markdown and save it to the output directory.
    
    Args:
        pdf_path (str): Path to the PDF file
        output_dir (str): Directory to save the Markdown file
    
    Returns:
        str: Path to the created Markdown file
    """
    print(f"Converting {pdf_path}...")
    
    # Extract text from PDF
    text = extract_text_from_pdf(pdf_path)
    
    # Clean the extracted text
    cleaned_text = clean_text(text)
    
    # Format as Markdown
    markdown_text = format_as_markdown(cleaned_text, pdf_path)
    
    # Create output filename
    output_filename = os.path.basename(pdf_path).replace('.pdf', '.md')
    output_path = os.path.join(output_dir, output_filename)
    
    # Save Markdown to file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(markdown_text)
    
    print(f"Created {output_path}")
    return output_path


def main():
    """Main function to parse arguments and convert PDFs."""
    parser = argparse.ArgumentParser(description='Convert PDF files to Markdown format.')
    parser.add_argument('--source', 
                        default='/home/demute/code/CyberDeltaEngine/.ai_workflow/study',
                        help='Source directory containing PDF files')
    parser.add_argument('--dest', 
                        default='/home/demute/code/CyberDeltaEngine/.ai_workflow/study/pdf',
                        help='Destination directory for Markdown files')
    
    args = parser.parse_args()
    
    # Ensure destination directory exists
    os.makedirs(args.dest, exist_ok=True)
    
    # Find all PDF files in the source directory
    pdf_files = list(Path(args.source).glob('*.pdf'))
    
    if not pdf_files:
        print(f"No PDF files found in {args.source}")
        return
    
    print(f"Found {len(pdf_files)} PDF files to convert")
    
    # Convert each PDF file to Markdown
    for pdf_path in pdf_files:
        convert_pdf_to_md(str(pdf_path), args.dest)
    
    print("Conversion completed!")


if __name__ == '__main__':
    main() 