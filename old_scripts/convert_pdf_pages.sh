#!/bin/bash
# Convert PDF files to images using pdftoppm

# Default settings
SOURCE_DIR="/home/demute/code/CyberDeltaEngine/.ai_workflow/study"
OUTPUT_DIR="/home/demute/code/CyberDeltaEngine/.ai_workflow/study/images"
DPI=300
FORMAT="png"
PREFIX="page"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Function to convert a PDF file to images
convert_pdf_to_images() {
    local pdf_file="$1"
    local pdf_name=$(basename "$pdf_file" .pdf)
    local pdf_output_dir="$OUTPUT_DIR/$pdf_name"
    
    # Create directory for this PDF
    mkdir -p "$pdf_output_dir"
    
    echo "Converting $pdf_file to images..."
    
    # Use pdftoppm to convert PDF to images
    pdftoppm -$FORMAT -r $DPI "$pdf_file" "$pdf_output_dir/$PREFIX"
    
    echo "Saved images to $pdf_output_dir/"
    
    # Create a preview image of the first page at lower resolution
    pdftoppm -$FORMAT -r 150 -f 1 -l 1 "$pdf_file" "$OUTPUT_DIR/${pdf_name}_preview"
}

# Create an HTML index file
create_index_html() {
    local index_file="$OUTPUT_DIR/index.html"
    
    echo "Creating index HTML file..."
    
    cat > "$index_file" << HTML
<!DOCTYPE html>
<html>
<head>
    <title>PDF Previews - CyberDeltaEngine</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
        h1 { color: #333; }
        .preview-container { display: flex; flex-wrap: wrap; }
        .preview-card { margin: 10px; background-color: white; box-shadow: 0 2px 5px rgba(0,0,0,0.1); padding: 15px; border-radius: 5px; }
        .preview-card h2 { margin-top: 0; font-size: 1.2em; }
        .preview-card img { max-width: 200px; max-height: 300px; display: block; margin-bottom: 10px; }
        .preview-card a { display: inline-block; margin-top: 10px; color: #0066cc; text-decoration: none; }
        .preview-card a:hover { text-decoration: underline; }
    </style>
</head>
<body>
    <h1>PDF Previews</h1>
    <div class="preview-container">
HTML
    
    # Add each PDF's preview to the index
    for preview_file in "$OUTPUT_DIR"/*_preview-1.$FORMAT; do
        if [ -f "$preview_file" ]; then
            pdf_name=$(basename "$preview_file" _preview-1.$FORMAT)
            pdf_dir=$(basename "$pdf_name")
            
            cat >> "$index_file" << HTML
        <div class="preview-card">
            <h2>$pdf_name</h2>
            <a href="$pdf_dir/"><img src="$(basename "$preview_file")" alt="$pdf_name preview"></a>
            <a href="$pdf_dir/">View all pages</a>
        </div>
HTML
        fi
    done
    
    # Close the HTML file
    cat >> "$index_file" << HTML
    </div>
</body>
</html>
HTML
    
    echo "Created index file: $index_file"
}

# Create an HTML file for each PDF directory
create_pdf_html() {
    local pdf_dir="$1"
    local pdf_name=$(basename "$pdf_dir")
    local index_file="$pdf_dir/index.html"
    
    echo "Creating HTML index for $pdf_name..."
    
    cat > "$index_file" << HTML
<!DOCTYPE html>
<html>
<head>
    <title>$pdf_name - Page Images</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
        h1 { color: #333; }
        .image-container { display: flex; flex-direction: column; align-items: center; }
        .page-image { margin: 20px 0; background-color: white; box-shadow: 0 2px 5px rgba(0,0,0,0.1); padding: 15px; border-radius: 5px; }
        .page-image img { max-width: 100%; height: auto; }
        .page-image p { margin: 10px 0 0 0; font-weight: bold; }
        .navigation { position: fixed; bottom: 20px; right: 20px; background-color: white; padding: 10px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.2); }
        .navigation a { margin: 0 5px; }
    </style>
</head>
<body>
    <h1>$pdf_name</h1>
    <a href="../index.html">Back to index</a>
    <div class="image-container">
HTML
    
    # Add each image to the HTML file
    local counter=1
    for image_file in "$pdf_dir"/*.$FORMAT; do
        if [ -f "$image_file" ]; then
            page_num=$(basename "$image_file" .$FORMAT | sed "s/$PREFIX-//")
            
            cat >> "$index_file" << HTML
        <div class="page-image" id="page$page_num">
            <p>Page $page_num</p>
            <img src="$(basename "$image_file")" alt="Page $page_num">
        </div>
HTML
            counter=$((counter + 1))
        fi
    done
    
    # Add navigation links
    cat >> "$index_file" << HTML
    </div>
    <div class="navigation">
        <a href="#top">Top</a>
HTML
    
    # Add quick navigation links for every 5 pages
    for ((i=1; i<counter; i+=5)); do
        page_num=$(printf "%02d" $i)
        cat >> "$index_file" << HTML
        <a href="#page$page_num">Page $i</a>
HTML
    done
    
    # Close the HTML file
    cat >> "$index_file" << HTML
    </div>
</body>
</html>
HTML
}

# Main script
echo "PDF to Image Converter"
echo "Source directory: $SOURCE_DIR"
echo "Output directory: $OUTPUT_DIR"
echo "DPI: $DPI"

# Check if pdftoppm is installed
if ! command -v pdftoppm &> /dev/null; then
    echo "Error: pdftoppm is not installed. Please install it with:"
    echo "  sudo pacman -S poppler"
    exit 1
fi

# Process each PDF file
for pdf_file in "$SOURCE_DIR"/*.pdf; do
    if [ -f "$pdf_file" ]; then
        convert_pdf_to_images "$pdf_file"
    fi
done

# Create HTML index files
create_index_html

# Create HTML files for each PDF directory
for dir in "$OUTPUT_DIR"/*; do
    if [ -d "$dir" ]; then
        create_pdf_html "$dir"
    fi
done

echo "Conversion completed!"
echo "Open $OUTPUT_DIR/index.html in a web browser to view the results" 