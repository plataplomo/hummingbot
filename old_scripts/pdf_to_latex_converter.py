#!/usr/bin/env python3
"""
PDF to LaTeX Converter

This script converts PDF files to LaTeX format, preserving mathematical formulas and document structure.
It uses the pdftotext utility for initial text extraction, then processes the content to generate
LaTeX with proper mathematical notation.

Usage:
    python pdf_to_latex_converter.py [--source SOURCE_DIR] [--dest DEST_DIR]

The script will process all PDF files in the source directory and save the LaTeX files
in the destination directory with the same filename but with a .tex extension.
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


def identify_math_formulas(text):
    """
    Attempt to identify mathematical formulas in the text.
    
    Args:
        text (str): Raw text extracted from PDF
    
    Returns:
        str: Text with potential formulas marked for LaTeX processing
    """
    # Pattern for common mathematical symbols and expressions
    math_patterns = [
        # Equations with equality, inequality symbols
        r'([^a-zA-Z0-9\s]?[a-zA-Z0-9\s]+[=><≤≥≈≠][a-zA-Z0-9\s\+\-\*\/\^\(\)\[\]\{\}]+)',
        # Numbered equations like (2.3) or (2.4)
        r'\([0-9]+\.[0-9]+\)',
        # Standalone greek letters
        r'([^a-zA-Z])(alpha|beta|gamma|delta|epsilon|zeta|eta|theta|iota|kappa|lambda|mu|nu|xi|omicron|pi|rho|sigma|tau|upsilon|phi|chi|psi|omega)([^a-zA-Z])',
        # Common uppercase Greek letters
        r'\b(Gamma|Delta|Theta|Lambda|Xi|Pi|Sigma|Upsilon|Phi|Psi|Omega)\b',
        # Summations, integrals, products
        r'(sum|Σ|∑|int|∫|prod|Π|∏)(\s*_\s*\{[^\}]+\})?(\s*\^\s*\{[^\}]+\})?',
        # Fractions
        r'[^a-zA-Z0-9]([a-zA-Z0-9]+)/([a-zA-Z0-9]+)[^a-zA-Z0-9]',
        # Subscripts and superscripts
        r'[a-zA-Z]_[a-zA-Z0-9]',
        r'[a-zA-Z]\^[a-zA-Z0-9]',
        r'[a-zA-Z]_\{[^\}]+\}',
        r'[a-zA-Z]\^\{[^\}]+\}',
        # Math symbols (expanded set)
        r'[∫∑∏∂√∞±×÷≠≈≤≥→←↔∀∃∄∈∉⊂⊃⊆⊇∪∩∧∨¬∅]',
        # Common function names
        r'\b(sin|cos|tan|exp|log|ln|lim|max|min|sup|inf|arg min|arg max|var|cov|corr|E\s*\[|P\s*\()\b',
        # Matrices notation
        r'[A-Z]_\{[0-9,ij]+\}',
        r'[A-Z]_\{[0-9]+,[0-9]+\}',
        # Financial/stats notation
        r'\b(var|std|E\[|P\(|Var\[|Cov\()\b',
        # Partial derivatives
        r'partial[a-zA-Z]/partial[a-zA-Z]',
        # Hat, tilde, bar, vec notations
        r'\b[a-zA-Z]hat\b',
        r'\b[a-zA-Z]tilde\b',
        r'\b[a-zA-Z]bar\b',
        r'\b[a-zA-Z]vec\b',
        r'\b[a-zA-Z]dot\b',
        # Common equation patterns in financial papers
        r'[a-zA-Z]+\s*=\s*[a-zA-Z0-9\+\-\*\/\^\(\)\[\]\{\}]+',
        r'[a-zA-Z]+\s*\+\s*[a-zA-Z0-9\+\-\*\/\^\(\)\[\]\{\}]+',
        # Time indices (common in time series analysis)
        r'[a-zA-Z]+_t',
        r'[a-zA-Z]+_\{t\+[0-9]+\}',
        r'[a-zA-Z]+_\{t-[0-9]+\}',
    ]
    
    # Regular expressions for lines that likely contain entire equations
    equation_line_patterns = [
        r'^\s*[a-zA-Z0-9]+\s*=\s*.+$',  # Lines starting with a variable and equals sign
        r'^\s*\(.+\)\s*$',  # Lines with parenthesized content spanning the whole line
        r'^\s*\d+\.\d+\s*$',  # Equation numbers
        r'^\s*[a-zA-Z0-9]+\s*:\s*.+$',  # Definitions with colon
        r'^\s*\\begin\{equation\}',  # Already LaTeX-formatted equations
        r'^\s*\\end\{equation\}',
        r'^\s*[a-zA-Z0-9]+\s*[=><≤≥≈≠]\s*.+[=><≤≥≈≠]\s*.+$',  # Multiple equalities/inequalities
    ]
    
    # Process each line to identify potential formulas
    lines = text.split('\n')
    processed_lines = []
    
    in_equation_block = False
    equation_buffer = []
    
    for line in lines:
        # Skip empty lines
        if not line.strip():
            processed_lines.append(line)
            continue
        
        # Check if line is an equation number like (2.3) on its own
        if re.match(r'^\s*\([0-9]+\.[0-9]+\)\s*$', line.strip()):
            processed_lines.append(f"\\begin{{equation}}\\label{{eq:{line.strip().strip('()')}}}\\end{{equation}}")
            continue
        
        # Check if line appears to be a full equation
        is_equation_line = False
        for pattern in equation_line_patterns:
            if re.match(pattern, line):
                is_equation_line = True
                break
        
        # Check for centered content which is often an equation
        centered = False
        if len(line.strip()) > 0 and len(line) - len(line.lstrip()) > 10:
            # Line has significant leading whitespace, might be centered
            centered = True
        
        # Check if line contains any mathematical symbols
        contains_math = False
        for pattern in math_patterns:
            if re.search(pattern, line):
                contains_math = True
                break
        
        # Handle equations
        if (is_equation_line or (centered and contains_math)) and len(line.strip()) < 100:
            # This looks like a displayed equation
            if "=" in line or "≈" in line or "≤" in line or "≥" in line:
                processed_lines.append(f"$$$ {line.strip()} $$$")
            else:
                processed_lines.append(f"$$$ {line.strip()} $$$")
        elif contains_math:
            # Line contains math but isn't a full equation
            # Mark with special inline math delimiters
            processed_lines.append(f"$@ {line} @$")
        else:
            # Regular text
            processed_lines.append(line)
    
    return '\n'.join(processed_lines)


def convert_equation_to_latex(equation):
    """
    Convert a plain text equation to LaTeX math notation.
    
    Args:
        equation (str): Plain text equation
    
    Returns:
        str: LaTeX formatted equation
    """
    # Common replacements for mathematical notation
    replacements = [
        # Greek letters (lowercase)
        (r'\balpha\b', r'\\alpha'),
        (r'\bbeta\b', r'\\beta'),
        (r'\bgamma\b', r'\\gamma'),
        (r'\bdelta\b', r'\\delta'),
        (r'\bepsilon\b', r'\\epsilon'),
        (r'\bzeta\b', r'\\zeta'),
        (r'\btheta\b', r'\\theta'),
        (r'\blambda\b', r'\\lambda'),
        (r'\bmu\b', r'\\mu'),
        (r'\bpi\b', r'\\pi'),
        (r'\bsigma\b', r'\\sigma'),
        (r'\bomega\b', r'\\omega'),
        (r'\bxi\b', r'\\xi'),
        (r'\bphi\b', r'\\phi'),
        (r'\bpsi\b', r'\\psi'),
        (r'\bchi\b', r'\\chi'),
        (r'\bnu\b', r'\\nu'),
        (r'\beta\b', r'\\eta'),
        (r'\brho\b', r'\\rho'),
        (r'\btau\b', r'\\tau'),
        # Greek letters (uppercase)
        (r'\bGamma\b', r'\\Gamma'),
        (r'\bDelta\b', r'\\Delta'),
        (r'\bTheta\b', r'\\Theta'),
        (r'\bLambda\b', r'\\Lambda'),
        (r'\bXi\b', r'\\Xi'),
        (r'\bPi\b', r'\\Pi'),
        (r'\bSigma\b', r'\\Sigma'),
        (r'\bUpsilon\b', r'\\Upsilon'),
        (r'\bPhi\b', r'\\Phi'),
        (r'\bPsi\b', r'\\Psi'),
        (r'\bOmega\b', r'\\Omega'),
        # Superscripts and subscripts
        (r'([a-zA-Z0-9])_([a-zA-Z0-9])', r'\1_{\2}'),
        (r'([a-zA-Z0-9])\^([a-zA-Z0-9])', r'\1^{\2}'),
        # Advanced subscripts with multiple characters
        (r'([a-zA-Z0-9])_([a-zA-Z0-9]+)([^a-zA-Z0-9{])', r'\1_{\2}\3'),
        # Financial notation
        (r'\bE\[', r'\\mathbb{E}['),
        (r'\bP\(', r'\\mathbb{P}('),
        (r'\bE\s*\[', r'\\mathbb{E}['),
        (r'\bP\s*\(', r'\\mathbb{P}('),
        (r'\bVar\[', r'\\text{Var}['),
        (r'\bCov\(', r'\\text{Cov}('),
        (r'\bCorr\(', r'\\text{Corr}('),
        (r'\bvar\(', r'\\text{var}('),
        (r'\bcov\(', r'\\text{cov}('),
        (r'\bcorr\(', r'\\text{corr}('),
        (r'\bexp\(', r'\\exp('),
        # Functions
        (r'\bsin\b', r'\\sin'),
        (r'\bcos\b', r'\\cos'),
        (r'\btan\b', r'\\tan'),
        (r'\blog\b', r'\\log'),
        (r'\bln\b', r'\\ln'),
        (r'\bexp\b', r'\\exp'),
        (r'\blim\b', r'\\lim'),
        (r'\bmax\b', r'\\max'),
        (r'\bmin\b', r'\\min'),
        (r'\barg\s*min\b', r'\\arg\\min'),
        (r'\barg\s*max\b', r'\\arg\\max'),
        (r'\bsup\b', r'\\sup'),
        (r'\binf\b', r'\\inf'),
        # Fractions
        (r'([a-zA-Z0-9]+)/([a-zA-Z0-9]+)', r'\\frac{\1}{\2}'),
        # Symbols
        (r'infinity', r'\\infty'),
        (r'inf', r'\\infty'),
        (r'<=', r'\\leq'),
        (r'>=', r'\\geq'),
        (r'!=', r'\\neq'),
        (r'==', r'='),
        (r'->', r'\\rightarrow'),
        (r'<-', r'\\leftarrow'),
        (r'<->', r'\\leftrightarrow'),
        (r'\bsum\b', r'\\sum'),
        (r'\bint\b', r'\\int'),
        (r'\bprod\b', r'\\prod'),
        # Partial derivatives
        (r'partial([a-zA-Z])/partial([a-zA-Z])', r'\\frac{\\partial \1}{\\partial \2}'),
        # Hat, tilde, etc.
        (r'([a-zA-Z])hat\b', r'\\hat{\1}'),
        (r'([a-zA-Z])tilde\b', r'\\tilde{\1}'),
        (r'([a-zA-Z])bar\b', r'\\bar{\1}'),
        (r'([a-zA-Z])vec\b', r'\\vec{\1}'),
        (r'([a-zA-Z])dot\b', r'\\dot{\1}'),
        # Equation numbers
        (r'\(([0-9]+\.[0-9]+)\)', r'\\tag{\1}'),
    ]
    
    eq = equation
    for pattern, replacement in replacements:
        eq = re.sub(pattern, replacement, eq)
    
    return eq


def convert_to_latex(text, pdf_path):
    """
    Convert processed text to LaTeX format.
    
    Args:
        text (str): Processed text with marked formulas
        pdf_path (str): Path to the original PDF file
    
    Returns:
        str: LaTeX formatted text
    """
    # Extract filename for document title
    filename = os.path.basename(pdf_path).replace('.pdf', '')
    
    # Replace common LaTeX special characters
    latex_special_chars = {
        '&': r'\&',
        '%': r'\%',
        '$': r'\$',
        '#': r'\#',
        '_': r'\_',
        '{': r'\{',
        '}': r'\}',
        '~': r'\textasciitilde{}',
        '^': r'\textasciicircum{}',
        '\\': r'\textbackslash{}',
        '<': r'\textless{}',
        '>': r'\textgreater{}'
    }
    
    # Process each line
    lines = text.split('\n')
    processed_lines = []
    
    for line in lines:
        # Handle marked displayed equations
        if line.strip().startswith('$$$'):
            equation = line.strip().strip('$$$').strip()
            # Convert to proper LaTeX equation
            equation = convert_equation_to_latex(equation)
            if "\\tag" in equation:
                # Already has an equation number
                processed_lines.append(f"\\begin{{equation*}}\n{equation}\n\\end{{equation*}}")
            else:
                processed_lines.append(f"\\begin{{equation}}\n{equation}\n\\end{{equation}}")
            continue
        
        # Handle inline math expressions (marked with special delimiters)
        if line.strip().startswith('$@ ') and line.strip().endswith(' @$'):
            content = line.strip()[3:-3]
            
            # Simple replacement for special characters
            for char, replacement in latex_special_chars.items():
                if char not in "$_^":  # Skip math-related special chars
                    content = content.replace(char, replacement)
            
            # Process potential math expressions
            # This is a simplified approach - we just add math delimiters around 
            # common math patterns
            math_patterns = [
                r'\b[a-zA-Z]_[a-zA-Z0-9]', 
                r'\b[a-zA-Z]\^[a-zA-Z0-9]',
                r'[a-zA-Z0-9]+/[a-zA-Z0-9]+',
                r'[=><≤≥≈≠\+\-\*\/\^]',
                r'\([a-zA-Z0-9\+\-\*\/\^\s]+\)',
                r'\[[a-zA-Z0-9\+\-\*\/\^\s]+\]',
                r'\{[a-zA-Z0-9\+\-\*\/\^\s]+\}',
                r'\b(alpha|beta|gamma|delta|theta|lambda|mu|sigma|omega)\b',
                r'\b(sin|cos|tan|exp|log|ln)\b',
            ]
            
            processed_content = content
            for pattern in math_patterns:
                try:
                    # Find all matches
                    matches = list(re.finditer(pattern, processed_content))
                    
                    # Process matches from end to beginning to avoid position shifts
                    for match in reversed(matches):
                        start, end = match.span()
                        # Convert the matched expression to LaTeX
                        math_expr = convert_equation_to_latex(match.group())
                        # Replace in the content
                        processed_content = (
                            processed_content[:start] + 
                            "$" + math_expr + "$" + 
                            processed_content[end:]
                        )
                except re.error:
                    # If there's a regex error, just continue with the next pattern
                    continue
            
            processed_lines.append(processed_content)
            continue
        
        # Handle section headings
        if re.match(r'^#+ ', line):
            level = len(re.match(r'^(#+) ', line).group(1))
            title = line[level+1:].strip()
            
            if level == 1:
                processed_lines.append(f"\\section{{{title}}}")
            elif level == 2:
                processed_lines.append(f"\\subsection{{{title}}}")
            elif level == 3:
                processed_lines.append(f"\\subsubsection{{{title}}}")
            else:
                processed_lines.append(f"\\paragraph{{{title}}}")
            continue
        
        # Handle regular text
        clean_line = line
        
        # Replace LaTeX special characters
        for char, replacement in latex_special_chars.items():
            clean_line = clean_line.replace(char, replacement)
        
        processed_lines.append(clean_line)
    
    # Create LaTeX document
    latex_output = [
        "\\documentclass{article}",
        "\\usepackage[utf8]{inputenc}",
        "\\usepackage{amsmath}",
        "\\usepackage{amssymb}",
        "\\usepackage{amsthm}",
        "\\usepackage{mathtools}",
        "\\usepackage{graphicx}",
        "\\usepackage{hyperref}",
        "\\usepackage{geometry}",
        "\\usepackage{xcolor}",
        "\\usepackage{amsfonts}",  # For mathbb
        "\\geometry{margin=1in}",
        f"\\title{{{filename}}}",
        "\\author{CyberDeltaEngine PDF Converter}",
        "\\date{\\today}",
        "\\begin{document}",
        "\\maketitle",
        ""
    ]
    
    latex_output.extend(processed_lines)
    latex_output.append("\\end{document}")
    
    return '\n'.join(latex_output)


def convert_pdf_to_latex(pdf_path, output_dir):
    """
    Convert a PDF file to LaTeX and save it to the output directory.
    
    Args:
        pdf_path (str): Path to the PDF file
        output_dir (str): Directory to save the LaTeX file
    
    Returns:
        str: Path to the created LaTeX file
    """
    print(f"Converting {pdf_path} to LaTeX...")
    
    # Extract text from PDF
    text = extract_text_from_pdf(pdf_path)
    
    # Process text and identify mathematical formulas
    processed_text = identify_math_formulas(text)
    
    # Convert to LaTeX
    latex_text = convert_to_latex(processed_text, pdf_path)
    
    # Create output filename
    output_filename = os.path.basename(pdf_path).replace('.pdf', '.tex')
    output_path = os.path.join(output_dir, output_filename)
    
    # Save LaTeX to file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(latex_text)
    
    print(f"Created {output_path}")
    return output_path


def main():
    """Main function to parse arguments and convert PDFs."""
    parser = argparse.ArgumentParser(description='Convert PDF files to LaTeX format.')
    parser.add_argument('--source', 
                        default='/home/demute/code/CyberDeltaEngine/.ai_workflow/study',
                        help='Source directory containing PDF files')
    parser.add_argument('--dest', 
                        default='/home/demute/code/CyberDeltaEngine/.ai_workflow/study/latex',
                        help='Destination directory for LaTeX files')
    
    args = parser.parse_args()
    
    # Ensure destination directory exists
    os.makedirs(args.dest, exist_ok=True)
    
    # Find all PDF files in the source directory
    pdf_files = list(Path(args.source).glob('*.pdf'))
    
    if not pdf_files:
        print(f"No PDF files found in {args.source}")
        return
    
    print(f"Found {len(pdf_files)} PDF files to convert")
    
    # Convert each PDF file to LaTeX
    for pdf_path in pdf_files:
        convert_pdf_to_latex(str(pdf_path), args.dest)
    
    print("Conversion completed!")


if __name__ == '__main__':
    main() 