#!/usr/bin/env python3
"""
Scrape Carbon Pulse articles using curl and convert them to PDFs.
Usage: python scrape_to_pdf_curl.py
"""

import os
import sys
import subprocess
from pathlib import Path
from bs4 import BeautifulSoup
from weasyprint import HTML
import time
import re

# List of URLs to scrape
URLS = [
    "https://carbon-pulse.com/269445/",
    "https://carbon-pulse.com/318359/",
    "https://carbon-pulse.com/246231/",
    "https://carbon-pulse.com/324818/",
    "https://carbon-pulse.com/322494/",
    "https://carbon-pulse.com/416531/",
    "https://carbon-pulse.com/426888/",
    "https://carbon-pulse.com/425954/",
    "https://carbon-pulse.com/331395/",
    "https://carbon-pulse.com/398798/",
    "https://carbon-pulse.com/186795/",
    "https://carbon-pulse.com/373519/",
    "https://carbon-pulse.com/417007/",
    "https://carbon-pulse.com/372548/",
    "https://carbon-pulse.com/360658/",
    "https://carbon-pulse.com/395382/",
    "https://carbon-pulse.com/386964/",
    "https://carbon-pulse.com/369091/",
    "https://carbon-pulse.com/376874/",
    "https://carbon-pulse.com/369511/",
]

def sanitize_filename(text):
    """Convert text to a safe filename."""
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '_', text)
    return text[:100]

def fetch_with_curl(url):
    """Fetch URL content using curl."""
    try:
        cmd = [
            'curl',
            '-s',  # Silent mode
            '-L',  # Follow redirects
            '-A', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            url
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            return result.stdout
        else:
            print(f"  curl failed with return code {result.returncode}")
            return None
    except Exception as e:
        print(f"  Error fetching with curl: {str(e)}")
        return None

def scrape_article(url):
    """Scrape an article from the given URL using curl."""
    print(f"Scraping: {url}")

    try:
        # Fetch content with curl
        html_content = fetch_with_curl(url)
        if not html_content:
            return None

        # Parse with BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find the article title - Carbon Pulse uses h2.posttitle
        title_elem = soup.select_one('h2.posttitle')
        if title_elem:
            title = title_elem.get_text(strip=True)
        else:
            # Fallback
            title_elem = soup.find(['h1', 'h2'])
            title = title_elem.get_text(strip=True) if title_elem else f"Article_{url.split('/')[-2]}"

        # Find the article content - Carbon Pulse uses div.entry
        article = soup.select_one('div.entry')

        if not article:
            print(f"  Warning: Could not find article content for {url}")
            # Fallback
            for selector in ['.entry-content', 'article', '.post-content', 'main']:
                article = soup.select_one(selector)
                if article:
                    break

            if not article:
                print(f"  Error: No article content found")
                return None

        # Remove unwanted elements
        unwanted_selectors = [
            'nav', 'header', 'footer', '.sidebar', '.navigation',
            '.menu', '.ads', '.advertisement', '.social-share',
            '.comments', 'script', 'style', 'iframe', '.related-posts'
        ]

        for selector in unwanted_selectors:
            for elem in article.select(selector):
                elem.decompose()

        # Build HTML for PDF
        pdf_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>{title}</title>
            <style>
                body {{
                    font-family: Georgia, serif;
                    line-height: 1.6;
                    max-width: 800px;
                    margin: 0 auto;
                    padding: 20px;
                    color: #333;
                }}
                h1, h2 {{
                    color: #000;
                }}
                h1 {{
                    font-size: 28px;
                    margin-bottom: 10px;
                }}
                h2 {{
                    font-size: 22px;
                    margin-top: 20px;
                }}
                h3 {{
                    font-size: 18px;
                    margin-top: 15px;
                }}
                p {{
                    margin: 10px 0;
                }}
                a {{
                    color: #0066cc;
                    text-decoration: none;
                }}
                img {{
                    max-width: 100%;
                    height: auto;
                }}
                .source {{
                    margin-top: 30px;
                    padding-top: 15px;
                    border-top: 1px solid #ccc;
                    font-size: 12px;
                    color: #666;
                }}
            </style>
        </head>
        <body>
            <h1>{title}</h1>
            {article}
            <div class="source">
                <p>Source: <a href="{url}">{url}</a></p>
            </div>
        </body>
        </html>
        """

        return {
            'title': title,
            'html': pdf_html,
            'url': url
        }

    except Exception as e:
        print(f"  Error scraping {url}: {str(e)}")
        return None

def save_to_pdf(article_data, output_dir):
    """Save article data to PDF."""
    if not article_data:
        return None

    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    filename = sanitize_filename(article_data['title'])
    pdf_path = output_dir / f"{filename}.pdf"

    try:
        HTML(string=article_data['html']).write_pdf(pdf_path)
        print(f"  Saved: {pdf_path.name}")
        return pdf_path
    except Exception as e:
        print(f"  Error saving PDF: {str(e)}")
        return None

def main():
    """Main function to scrape all URLs and convert to PDFs."""
    output_dir = Path(__file__).parent.parent / "carbon_pulse_pdfs"

    print(f"Starting scrape of {len(URLS)} URLs...")
    print(f"Output directory: {output_dir}\n")

    successful = 0
    failed = 0

    for i, url in enumerate(URLS, 1):
        print(f"[{i}/{len(URLS)}] ", end='')
        article_data = scrape_article(url)

        if article_data:
            pdf_path = save_to_pdf(article_data, output_dir)
            if pdf_path:
                successful += 1
            else:
                failed += 1
        else:
            failed += 1

        # Be polite - wait between requests
        if i < len(URLS):
            time.sleep(3)

    print("\n" + "="*60)
    print(f"Scraping complete!")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"PDFs saved to: {output_dir}")
    print("="*60)

if __name__ == "__main__":
    main()
