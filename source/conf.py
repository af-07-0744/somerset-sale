# Configuration file for the Sphinx documentation builder.

project = "3000 Somervale Court SW #209 - Pricing Memo"
copyright = "2026, Joseph Surmava"
author = "Joseph Surmava"
release = "0.1.0"

extensions = [
    "sphinx_design",
    "rst2pdf.pdfbuilder",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
source_suffix = ".rst"

html_theme = "sphinx_book_theme"
html_theme_options = {
    "navigation_with_keys": True,
}
html_static_path = ["_static"]
html_css_files = ["custom.css"]

pdf_documents = [
    ("index", "SomervalePricingMemo", "Somervale Pricing Memo", "Joseph Surmava"),
]
pdf_stylesheets = ["sphinx"]
