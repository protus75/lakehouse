"""
PDF PROJECT SETUP WIZARD
========================

Interactive prompt that asks for project requirements and generates all files.
This is the main entry point for creating new PDF data projects.

Run from Jupyter:
  exec(open('notebooks/setup_pdf_project.py').read())

Or from CLI:
  python notebooks/setup_pdf_project.py
"""

import re
from pathlib import Path
from tools.pdf_project_generator import create_project


def slugify(name: str) -> str:
    """Convert project name to slug format."""
    slug = name.lower().replace(" ", "_")
    slug = re.sub(r"[^a-z0-9_]", "", slug)
    return slug


def parse_metadata_string(metadata_str: str) -> list[str]:
    """Parse metadata string into list of field names."""
    if not metadata_str:
        return []
    items = [m.strip().lower().replace(" ", "_") for m in metadata_str.split(",")]
    return [re.sub(r"[^a-z0-9_]", "", item) for item in items if item]


def interactive_setup():
    """
    Interactive project setup wizard.

    In a Jupyter notebook, this would use Claude's AskUserQuestion tool.
    For now, it can be customized based on implementation environment.
    """
    print("\n" + "=" * 60)
    print("PDF DATA PROJECT SETUP WIZARD")
    print("=" * 60)
    print()

    # Step 1: Project name
    print("Step 1 of 4: Project Information")
    print("-" * 40)
    project_name = input("Project name (e.g., 'Research Papers', 'Legal Documents'): ").strip()
    if not project_name:
        print("❌ Project name required")
        return

    project_slug = slugify(project_name)
    print(f"✓ Project slug: {project_slug}")
    print()

    # Step 2: Document type
    print("Step 2 of 4: Document Type")
    print("-" * 40)
    print("What types of documents will you be ingesting?")
    print("1. Research papers")
    print("2. Business documents (reports, contracts, policies)")
    print("3. Technical documentation (guides, API docs)")
    print("4. Other/Mixed")
    choice = input("Select (1-4): ").strip()

    doc_types = {
        "1": "Research papers",
        "2": "Business documents",
        "3": "Technical documentation",
        "4": "Other/Mixed",
    }
    doc_type = doc_types.get(choice, "Mixed")
    print(f"✓ Document type: {doc_type}")
    print()

    # Step 3: Use case
    print("Step 3 of 4: Primary Use Case")
    print("-" * 40)
    print("What is the primary use case?")
    print("1. Q&A / Search only")
    print("2. Analysis / Reporting only")
    print("3. Both (Q&A + Analytics)")
    choice = input("Select (1-3): ").strip()

    use_cases = {
        "1": "Q&A / Search",
        "2": "Analysis / Reporting",
        "3": "Both (Recommended)",
    }
    use_case = use_cases.get(choice, "Both (Recommended)")
    print(f"✓ Use case: {use_case}")
    print()

    # Step 4: Metadata fields
    print("Step 4 of 4: Metadata Fields")
    print("-" * 40)
    print("Which metadata should be tracked?")
    print("1. Basic only (filename, document title)")
    print("2. Enhanced (filename, title, type, tags, version)")
    print("3. Custom (provide comma-separated field names)")
    choice = input("Select (1-3): ").strip()

    if choice == "1":
        metadata_fields = ["document_type"]
    elif choice == "2":
        metadata_fields = ["document_type", "tags", "version"]
    else:
        custom = input("Enter custom fields (comma-separated, e.g., 'category, author, date'): ").strip()
        metadata_fields = parse_metadata_string(custom)
        if not metadata_fields:
            metadata_fields = ["document_type"]

    print(f"✓ Metadata fields: {', '.join(metadata_fields)}")
    print()

    # Summary
    print("=" * 60)
    print("PROJECT SUMMARY")
    print("=" * 60)
    print(f"Name:              {project_name}")
    print(f"Slug:              {project_slug}")
    print(f"Document Type:     {doc_type}")
    print(f"Use Case:          {use_case}")
    print(f"Metadata Fields:   {', '.join(metadata_fields)}")
    print("=" * 60)
    print()

    confirm = input("Create project? (y/n): ").strip().lower()
    if confirm != "y":
        print("❌ Setup cancelled")
        return

    # Generate project
    create_project(
        project_name=project_name,
        project_slug=project_slug,
        doc_type=doc_type,
        use_case=use_case,
        metadata_fields=metadata_fields,
    )


def jupyter_setup():
    """
    Version for Jupyter notebook using Claude's AskUserQuestion.

    This would be called from a Jupyter cell with access to AskUserQuestion.
    It returns a dictionary of user choices that can be passed to create_project().

    Usage in Jupyter:
      from notebooks.setup_pdf_project import jupyter_setup
      choices = jupyter_setup()  # Will prompt user with questions
      # Then creates the project automatically
    """
    from IPython.display import display, Markdown

    display(Markdown("# PDF Project Setup Wizard"))
    display(Markdown("Answer the following questions to configure your new project."))

    # This is where AskUserQuestion would be called
    # For now, returning template for how it would work
    return {
        "project_name": "",
        "doc_type": "",
        "use_case": "",
        "metadata_fields": [],
    }


if __name__ == "__main__":
    interactive_setup()
