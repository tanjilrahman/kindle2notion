from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set

import notional
from notional.blocks import Paragraph, TextObject, Quote, Callout, Divider, Heading3
from notional.query import TextCondition
from notional.types import Date, ExternalFile, Number, RichText, Title, DatabaseRef
from notional.text import plain_text
from requests import get

# from notional.text import Annotations

# from more_itertools import grouper


NO_COVER_IMG = "https://via.placeholder.com/150x200?text=No%20Cover"


def export_to_notion(
    all_books: Dict,
    enable_location: bool,
    enable_highlight_date: bool,
    enable_book_cover: bool,
    separate_blocks: bool,
    notion_api_auth_token: str,
    notion_database_id: str,
) -> None:
    print("Initiating transfer...\n")
    
    # Create a single Notion connection to reuse for all books
    notion = notional.connect(auth=notion_api_auth_token)
    
    # Pre-fetch database info once (data_source_id) to avoid repeated lookups
    from uuid import UUID
    from notional import util
    
    db_id_formatted = notion_database_id
    if len(notion_database_id) == 32 and '-' not in notion_database_id:
        db_id_formatted = f"{notion_database_id[:8]}-{notion_database_id[8:12]}-{notion_database_id[12:16]}-{notion_database_id[16:20]}-{notion_database_id[20:]}"
    else:
        try:
            db_uuid = UUID(notion_database_id)
            db_id_formatted = str(db_uuid)
        except ValueError:
            extracted_id = util.extract_id_from_string(notion_database_id)
            if extracted_id:
                db_uuid = UUID(extracted_id)
                db_id_formatted = str(db_uuid)
    
    # Cache database data_source_ids
    db_data_source_ids = []
    try:
        db_data = notion.client.databases.retrieve(database_id=db_id_formatted)
        if "data_sources" in db_data:
            data_sources = db_data.get("data_sources", [])
            for ds in data_sources:
                ds_id = ds.get("id")
                if ds_id:
                    db_data_source_ids.append(ds_id)
    except Exception:
        pass
    
    db_data_source_ids_normalized = [ds.replace("-", "").lower() for ds in db_data_source_ids]

    for title in all_books:
        each_book = all_books[title]
        author = each_book["author"]
        clippings = each_book["highlights"]
        
        # Print book title and author
        title_and_author = f"{title} ({author})"
        print(title_and_author)
        
        (
            formatted_clippings,
            last_date,
        ) = _prepare_aggregated_text_for_one_book(clippings, enable_location, enable_highlight_date)
        
        # Use the actual count after removing duplicates
        clippings_count = len(formatted_clippings)
        
        message = _add_book_to_notion(
            title,
            author,
            clippings_count,
            formatted_clippings,
            last_date,
            notion,
            notion_database_id,
            enable_book_cover,
            separate_blocks,
            enable_location,
            enable_highlight_date,
            db_id_formatted,
            db_data_source_ids_normalized,
        )
        if message != "None to add":
            print()


def _prepare_aggregated_text_for_one_book(
        clippings: List, enable_location: bool, enable_highlight_date: bool
) -> Tuple[List, str]:
    """
    Prepare clippings for display. Returns a list of clipping dictionaries
    with structured data and the last date. Removes duplicate clippings.
    """
    formatted_clippings = []
    seen_ids = set()  # Track unique clipping IDs to prevent duplicates
    duplicates_removed = 0
    
    for each_clipping in clippings:
        text = each_clipping[0]
        page = each_clipping[1]
        location = each_clipping[2]
        date = each_clipping[3]
        is_note = each_clipping[4]
        
        # Create a unique identifier for this clipping (text + location + page)
        # Normalize text for comparison (strip whitespace, lowercase first 50 chars)
        text_normalized = text.strip()[:50].lower()
        clipping_id = f"{text_normalized}|{location}|{page}".strip()
        
        # Skip if we've already seen this clipping
        if clipping_id in seen_ids:
            duplicates_removed += 1
            continue
        
        seen_ids.add(clipping_id)
        
        clipping_data = {
            "text": text,
            "page": page,
            "location": location,
            "date": date,
            "is_note": is_note,
            "id": clipping_id,
        }
        formatted_clippings.append(clipping_data)
        last_date = date
    
    if duplicates_removed > 0:
        print(f"  Removed {duplicates_removed} duplicate highlight(s).")
    
    return formatted_clippings, last_date


def _query_database_for_title(
    notion, 
    notion_database_id: str, 
    title: str,
    db_id_formatted: str = None,
    db_data_source_ids_normalized: List[str] = None,
) -> Optional[dict]:
    """
    Retrieve the first page in the database whose Title equals the given value.
    Uses search endpoint to find pages, then verifies they're in our database.
    
    Args:
        db_id_formatted: Pre-formatted database ID (optional, will format if not provided)
        db_data_source_ids_normalized: Pre-fetched normalized data source IDs (optional)
    """
    try:
        from uuid import UUID
        from notional import util
        
        # Format database ID if not provided
        if db_id_formatted is None:
            db_id_formatted = notion_database_id
            if len(notion_database_id) == 32 and '-' not in notion_database_id:
                db_id_formatted = f"{notion_database_id[:8]}-{notion_database_id[8:12]}-{notion_database_id[12:16]}-{notion_database_id[16:20]}-{notion_database_id[20:]}"
            else:
                try:
                    db_uuid = UUID(notion_database_id)
                    db_id_formatted = str(db_uuid)
                except ValueError:
                    extracted_id = util.extract_id_from_string(notion_database_id)
                    if extracted_id:
                        db_uuid = UUID(extracted_id)
                        db_id_formatted = str(db_uuid)
        
        # Get data_source_ids if not provided (cache to avoid repeated lookups)
        if db_data_source_ids_normalized is None:
            db_data_source_ids = []
            try:
                db_data = notion.client.databases.retrieve(database_id=db_id_formatted)
                if "data_sources" in db_data:
                    data_sources = db_data.get("data_sources", [])
                    for ds in data_sources:
                        ds_id = ds.get("id")
                        if ds_id:
                            db_data_source_ids.append(ds_id)
            except Exception:
                pass
            db_data_source_ids_normalized = [ds.replace("-", "").lower() for ds in db_data_source_ids]
        
        # Also normalize the database ID for comparison
        db_id_normalized = db_id_formatted.replace("-", "").lower()
        
        # Use search endpoint to find pages with matching title
        search_response = notion.client.search(
            query=title,
            filter={"property": "object", "value": "page"},
            page_size=100
        )
        
        results = search_response.get("results", [])
        
        # Check each result to see if it belongs to our database
        for result in results:
            try:
                page_id = result.get("id")
                if not page_id:
                    continue
                
                # Retrieve the full page using raw API to handle data_source_id parents
                try:
                    page_data = notion.client.pages.retrieve(page_id=page_id)
                except Exception:
                    continue
                
                # Get the page's parent from raw data
                page_parent = page_data.get("parent", {})
                parent_type = page_parent.get("type")
                
                # Check if parent is our database (could be database_id or data_source_id)
                page_db_id = None
                page_data_source_id = None
                if parent_type == "database_id":
                    page_db_id = page_parent.get("database_id")
                elif parent_type == "data_source_id":
                    page_data_source_id = page_parent.get("data_source_id")
                elif parent_type == "page_id":
                    continue
                
                # Check if page belongs to our database
                is_our_database = False
                
                if page_db_id:
                    page_db_id_normalized = page_db_id.replace("-", "").lower()
                    if page_db_id_normalized == db_id_normalized:
                        is_our_database = True
                
                if page_data_source_id and db_data_source_ids_normalized:
                    page_ds_id_normalized = page_data_source_id.replace("-", "").lower()
                    if page_ds_id_normalized in db_data_source_ids_normalized:
                        is_our_database = True
                
                if is_our_database:
                    # Extract title from raw page data
                    props = page_data.get("properties", {})
                    title_prop = props.get("Title", {})
                    if title_prop.get("type") == "title":
                        title_array = title_prop.get("title", [])
                        if title_array:
                            page_title = "".join([t.get("plain_text", "") for t in title_array])
                        else:
                            page_title = ""
                    else:
                        page_title = str(title_prop.get("title", ""))
                    
                    if page_title.strip().lower() == title.strip().lower():
                        return page_data
                        
            except Exception:
                continue
        
        # Handle pagination if needed
        while search_response.get("has_more"):
            cursor = search_response.get("next_cursor")
            search_response = notion.client.search(
                query=title,
                filter={"property": "object", "value": "page"},
                page_size=100,
                start_cursor=cursor
            )
            results = search_response.get("results", [])
            for result in results:
                try:
                    page_id = result.get("id")
                    if page_id:
                        # Use raw API to retrieve page
                        page_data = notion.client.pages.retrieve(page_id=page_id)
                        page_parent = page_data.get("parent", {})
                        parent_type = page_parent.get("type")
                        
                        page_db_id = None
                        if parent_type == "database_id":
                            page_db_id = page_parent.get("database_id")
                        elif parent_type == "data_source_id":
                            page_db_id = page_parent.get("data_source_id")
                        
                        if page_db_id:
                            page_db_id_normalized = page_db_id.replace("-", "").lower()
                            db_id_normalized = db_id_formatted.replace("-", "").lower()
                            if page_db_id_normalized == db_id_normalized:
                                # Extract title from raw data
                                props = page_data.get("properties", {})
                                title_prop = props.get("Title", {})
                                if title_prop.get("type") == "title":
                                    title_array = title_prop.get("title", [])
                                    if title_array:
                                        page_title = "".join([t.get("plain_text", "") for t in title_array])
                                    else:
                                        page_title = ""
                                else:
                                    page_title = str(title_prop.get("title", ""))
                                
                                if page_title.strip().lower() == title.strip().lower():
                                    return page_data
                except Exception:
                    continue
                    
    except Exception:
        pass

    return None


def _create_page_raw(notion, notion_database_id: str, properties: dict) -> dict:
    """
    Create a page using the raw API to bypass notional's Page model
    which doesn't support data_source_id parent types.
    Returns the raw page data as a dict.
    """
    from uuid import UUID
    from notional import util
    
    # Extract UUID from database ID (handle both plain UUIDs and full strings)
    try:
        db_uuid = UUID(notion_database_id)
    except ValueError:
        # If it's not a valid UUID, try to extract it from a string
        extracted_id = util.extract_id_from_string(notion_database_id)
        if extracted_id:
            db_uuid = UUID(extracted_id)
        else:
            raise ValueError(f"Invalid database ID format: {notion_database_id}")
    
    # Build the request payload
    request = {
        "parent": {"database_id": str(db_uuid)},
        "properties": {
            name: prop.dict() if hasattr(prop, "dict") else prop
            for name, prop in properties.items()
            if prop is not None
        },
    }
    
    # Use the raw client to create the page
    data = notion.client.pages.create(**request)
    return data


def _retrieve_existing_clippings(notion, page_id: str) -> Set[str]:
    """
    Retrieve all existing clippings from a Notion page and return their IDs.
    Clipping IDs are based on text content, location, and page.
    """
    existing_ids = set()
    try:
        from uuid import UUID
        from notional import util
        
        # Format page ID
        try:
            page_uuid = UUID(page_id)
            page_id_formatted = str(page_uuid)
        except ValueError:
            extracted_id = util.extract_id_from_string(page_id)
            if extracted_id:
                page_uuid = UUID(extracted_id)
                page_id_formatted = str(page_uuid)
            else:
                page_id_formatted = page_id
        
        # Get all blocks from the page using raw API
        response = notion.client.blocks.children.list(block_id=page_id_formatted)
        blocks_data = response.get("results", [])
        
        current_text = ""
        current_location = ""
        current_page = ""
        
        for block_data in blocks_data:
            block_type = block_data.get("type", "")
            
            # Skip dividers
            if block_type == "divider":
                # Reset for next clipping
                if current_text:
                    main_text = current_text.split("\n")[0].replace("💡 NOTE\n\n", "").strip()
                    clipping_id = f"{main_text[:50]}|{current_location}|{current_page}".strip()
                    if clipping_id:
                        existing_ids.add(clipping_id)
                    current_text = ""
                    current_location = ""
                    current_page = ""
                continue
            
            # Extract text from different block types using raw data structure
            text_content = ""
            if block_type == "paragraph":
                rich_text = block_data.get("paragraph", {}).get("rich_text", [])
                if rich_text:
                    text_content = "".join([t.get("plain_text", "") for t in rich_text])
            elif block_type == "quote":
                rich_text = block_data.get("quote", {}).get("rich_text", [])
                if rich_text:
                    text_content = "".join([t.get("plain_text", "") for t in rich_text])
            elif block_type == "callout":
                rich_text = block_data.get("callout", {}).get("rich_text", [])
                if rich_text:
                    text_content = "".join([t.get("plain_text", "") for t in rich_text])
            
            if text_content:
                # Extract location and page info from the text
                if "📍 Location" in text_content:
                    try:
                        loc_part = text_content.split("📍 Location")[1].split("•")[0].strip()
                        current_location = loc_part
                    except:
                        pass
                elif "Location:" in text_content:
                    try:
                        loc_part = text_content.split("Location:")[1].split(",")[0].strip()
                        current_location = loc_part
                    except:
                        pass
                
                if "📄 Page" in text_content:
                    try:
                        page_part = text_content.split("📄 Page")[1].split("•")[0].strip()
                        current_page = page_part
                    except:
                        pass
                elif "Page:" in text_content:
                    try:
                        page_part = text_content.split("Page:")[1].split(",")[0].strip()
                        current_page = page_part
                    except:
                        pass
                
                # Extract main text (remove metadata and formatting)
                main_text = text_content
                # Remove NOTE prefix if present
                if "💡 NOTE" in main_text:
                    main_text = main_text.split("💡 NOTE\n\n")[-1]
                # Remove metadata lines (lines with emojis or "Page:", "Location:", "Date Added:")
                lines = main_text.split("\n")
                clean_lines = []
                for line in lines:
                    if not any(x in line for x in ["📄", "📍", "📅", "Page:", "Location:", "Date Added:", "_"]):
                        clean_lines.append(line)
                main_text = "\n".join(clean_lines).strip()
                
                if main_text:
                    current_text = main_text
                    # Create ID from first 50 chars of text + location + page
                    clipping_id = f"{main_text[:50]}|{current_location}|{current_page}".strip()
                    if clipping_id:
                        existing_ids.add(clipping_id)
        
        # Handle pagination
        while response.get("has_more"):
            cursor = response.get("next_cursor")
            response = notion.client.blocks.children.list(block_id=page_id_formatted, start_cursor=cursor)
            blocks_data = response.get("results", [])
            
            for block_data in blocks_data:
                block_type = block_data.get("type", "")
                
                if block_type == "divider":
                    if current_text:
                        main_text = current_text.split("\n")[0].replace("💡 NOTE\n\n", "").strip()
                        clipping_id = f"{main_text[:50]}|{current_location}|{current_page}".strip()
                        if clipping_id:
                            existing_ids.add(clipping_id)
                        current_text = ""
                        current_location = ""
                        current_page = ""
                    continue
                
                text_content = ""
                if block_type == "paragraph":
                    rich_text = block_data.get("paragraph", {}).get("rich_text", [])
                    if rich_text:
                        text_content = "".join([t.get("plain_text", "") for t in rich_text])
                elif block_type == "quote":
                    rich_text = block_data.get("quote", {}).get("rich_text", [])
                    if rich_text:
                        text_content = "".join([t.get("plain_text", "") for t in rich_text])
                elif block_type == "callout":
                    rich_text = block_data.get("callout", {}).get("rich_text", [])
                    if rich_text:
                        text_content = "".join([t.get("plain_text", "") for t in rich_text])
                
                if text_content:
                    # Extract location and page info
                    if "📍 Location" in text_content:
                        try:
                            loc_part = text_content.split("📍 Location")[1].split("•")[0].strip()
                            current_location = loc_part
                        except:
                            pass
                    elif "Location:" in text_content:
                        try:
                            loc_part = text_content.split("Location:")[1].split(",")[0].strip()
                            current_location = loc_part
                        except:
                            pass
                    
                    if "📄 Page" in text_content:
                        try:
                            page_part = text_content.split("📄 Page")[1].split("•")[0].strip()
                            current_page = page_part
                        except:
                            pass
                    elif "Page:" in text_content:
                        try:
                            page_part = text_content.split("Page:")[1].split(",")[0].strip()
                            current_page = page_part
                        except:
                            pass
                    
                    # Extract main text
                    main_text = text_content
                    if "💡 NOTE" in main_text:
                        main_text = main_text.split("💡 NOTE\n\n")[-1]
                    lines = main_text.split("\n")
                    clean_lines = []
                    for line in lines:
                        if not any(x in line for x in ["📄", "📍", "📅", "Page:", "Location:", "Date Added:", "_"]):
                            clean_lines.append(line)
                    main_text = "\n".join(clean_lines).strip()
                    
                    if main_text:
                        current_text = main_text
                        clipping_id = f"{main_text[:50]}|{current_location}|{current_page}".strip()
                        if clipping_id:
                            existing_ids.add(clipping_id)
        
        # Handle last clipping if no trailing divider
        if current_text:
            main_text = current_text.split("\n")[0].replace("💡 NOTE\n\n", "").strip()
            clipping_id = f"{main_text[:50]}|{current_location}|{current_page}".strip()
            if clipping_id:
                existing_ids.add(clipping_id)
                
    except Exception as e:
        # If we can't retrieve blocks, assume no existing clippings
        print(f"Note: Could not retrieve existing clippings: {e}")
        import traceback
        traceback.print_exc()
    
    return existing_ids


def _create_rich_text_array(text: str) -> list:
    """
    Convert plain text to Notion rich_text array format.
    Notion handles newlines automatically in a single text object.
    """
    if not text:
        return []
    return [{
        "type": "text",
        "text": {"content": text},
        "annotations": {
            "bold": False,
            "italic": False,
            "strikethrough": False,
            "underline": False,
            "code": False,
            "color": "default"
        },
        "plain_text": text
    }]


def _create_formatted_clipping_block_raw(
    clipping_data: dict, enable_location: bool, enable_highlight_date: bool
) -> List[dict]:
    """
    Create a beautifully formatted Notion block for a clipping in raw API format.
    Returns a list of block dictionaries ready for batch API calls.
    """
    blocks = []
    text = clipping_data["text"]
    page = clipping_data["page"]
    location = clipping_data["location"]
    date = clipping_data["date"]
    is_note = clipping_data["is_note"]
    
    # Build metadata text
    metadata_parts = []
    if enable_location:
        if page:
            metadata_parts.append(f"📄 Page {page}")
        if location:
            metadata_parts.append(f"📍 Location {location}")
    if enable_highlight_date and date:
        metadata_parts.append(f"📅 {date}")
    
    metadata_text = " • ".join(metadata_parts) if metadata_parts else ""
    
    # Create the main content block in raw API format
    if is_note:
        # Use Callout block for notes with a nice icon
        note_text = f"💡 NOTE\n\n{text}"
        if metadata_text:
            note_text += f"\n\n{metadata_text}"
        blocks.append({
            "type": "callout",
            "callout": {
                "rich_text": _create_rich_text_array(note_text),
                "icon": {"emoji": "💡"}
            }
        })
    else:
        # Use Quote block for highlights with better formatting
        highlight_text = text
        if metadata_text:
            highlight_text += f"\n\n{metadata_text}"
        blocks.append({
            "type": "quote",
            "quote": {
                "rich_text": _create_rich_text_array(highlight_text)
            }
        })
    
    # Add a subtle divider between clippings
    blocks.append({
        "type": "divider",
        "divider": {}
    })
    
    return blocks


def _create_formatted_clipping_block(
    clipping_data: dict, enable_location: bool, enable_highlight_date: bool
) -> List:
    """
    Create a beautifully formatted Notion block for a clipping.
    Returns a list of blocks (may include callout, divider, etc.)
    This is kept for backward compatibility but we'll use the raw version for batch operations.
    """
    # Use raw format for better batch performance
    return _create_formatted_clipping_block_raw(clipping_data, enable_location, enable_highlight_date)


def _find_new_clippings(
    all_clippings: List[dict], existing_ids: Set[str]
) -> List[dict]:
    """
    Compare clippings and return only those that don't already exist.
    """
    new_clippings = []
    for clipping in all_clippings:
        if clipping["id"] not in existing_ids:
            new_clippings.append(clipping)
    return new_clippings


def _set_page_cover_raw(notion, page_id: str, cover) -> None:
    """
    Set a page cover using the raw API to bypass notional's Page model
    which doesn't support data_source_id parent types.
    """
    from uuid import UUID
    from notional import util
    
    # Extract UUID from page ID (handle both plain UUIDs and full strings)
    try:
        page_uuid = UUID(page_id)
    except ValueError:
        # If it's not a valid UUID, try to extract it from a string
        extracted_id = util.extract_id_from_string(page_id)
        if extracted_id:
            page_uuid = UUID(extracted_id)
        else:
            raise ValueError(f"Invalid page ID format: {page_id}")
    
    # Build the request payload
    cover_dict = cover.dict() if hasattr(cover, "dict") else cover
    request = {"cover": cover_dict}
    
    # Use the raw client to update the page cover
    notion.client.pages.update(page_id=page_uuid.hex, **request)


def _add_book_to_notion(
    title: str,
    author: str,
    clippings_count: int,
    formatted_clippings: list,
    last_date: str,
    notion,  # Reuse existing connection instead of creating new one
    notion_database_id: str,
    enable_book_cover: bool,
    separate_blocks: bool,
    enable_location: bool,
    enable_highlight_date: bool,
    db_id_formatted: str,  # Pre-formatted database ID
    db_data_source_ids_normalized: List[str],  # Pre-fetched data source IDs
):
    last_date = datetime.strptime(last_date, "%A, %d %B %Y %I:%M:%S %p")

    # Condition variables
    title_exists = False
    current_clippings_count = 0
    block_id = None

    data = _query_database_for_title(notion, notion_database_id, title, db_id_formatted, db_data_source_ids_normalized)

    if data:
        title_exists = True
        # Handle both dict and object responses
        if isinstance(data, dict):
            block_id = data.get("id")
        else:
            block_id = getattr(data, "id", None)
        
        if block_id:
            try:
                # Use raw API to retrieve page to handle data_source_id parents
                block_data = notion.client.pages.retrieve(page_id=block_id)
                # Extract highlights count from raw data
                props = block_data.get("properties", {})
                highlights_prop = props.get("Highlights", {})
                if highlights_prop.get("type") == "number":
                    current_clippings_count = highlights_prop.get("number", 0) or 0
                else:
                    current_clippings_count = 0
            except Exception:
                title_exists = False
        else:
            title_exists = False

    # Add a new book to the database
    if not title_exists:
        print(f"  Adding {clippings_count} highlights...", end="")
        # Use raw API call to bypass notional's Page model which doesn't support data_source_id
        page_data = _create_page_raw(
            notion,
            notion_database_id,
            {
                "Title": Title[title],
                "Author": RichText[author],
                "Highlights": Number[clippings_count],
                "Last Highlighted": Date[last_date.isoformat()],
                "Last Synced": Date[datetime.now().isoformat()],
            },
        )
        # Create a minimal page-like object for compatibility with existing code
        class PageWrapper:
            def __init__(self, data):
                self.id = data.get("id")
                self.cover = data.get("cover")
        
        new_page = PageWrapper(page_data)
        block_id = new_page.id

        # Collect all blocks first, then add them in batches for better performance
        all_blocks = []
        for clipping_data in formatted_clippings:
            blocks = _create_formatted_clipping_block(
                clipping_data, enable_location, enable_highlight_date
            )
            all_blocks.extend(blocks)
        
        # Notion API allows up to 100 blocks per request, so batch them using raw API
        batch_size = 100
        for i in range(0, len(all_blocks), batch_size):
            batch = all_blocks[i:i + batch_size]
            # Blocks are already in raw API format from _create_formatted_clipping_block_raw
            notion.client.blocks.children.append(block_id=new_page.id, children=batch)
        
        print(" ✓")

        if enable_book_cover:
            # Fetch a book cover from Google Books if the cover for the page is not set
            if new_page.cover is None:
                result = _get_book_cover_uri(title, author)

            if result is None:
                # Set the page cover to a placeholder image
                cover = ExternalFile[NO_COVER_IMG]
                print(
                    "× Book cover couldn't be found. "
                    "Please replace the placeholder image with the original book cover manually."
                )
            else:
                # Set the page cover to that of the book
                cover = ExternalFile[result]
                print("✓ Added book cover.")

            _set_page_cover_raw(notion, new_page.id, cover)
        
        message = f"Added {clippings_count} highlights.\n"
    else:
        # Update existing book - check if highlights count matches
        # Compare highlights count - if it matches, no update needed
        if current_clippings_count == clippings_count:
            # Still update Last Synced timestamp
            try:
                from uuid import UUID
                from notional import util
                
                try:
                    page_uuid = UUID(block_id)
                    page_id_formatted = str(page_uuid)
                except ValueError:
                    extracted_id = util.extract_id_from_string(block_id)
                    if extracted_id:
                        page_uuid = UUID(extracted_id)
                        page_id_formatted = str(page_uuid)
                    else:
                        page_id_formatted = block_id
                
                update_props = {
                    "Last Synced": {"date": {"start": datetime.now().isoformat()}}
                }
                notion.client.pages.update(page_id=page_id_formatted, properties=update_props)
            except Exception as e:
                print(f"Warning: Could not update page metadata: {e}")
            print("  No changes needed.")
            return "No changes needed.\n"
        
        # Highlights count doesn't match - delete page and recreate with new clippings
        print(f"  Updating {clippings_count} highlights...", end="")
        
        try:
            from uuid import UUID
            from notional import util
            
            try:
                page_uuid = UUID(block_id)
                page_id_formatted = str(page_uuid)
            except ValueError:
                extracted_id = util.extract_id_from_string(block_id)
                if extracted_id:
                    page_uuid = UUID(extracted_id)
                    page_id_formatted = str(page_uuid)
                else:
                    page_id_formatted = block_id
            
            # Get current page data to preserve properties
            page_data = notion.client.pages.retrieve(page_id=page_id_formatted)
            existing_cover = page_data.get("cover")
            
            # Delete the entire page (archives it)
            notion.client.pages.update(
                page_id=page_id_formatted,
                archived=True
            )
            
            # Recreate the page with updated properties
            new_page_data = _create_page_raw(
                notion,
                notion_database_id,
                {
                    "Title": Title[title],
                    "Author": RichText[author],
                    "Highlights": Number[clippings_count],
                    "Last Highlighted": Date[last_date.isoformat()],
                    "Last Synced": Date[datetime.now().isoformat()],
                },
            )
            
            # Create a minimal page-like object for compatibility
            class PageWrapper:
                def __init__(self, data):
                    self.id = data.get("id")
                    self.cover = data.get("cover")
            
            new_page = PageWrapper(new_page_data)
            new_page_id = new_page.id
            
            # Restore cover if it existed
            if existing_cover and enable_book_cover:
                try:
                    if existing_cover.get("type") == "external":
                        cover_url = existing_cover.get("external", {}).get("url")
                        if cover_url:
                            cover = ExternalFile[cover_url]
                            _set_page_cover_raw(notion, new_page_id, cover)
                except Exception:
                    pass
            
            # Prepare all blocks
            all_blocks = []
            for clipping_data in formatted_clippings:
                blocks = _create_formatted_clipping_block(
                    clipping_data, enable_location, enable_highlight_date
                )
                all_blocks.extend(blocks)
            
            # Add all blocks in batches using raw API
            batch_size = 100
            total_batches = (len(all_blocks) + batch_size - 1) // batch_size
            for i in range(0, len(all_blocks), batch_size):
                batch = all_blocks[i:i + batch_size]
                # Blocks are already in raw API format from _create_formatted_clipping_block_raw
                notion.client.blocks.children.append(block_id=new_page_id, children=batch)
            
            print(" ✓")
            
            # Update block_id for any subsequent operations
            block_id = new_page_id
            
        except Exception as e:
            print(f"Warning: Could not recreate page: {e}")
            import traceback
            traceback.print_exc()
            return f"Error updating book: {e}\n"

        diff_count = clippings_count - current_clippings_count
        if diff_count > 0:
            message = f"Added {diff_count} new highlights. Total: {clippings_count}.\n"
        elif diff_count < 0:
            message = f"Removed {abs(diff_count)} highlights. Total: {clippings_count}.\n"
        else:
            message = f"Updated highlights. Total: {clippings_count}.\n"

    return message


# def _create_rich_text_object(text):
#     if "Note: " in text:
#         # Bold text
#         nested = TextObject._NestedData(content=text)
#         rich = TextObject(text=nested, plain_text=text, annotations=Annotations(bold=True))
#     elif any(item in text for item in ["Page: ", "Location: ", "Date Added: "]):
#         # Italic text
#         nested = TextObject._NestedData(content=text)
#         rich = TextObject(text=nested, plain_text=text, annotations=Annotations(italic=True))
#     else:
#         # Plain text
#         nested = TextObject._NestedData(content=text)
#         rich = TextObject(text=nested, plain_text=text)
#     return rich


# def _update_book_with_clippings(formatted_clippings):
#     rtf = []
#     for each_clipping in formatted_clippings:
#         each_clipping_list = each_clipping.split("*")
#         each_clipping_list = list(filter(None, each_clipping_list))
#         for each_line in each_clipping_list:
#             rtf.append(_create_rich_text_object(each_line))
#     print(len(rtf))
#     content = Paragraph._NestedData(rich_text=rtf)
#     para = Paragraph(paragraph=content)
#     return para


def _get_book_cover_uri(title: str, author: str):
    req_uri = "https://www.googleapis.com/books/v1/volumes?q="

    if title is None:
        return
    req_uri += "intitle:" + title

    if author is not None:
        req_uri += "+inauthor:" + author

    response = get(req_uri).json().get("items", [])
    if len(response) > 0:
        for x in response:
            if x.get("volumeInfo", {}).get("imageLinks", {}).get("thumbnail"):
                return (
                    x.get("volumeInfo", {})
                    .get("imageLinks", {})
                    .get("thumbnail")
                    .replace("http://", "https://")
                )
    return
