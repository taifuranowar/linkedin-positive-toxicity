from playwright.sync_api import sync_playwright, TimeoutError
import time
import os
import re
import sqlite3
from datetime import datetime, timedelta
import argparse
import urllib.parse
import uuid

def init_database(db_path):
    """
    Initialize SQLite database and create table if it doesn't exist
    
    Args:
        db_path (str): Path to the SQLite database file
    
    Returns:
        sqlite3.Connection: Database connection object
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create posts table if it doesn't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS linkedin_posts (
        post_id TEXT PRIMARY KEY,
        post_date TEXT,
        post_author TEXT,
        profile_headline TEXT,
        text TEXT NOT NULL,
        post_url TEXT,
        hashtags TEXT
    )
    ''')
    
    conn.commit()
    return conn

def extract_hashtags(text):
    """Extract hashtags from post text"""
    if not text:
        return None
    
    hashtags = re.findall(r'#\w+', text)
    if hashtags:
        return ', '.join(hashtags)
    return None

def remove_duplicated_text(text):
    """
    Remove duplicated text that sometimes appears in LinkedIn elements
    
    Args:
        text (str): Text that might contain duplications
    
    Returns:
        str: Clean text without duplications
    """
    if not text:
        return None
        
    # Check if text is duplicated (same text appears twice)
    text = text.strip()
    half_length = len(text) // 2
    
    if half_length > 0 and text[:half_length] == text[half_length:]:
        return text[:half_length].strip()
    
    # Check for other common duplication patterns
    lines = text.split('\n')
    if len(lines) >= 2 and lines[0].strip() == lines[1].strip():
        return lines[0].strip()
        
    return text

def clean_author_name(text):
    """
    Clean author name from LinkedIn formatting
    
    Args:
        text (str): Raw author text that might contain connection info
    
    Returns:
        str: Clean author name
    """
    if not text:
        return None
    
    # First, remove any duplication
    text = remove_duplicated_text(text)
    
    # Then remove connection information
    # Pattern: anything after " • " or just the connection level "3rd+"
    text = re.sub(r'\s*•\s*.*$', '', text)
    text = re.sub(r'\s*3rd\+.*$', '', text)
    
    return text.strip()

def convert_relative_date(relative_date):
    """
    Convert LinkedIn's relative date (e.g., "1d", "2h", "3w") to actual date
    
    Args:
        relative_date (str): LinkedIn's relative date string
    
    Returns:
        str: Formatted date (YYYY-MM-DD)
    """
    if not relative_date:
        return None
        
    current_date = datetime.now()
    
    # Extract the number and unit
    match = re.match(r'(\d+)([dhmsw])', relative_date)
    if not match:
        return None
        
    value = int(match.group(1))
    unit = match.group(2)
    
    # Calculate the actual date
    if unit == 'd':  # days
        actual_date = current_date - timedelta(days=value)
    elif unit == 'h':  # hours
        actual_date = current_date - timedelta(hours=value)
    elif unit == 'm':  # minutes
        actual_date = current_date - timedelta(minutes=value)
    elif unit == 's':  # seconds
        actual_date = current_date - timedelta(seconds=value)
    elif unit == 'w':  # weeks
        actual_date = current_date - timedelta(weeks=value)
    else:
        return None
    
    # Format the date
    return actual_date.strftime('%Y-%m-%d')

def save_posts_to_db(conn, posts):
    """
    Save posts to SQLite database
    
    Args:
        conn (sqlite3.Connection): Database connection
        posts (list): List of post texts or post dictionaries
    
    Returns:
        int: Number of posts saved
    """
    if not posts:
        print("No posts collected to save.")
        return 0
    
    cursor = conn.cursor()
    saved_count = 0
    
    for post in posts:
        # If post is just a text string, create a post dictionary with only text populated
        if isinstance(post, str):
            post_text = post
            post = {
                'text': post_text,
                'post_id': str(uuid.uuid4()),
                'post_date': None,
                'post_author': None,
                'profile_headline': None,
                'post_url': None,
                'hashtags': extract_hashtags(post_text)
            }
        
        # Insert the post into the database
        try:
            cursor.execute('''
            INSERT INTO linkedin_posts 
            (post_id, post_date, post_author, profile_headline, text, post_url, hashtags)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                post.get('post_id', str(uuid.uuid4())),
                post.get('post_date'),
                post.get('post_author'),
                post.get('profile_headline'),
                post.get('text'),
                post.get('post_url'),
                post.get('hashtags')
            ))
            saved_count += 1
        except sqlite3.IntegrityError:
            print(f"Post with ID {post.get('post_id')} already exists in the database. Skipping.")
        except Exception as e:
            print(f"Error saving post to database: {e}")
    
    conn.commit()
    print(f"Batch saved: {saved_count} posts saved to database.")
    return saved_count

def scrape_linkedin_posts(db_conn, email=None, password=None, search_query=None, max_posts=50, scroll_delay=2, timeout=60000, batch_size=10):
    """
    Scrape LinkedIn posts and save them to SQLite database
    
    Args:
        db_conn (sqlite3.Connection): Database connection
        email (str, optional): LinkedIn email/username
        password (str, optional): LinkedIn password
        search_query (str, optional): Search query to find specific posts
        max_posts (int, optional): Maximum number of posts to scrape
        scroll_delay (int, optional): Delay between scrolls in seconds
        timeout (int, optional): Timeout for page operations in milliseconds
        batch_size (int, optional): Save to database after collecting this many posts
    
    Returns:
        int: Number of posts saved to database
    """
    with sync_playwright() as p:
        # Launch browser - using headless=False to see the automation in action
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            viewport={'width': 1280, 'height': 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
        )
        
        # Set default timeout for all operations
        context.set_default_timeout(timeout)
        page = context.new_page()
        
        try:
            # Navigate to LinkedIn login page
            print("Navigating to LinkedIn login page...")
            page.goto('https://www.linkedin.com/login', wait_until="domcontentloaded")
            
            # Wait for login form to be interactive
            page.wait_for_selector('input[id="username"]', state="visible")
            
            # Get LinkedIn credentials
            linkedin_email = email or os.environ.get('LINKEDIN_EMAIL') or input("Enter your LinkedIn email: ")
            linkedin_password = password or os.environ.get('LINKEDIN_PASSWORD') or input("Enter your LinkedIn password: ")
            
            # Login
            print("Logging in...")
            page.fill('input[id="username"]', linkedin_email)
            page.fill('input[id="password"]', linkedin_password)
            
            # Click login button
            page.click('button[type="submit"]')
            
            # Wait for login to complete by checking URL change
            print("Waiting for login to complete...")
            
            # Wait briefly to allow redirect to start
            time.sleep(2)
            
            # Debug: Print current URL
            print(f"Current URL after login attempt: {page.url}")
            
            # Check if we're on a secure checkpoint page
            if 'checkpoint' in page.url:
                print("Security checkpoint detected. Please complete the verification manually.")
                input("Press Enter after completing the security verification...")
            
            # Check if we're still on the login page (login failed)
            elif 'login' in page.url:
                print("Possible login failure. Still on login page.")
                
                # Check for error messages
                error_elements = page.query_selector_all('.alert, .form__alert--error')
                if error_elements:
                    for error in error_elements:
                        print(f"Error message found: {error.inner_text()}")
                    print("Login failed. Please check your credentials.")
                    return 0
                else:
                    print("No error message found but still on login page. Will continue anyway...")
            
            else:
                print("Login appears successful. Proceeding...")
            
            # If search query is provided, navigate directly to search results
            if search_query:
                try:
                    # URL encode the search query
                    encoded_query = urllib.parse.quote(search_query)
                    
                    # Build the search URL
                    search_url = f"https://www.linkedin.com/search/results/content/?keywords={encoded_query}&origin=CLUSTER_EXPANSION&sid=Pfc"
                    
                    print(f"Navigating directly to search results for: {search_query}")
                    print(f"URL: {search_url}")
                    
                    # Navigate to search results page
                    page.goto(search_url, wait_until="domcontentloaded")
                    
                    # Wait for content to load
                    print("Waiting for search results to load...")
                    time.sleep(5)  # Give some time for results to load
                    
                    # Verify we're on the right page
                    print(f"Current URL after navigation to search results: {page.url}")
                    
                except Exception as e:
                    print(f"Error navigating to search results: {e}")
                    print("Continuing with the current page content...")
            else:
                # If no search query, just go to the feed
                try:
                    print("No search query provided. Navigating to LinkedIn feed...")
                    page.goto('https://www.linkedin.com/feed/', wait_until="domcontentloaded")
                    time.sleep(3)  # Give time for the feed to load
                except Exception as e:
                    print(f"Error navigating to feed: {e}")
            
            # Store seen posts to avoid duplicates
            seen_posts = set()
            all_posts = []
            total_saved = 0
            current_batch = []
            
            print(f"Starting to scrape LinkedIn posts...")
            
            # Function to extract posts currently in view
            def extract_visible_posts():
                try:
                    # First, find all "...more" buttons and click them to expand the posts
                    expand_more_buttons = page.query_selector_all('button.feed-shared-inline-show-more-text__see-more-less-toggle.see-more')
                    if expand_more_buttons:
                        print(f"Found {len(expand_more_buttons)} '...more' buttons to expand")
                        for i, button in enumerate(expand_more_buttons):
                            try:
                                # Check if this button is visible in the viewport
                                is_visible = button.is_visible()
                                if is_visible:
                                    print(f"Clicking '...more' button {i+1}/{len(expand_more_buttons)}")
                                    button.click()
                                    # Short delay to let content expand
                                    time.sleep(0.3)
                            except Exception as click_error:
                                print(f"Error clicking '...more' button: {click_error}")
                    
                    # Find all post containers
                    post_containers = page.query_selector_all('div.feed-shared-update-v2')
                    
                    if not post_containers:
                        print("No post containers found. Trying alternative selectors...")
                        # Try alternative selectors for different LinkedIn page structures
                        post_containers = page.query_selector_all('li.search-results__search-feed-update')
                    
                    if not post_containers:
                        print("No posts found with any of the selectors. Taking a screenshot...")
                        screenshot_path = f"no_posts_found_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                        page.screenshot(path=screenshot_path)
                        print(f"Screenshot saved to: {screenshot_path}")
                        return []
                    
                    print(f"Found {len(post_containers)} post containers")
                    
                    new_posts = []
                    for container in post_containers:
                        try:
                            # Extract post ID from data-urn attribute
                            post_id = None
                            data_urn = container.get_attribute('data-urn')
                            if data_urn:
                                match = re.search(r'urn:li:activity:(\d+)', data_urn)
                                if match:
                                    post_id = match.group(1)
                                    
                            # Skip if we've already processed this post
                            if post_id and post_id in seen_posts:
                                continue
                                
                            # Extract post text
                            post_text_element = container.query_selector('.feed-shared-update-v2__description')
                            if not post_text_element:
                                post_text_element = container.query_selector('.update-components-text')
                            
                            if not post_text_element:
                                # Try another selector for the text content
                                post_text_element = container.query_selector('.feed-shared-inline-show-more-text')
                            
                            post_text = ""
                            if post_text_element:
                                post_text = post_text_element.inner_text().strip()
                            
                            # Skip empty posts
                            if not post_text:
                                continue
                                
                            # Extract author name and clean it
                            author_element = container.query_selector('.update-components-actor__title')
                            author_name = None
                            if author_element:
                                raw_author = author_element.inner_text().strip()
                                author_name = clean_author_name(raw_author)
                            
                            # Extract author headline and clean it
                            headline_element = container.query_selector('.update-components-actor__description')
                            profile_headline = None
                            if headline_element:
                                raw_headline = headline_element.inner_text().strip()
                                profile_headline = remove_duplicated_text(raw_headline)
                            
                            # Extract post date (in text format like "2d")
                            date_element = container.query_selector('.update-components-actor__sub-description')
                            relative_date = None
                            formatted_date = None
                            if date_element:
                                date_text = date_element.inner_text().strip()
                                # Extract the date part (usually at the beginning, like "2d •")
                                date_match = re.search(r'^([0-9]+[dhmsw])', date_text)
                                if date_match:
                                    relative_date = date_match.group(1)
                                    # Convert relative date to actual date
                                    formatted_date = convert_relative_date(relative_date)
                            
                            # Construct post URL
                            post_url = None
                            if post_id:
                                post_url = f"https://www.linkedin.com/feed/update/urn:li:activity:{post_id}/"
                                
                                # Add post ID to seen posts set
                                seen_posts.add(post_id)
                            
                            # Create post dictionary
                            post_data = {
                                'post_id': post_id if post_id else str(uuid.uuid4()),
                                'post_date': formatted_date,
                                'post_author': author_name,
                                'profile_headline': profile_headline,
                                'text': post_text,
                                'post_url': post_url,
                                'hashtags': extract_hashtags(post_text)
                            }
                            
                            new_posts.append(post_data)
                            
                            # Print a preview of the post
                            preview = post_text[:100] + "..." if len(post_text) > 100 else post_text
                            print(f"\nPost {len(new_posts)}:")
                            print(f"ID: {post_data['post_id']}")
                            print(f"Author: {post_data['post_author']}")
                            print(f"Headline: {post_data['profile_headline']}")
                            print(f"Date: {relative_date} → {formatted_date}")
                            print(f"URL: {post_data['post_url']}")
                            print(f"Text: {preview}\n")
                            
                        except Exception as post_error:
                            print(f"Error extracting post data: {post_error}")
                    
                    return new_posts
                    
                except Exception as e:
                    print(f"Error extracting posts: {e}")
                    return []
            
            # Initial posts extraction - try several times if needed
            for attempt in range(3):
                print(f"Attempting initial post extraction (attempt {attempt+1}/3)...")
                initial_posts = extract_visible_posts()
                if initial_posts:
                    # Add posts to the current batch
                    current_batch.extend(initial_posts)
                    
                    # If batch size reached, save to database
                    if len(current_batch) >= batch_size:
                        saved = save_posts_to_db(db_conn, current_batch)
                        total_saved += saved
                        all_posts.extend(current_batch)
                        current_batch = []
                    break
                else:
                    print("No posts found, waiting and trying again...")
                    time.sleep(3)
            
            # Scroll and extract more posts until we reach max_posts or no new posts are found
            previous_post_count = len(all_posts) + len(current_batch)
            consecutive_no_new_posts = 0
            
            while (len(all_posts) + len(current_batch)) < max_posts and consecutive_no_new_posts < 3:
                try:
                    # Scroll down to load more posts
                    print("Scrolling to load more posts...")
                    page.evaluate("window.scrollBy(0, window.innerHeight)")
                    
                    # Wait for new content to load with a fixed delay
                    time.sleep(scroll_delay)
                    
                    # Extract newly loaded posts
                    new_posts = extract_visible_posts()
                    
                    # Add new posts to the current batch
                    current_batch.extend(new_posts)
                    
                    # If batch size reached, save to database
                    if len(current_batch) >= batch_size:
                        saved = save_posts_to_db(db_conn, current_batch)
                        total_saved += saved
                        all_posts.extend(current_batch)
                        current_batch = []
                    
                    # Check if we found any new posts
                    current_total = len(all_posts) + len(current_batch)
                    if current_total == previous_post_count:
                        consecutive_no_new_posts += 1
                        print(f"No new posts found after scrolling ({consecutive_no_new_posts}/3)")
                    else:
                        consecutive_no_new_posts = 0
                        
                    previous_post_count = current_total
                    
                    print(f"Total posts collected: {current_total}/{max_posts}")
                except Exception as e:
                    print(f"Error during scrolling: {e}")
                    consecutive_no_new_posts += 1
            
            # Save any remaining posts in the current batch
            if current_batch:
                saved = save_posts_to_db(db_conn, current_batch)
                total_saved += saved
                all_posts.extend(current_batch)
            
            return total_saved
            
        except Exception as e:
            print(f"An error occurred during scraping: {e}")
            return 0
        finally:
            # Close the browser
            print("Closing browser...")
            browser.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape LinkedIn posts text and save to SQLite database')
    parser.add_argument('--search', '-s', type=str, help='Search query to find specific posts', default=None)
    parser.add_argument('--max', '-m', type=int, help='Maximum number of posts to scrape', default=50)
    parser.add_argument('--delay', '-d', type=int, help='Delay between scrolls in seconds', default=2)
    parser.add_argument('--database', '-db', type=str, help='SQLite database file path', default='linkedin_posts.db')
    parser.add_argument('--timeout', '-t', type=int, help='Timeout for page operations in milliseconds', default=60000)
    parser.add_argument('--batch', '-b', type=int, help='Save to database after collecting this many posts', default=10)
    
    args = parser.parse_args()
    
    # Initialize the database
    print(f"Initializing database at {args.database}...")
    db_conn = init_database(args.database)
    
    # Run the scraper
    print("Starting LinkedIn scraper...")
    saved_count = scrape_linkedin_posts(
        db_conn=db_conn,
        email=None,
        password=None,
        search_query=args.search,
        max_posts=args.max,
        scroll_delay=args.delay,
        timeout=args.timeout,
        batch_size=args.batch
    )
    
    # Show results
    print(f"Scraping completed. Total of {saved_count} posts saved to the database.")
    
    # Close the database connection
    db_conn.close()