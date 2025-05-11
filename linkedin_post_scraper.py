from playwright.sync_api import sync_playwright, TimeoutError
import time
import os
from datetime import datetime
import argparse
import urllib.parse

def scrape_linkedin_posts(email=None, password=None, search_query=None, max_posts=50, scroll_delay=2, timeout=60000):
    """
    Scrape LinkedIn posts and extract their text content
    
    Args:
        email (str, optional): LinkedIn email/username
        password (str, optional): LinkedIn password
        search_query (str, optional): Search query to find specific posts
        max_posts (int, optional): Maximum number of posts to scrape
        scroll_delay (int, optional): Delay between scrolls in seconds
        timeout (int, optional): Timeout for page operations in milliseconds
    
    Returns:
        list: List of post texts
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
                    return []
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
                    
                    # After expanding all posts, try multiple selectors to find post content
                    selectors = [
                        'div.feed-shared-update-v2__description .update-components-text span.break-words',
                        '.update-components-text span.break-words',
                        '.feed-shared-update-v2 .feed-shared-inline-show-more-text',
                        '.feed-shared-text .feed-shared-inline-show-more-text',
                        '.ember-view .feed-shared-update-v2__description',
                        '.update-components-text',
                        '.feed-shared-update-v2',
                        '.search-results__cluster-content .feed-shared-inline-show-more-text'
                    ]
                    
                    posts = []
                    used_selector = None
                    
                    for selector in selectors:
                        try:
                            elements = page.query_selector_all(selector)
                            if elements and len(elements) > 0:
                                posts = elements
                                used_selector = selector
                                print(f"Found {len(posts)} posts using selector: {selector}")
                                break
                        except Exception as selector_error:
                            print(f"Error with selector {selector}: {selector_error}")
                    
                    if not posts:
                        print("No posts found with any of the selectors. Taking a screenshot...")
                        screenshot_path = f"no_posts_found_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                        page.screenshot(path=screenshot_path)
                        print(f"Screenshot saved to: {screenshot_path}")
                        return []
                    
                    new_posts = []
                    for post in posts:
                        try:
                            # Extract text content
                            post_text = post.inner_text().strip()
                            
                            # Skip empty posts and duplicates
                            if post_text and post_text not in seen_posts:
                                seen_posts.add(post_text)
                                new_posts.append(post_text)
                                
                                # Print a preview of the post
                                preview = post_text[:100] + "..." if len(post_text) > 100 else post_text
                                print(f"\nPost {len(seen_posts)}:\n{preview}\n")
                        except Exception as post_error:
                            print(f"Error extracting post text: {post_error}")
                    
                    return new_posts
                except Exception as e:
                    print(f"Error extracting posts: {e}")
                    return []
            
            # Initial posts extraction - try several times if needed
            for attempt in range(3):
                print(f"Attempting initial post extraction (attempt {attempt+1}/3)...")
                initial_posts = extract_visible_posts()
                if initial_posts:
                    all_posts.extend(initial_posts)
                    break
                else:
                    print("No posts found, waiting and trying again...")
                    time.sleep(3)
            
            # Scroll and extract more posts until we reach max_posts or no new posts are found
            previous_post_count = 0
            consecutive_no_new_posts = 0
            
            while len(all_posts) < max_posts and consecutive_no_new_posts < 3:
                try:
                    # Scroll down to load more posts
                    print("Scrolling to load more posts...")
                    page.evaluate("window.scrollBy(0, window.innerHeight)")
                    
                    # Wait for new content to load with a fixed delay
                    time.sleep(scroll_delay)
                    
                    # Extract newly loaded posts
                    new_posts = extract_visible_posts()
                    all_posts.extend(new_posts)
                    
                    # Check if we found any new posts
                    if len(all_posts) == previous_post_count:
                        consecutive_no_new_posts += 1
                        print(f"No new posts found after scrolling ({consecutive_no_new_posts}/3)")
                    else:
                        consecutive_no_new_posts = 0
                        
                    previous_post_count = len(all_posts)
                    
                    print(f"Total posts collected: {len(all_posts)}/{max_posts}")
                except Exception as e:
                    print(f"Error during scrolling: {e}")
                    consecutive_no_new_posts += 1
            
            return all_posts
            
        except Exception as e:
            print(f"An error occurred during scraping: {e}")
            return []
        finally:
            # Close the browser
            print("Closing browser...")
            browser.close()

def save_posts_to_file(posts, filename=None):
    """Save the scraped posts to a text file"""
    if not posts:
        print("No posts collected to save.")
        return
        
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"linkedin_posts_{timestamp}.txt"
    
    with open(filename, 'w', encoding='utf-8') as f:
        for i, post in enumerate(posts, 1):
            f.write(f"Post {i}:\n{post}\n\n{'='*80}\n\n")
    
    print(f"Saved {len(posts)} posts to {filename}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape LinkedIn posts text')
    parser.add_argument('--username', '-u', type=str, help='LinkedIn username/email', default=None)
    parser.add_argument('--password', '-p', type=str, help='LinkedIn password', default=None)
    parser.add_argument('--search', '-s', type=str, help='Search query to find specific posts', default=None)
    parser.add_argument('--max', '-m', type=int, help='Maximum number of posts to scrape', default=50)
    parser.add_argument('--delay', '-d', type=int, help='Delay between scrolls in seconds', default=2)
    parser.add_argument('--output', '-o', type=str, help='Output file name', default=None)
    parser.add_argument('--timeout', '-t', type=int, help='Timeout for page operations in milliseconds', default=60000)
    
    args = parser.parse_args()
    
    # Security warning for password in command line
    if args.password:
        print("WARNING: Supplying passwords via command line arguments is not secure!")
        print("         Consider using environment variables instead.")
    
    # Run the scraper
    posts = scrape_linkedin_posts(
        email=args.username,
        password=args.password,
        search_query=args.search,
        max_posts=args.max,
        scroll_delay=args.delay,
        timeout=args.timeout
    )
    
    # Save the results
    save_posts_to_file(posts, args.output)