import sqlite3
import time
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
from datetime import datetime
import argparse
import os
import huggingface_hub

# Database configuration
DB_PATH = "linkedin_posts.db"

def get_unanalyzed_posts(conn, limit=10):
    """
    Get posts from the database that don't have severity populated
    
    Args:
        conn (sqlite3.Connection): Database connection
        limit (int): Maximum number of posts to retrieve
        
    Returns:
        list: List of posts with post_id and text
    """
    cursor = conn.cursor()
    cursor.execute('''
    SELECT post_id, text FROM linkedin_posts 
    WHERE severity IS NULL OR severity = ''
    LIMIT ?
    ''', (limit,))
    
    posts = []
    for row in cursor.fetchall():
        posts.append({
            'post_id': row[0],
            'text': row[1]
        })
    
    return posts

def update_post_severity(conn, post_id, severity, reasons):
    """
    Update the severity and reasons for a post in the database
    
    Args:
        conn (sqlite3.Connection): Database connection
        post_id (str): Post ID to update
        severity (str): Severity value
        reasons (str): Reasons for the severity rating
    """
    cursor = conn.cursor()
    cursor.execute('''
    UPDATE linkedin_posts
    SET severity = ?, reasons = ?
    WHERE post_id = ?
    ''', (severity, reasons, post_id))
    conn.commit()

def authenticate_huggingface():
    """
    Authenticate with Hugging Face API
    
    Returns:
        bool: True if authentication successful, False otherwise
    """
    print("Authenticating with Hugging Face...")
    
    # Check if token is in environment variables
    token = os.environ.get("HF_TOKEN")
    
    # If not found in env vars, ask the user
    if not token:
        print("HF_TOKEN environment variable not found.")
        token = input("Please enter your Hugging Face API token: ")
        
    if not token:
        print("No token provided. Authentication failed.")
        return False
        
    try:
        huggingface_hub.login(token=token, add_to_git_credential=True)
        print("Authentication successful.")
        return True
    except Exception as e:
        print(f"Authentication failed: {e}")
        return False

def load_mistral_model():
    """
    Load the Mistral-7B-Instruct-v0.2 model with 4-bit quantization
    
    Returns:
        tuple: (model, tokenizer)
    """
    print("Loading Mistral-7B-Instruct-v0.2 model and tokenizer...")
    
    # Load tokenizer
    tokenizer = AutoTokenizer.from_pretrained("mistralai/Mistral-7B-Instruct-v0.2")
    
    # Load model with 4-bit quantization for efficiency on 6GB VRAM
    model = AutoModelForCausalLM.from_pretrained(
        "mistralai/Mistral-7B-Instruct-v0.2",
        device_map="auto",
        torch_dtype=torch.float16,
        load_in_4bit=True,
    )
    
    print("Model loaded successfully.")
    return model, tokenizer

def generate_response(model, tokenizer, prompt, max_length=2048):
    """
    Generate a response from the model
    
    Args:
        model: The loaded Mistral model
        tokenizer: The loaded tokenizer
        prompt (str): Prompt to send to the model
        max_length (int): Maximum response length
        
    Returns:
        str: Generated response
    """
    # Format prompt for Mistral-7B-Instruct-v0.2
    formatted_prompt = f"<s>[INST] {prompt} [/INST]"
    
    inputs = tokenizer(formatted_prompt, return_tensors="pt").to(model.device)
    
    # Generate response with appropriate parameters
    with torch.no_grad():
        outputs = model.generate(
            inputs.input_ids,
            max_new_tokens=512,
            temperature=0.3,  # Lower temperature for more focused responses
            top_p=0.9,
            do_sample=True,
            pad_token_id=tokenizer.eos_token_id
        )
    
    # Decode and clean up response
    full_output = tokenizer.decode(outputs[0], skip_special_tokens=True)
    
    # Extract just the assistant's response (after the instruction)
    response = full_output.split("[/INST]")[-1].strip()
    
    return response

def analyze_with_mistral(model, tokenizer, text):
    """
    Analyze text using Mistral-7B-Instruct-v0.2
    
    Args:
        model: The loaded Mistral model
        tokenizer: The loaded tokenizer
        text (str): Text to analyze
        
    Returns:
        tuple: (severity, reasons, full_response)
    """
    prompt = f"""You are an expert at analyzing content for toxic positivity. Analyze this LinkedIn post and rate it for toxic positivity.

Rate the severity on a scale from 0-3 where:
0 = non-toxic positive
1 = mildly toxic
2 = moderately toxic
3 = highly toxic

Provide a maximum of 5 bullet points explaining your rating. Focus on specific phrases, tone, and content that justify your severity rating.

Format your answer exactly like this:
Severity: [number]
Reasons:
- [First reason]
- [Second reason]
- [etc. up to 5 bullet points maximum]

Post:
{text}"""

    try:
        response_text = generate_response(model, tokenizer, prompt)
        
        # Extract severity and reasons
        severity = None
        reasons = None
        
        # Parse the response to extract severity
        severity_line = [line for line in response_text.split('\n') if line.lower().startswith('severity:')]
        if severity_line:
            try:
                severity_text = severity_line[0].split(':')[1].strip()
                # Convert numeric severity to the appropriate level
                if severity_text.isdigit() or severity_text.replace('.', '', 1).isdigit():
                    severity_num = int(float(severity_text))
                    if severity_num == 0:
                        severity = "0"  # Non-toxic positive
                    elif severity_num == 1:
                        severity = "1"  # Mildly toxic
                    elif severity_num == 2:
                        severity = "2"  # Moderately toxic
                    elif severity_num >= 3:  # Handle cases where model might output higher than 3
                        severity = "3"  # Highly toxic
                else:
                    # If the model returned a text description instead of a number
                    if "non" in severity_text.lower() or "not" in severity_text.lower():
                        severity = "0"
                    elif "mild" in severity_text.lower():
                        severity = "1"
                    elif "moderate" in severity_text.lower():
                        severity = "2"
                    elif "high" in severity_text.lower():
                        severity = "3"
                    else:
                        severity = severity_text  # Use as-is if can't parse
            except:
                severity = "Unknown"
        
        # Parse the response to extract reasons
        # First, check if there's a dedicated "Reasons:" section
        reasons_section = None
        if "Reasons:" in response_text:
            reasons_section = response_text.split("Reasons:")[1].strip()
        
        if reasons_section:
            # Try to extract bullet points
            bullet_points = []
            for line in reasons_section.split('\n'):
                line = line.strip()
                if line.startswith('-') or line.startswith('•') or line.startswith('*'):
                    bullet_points.append(line)
            
            if bullet_points:
                # Join bullet points with newlines to preserve formatting
                reasons = '\n'.join(bullet_points)
            else:
                # If no bullet points found, just use the whole section
                reasons = reasons_section
        else:
            # Fallback to the old method
            reasons_line = [line for line in response_text.split('\n') if line.lower().startswith('reasons:')]
            if reasons_line:
                try:
                    reasons = reasons_line[0].split(':')[1].strip()
                except:
                    reasons = "Unable to parse reasons"
        
        return severity, reasons, response_text
    
    except Exception as e:
        print(f"Error generating response: {e}")
        return None, None, f"Exception: {str(e)}"

def analyze_posts_with_mistral(batch_size=10):
    """
    Main function to analyze posts with Mistral-7B-Instruct-v0.2
    
    Args:
        batch_size (int): Number of posts to process in one batch
    """
    # Display information with the format specified by the user
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"Current Date and Time (UTC - YYYY-MM-DD HH:MM:SS formatted): {current_time}")
    print(f"Current User's Login: taifuranowar")
    print(f"{'='*80}\n")
    print(f"Using model: Mistral-7B-Instruct-v0.2")
    print(f"Batch size: {batch_size}")
    
    # Connect to the database
    try:
        conn = sqlite3.connect(DB_PATH)
        print(f"Connected to database: {DB_PATH}")
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return
    
    # Authenticate with Hugging Face
    if not authenticate_huggingface():
        print("Cannot proceed without Hugging Face authentication. Exiting.")
        conn.close()
        return
    
    # Load model and tokenizer
    try:
        model, tokenizer = load_mistral_model()
    except Exception as e:
        print(f"Error loading model: {e}")
        conn.close()
        return
    
    # Main processing loop
    total_processed = 0
    
    try:
        while True:
            # Get a batch of posts that need analysis
            posts = get_unanalyzed_posts(conn, batch_size)
            if not posts:
                print("No more posts found that need severity analysis.")
                break
            
            print(f"Found {len(posts)} posts that need severity analysis in this batch.")
            
            for i, post in enumerate(posts):
                print(f"\nProcessing post {i+1}/{len(posts)} (Total: {total_processed + i + 1})")
                print(f"Post ID: {post['post_id']}")
                post_text = post['text']
                print(f"Post Text (preview): {post_text[:100]}..." if len(post_text) > 100 else post_text)
                
                # Analyze the post with Mistral
                print(f"Analyzing with Mistral-7B-Instruct-v0.2...")
                severity, reasons, full_response = analyze_with_mistral(model, tokenizer, post_text)
                
                print(f"Analysis complete.")
                print(f"Severity: {severity}")
                print(f"Reasons:\n{reasons}")
                
                # Update the database
                if severity:
                    print(f"Updating database with severity: {severity}")
                    update_post_severity(conn, post['post_id'], severity, reasons)
                else:
                    print("Could not extract severity from model response.")
                    print(f"Full response: {full_response}")
                
                # Small delay between posts to allow GPU temperature management
                time.sleep(0.5)
            
            total_processed += len(posts)
            print(f"\nCompleted batch. Total posts processed: {total_processed}")
            
            # Optional delay between batches
            if len(posts) == batch_size:  # If we got a full batch, there might be more
                time.sleep(1)
    
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Saving progress...")
    
    except Exception as e:
        print(f"\nAn error occurred: {e}")
    
    finally:
        # Close the database connection
        conn.close()
        print("\nAnalysis session completed!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze LinkedIn posts with Mistral-7B-Instruct-v0.2')
    parser.add_argument('--batch', type=int, default=10, 
                        help='Batch size (default: 10)')
    parser.add_argument('--database', type=str, default="linkedin_posts.db", 
                        help='Database file path (default: linkedin_posts.db)')
    parser.add_argument('--token', type=str, default=None,
                        help='Hugging Face API token (can also use HF_TOKEN environment variable)')
    
    args = parser.parse_args()
    
    # Update global settings
    DB_PATH = args.database
    
    # Set token as environment variable if provided
    if args.token:
        os.environ["HF_TOKEN"] = args.token
    
    analyze_posts_with_mistral(batch_size=args.batch)