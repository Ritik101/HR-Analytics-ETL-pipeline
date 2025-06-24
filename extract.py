from faker import Faker
import random
import csv
import string
from google.cloud import storage

# Initialize Faker with consistent results
fake = Faker()
random.seed(42)  # For reproducible results

# Configuration
NUM_EMPLOYEES = 100
FILE_NAME = "employee_data.csv"
BUCKET_NAME = "bkt-empl-data"  # Replace with your bucket name

# Valid departments and corresponding job titles
DEPARTMENTS = {
    "HR": ["HR Manager", "Recruiter", "HR Generalist"],
    "IT": ["Software Engineer", "Data Analyst", "Data Scientist"],
    "Finance": ["Accountant", "Financial Analyst", "Controller"],
    "Sales": ["Sales Representative", "Account Executive", "Sales Manager"]
}

def generate_clean_data():
    """Generates database-friendly employee data"""
    department = random.choice(list(DEPARTMENTS.keys()))
    first_name = fake.first_name()
    last_name = fake.last_name()
    
    return {
        "first_name": first_name,
        "last_name": last_name,
        "job_title": random.choice(DEPARTMENTS[department]),
        "department": department,
        "email": f"{first_name.lower()}.{last_name.lower()}@company.com",
        "address": fake.street_address().replace('\n', ' '),  # Single line address
        "phone_number": f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}",  # US format
        "salary": random.randint(30000, 120000),  # Realistic salary range
        "password": ''.join(random.choices(string.ascii_letters + string.digits, k=8))  # Alphanumeric
    }

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to Google Cloud Storage"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        blob.upload_from_filename(source_file_name)
        print(f"✅ File uploaded to gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        print(f"❌ Upload failed: {str(e)}")
        print("\nTroubleshooting steps:")
        print("1. Run: gcloud auth login")
        print("2. Run: gcloud auth application-default login")
        print("3. Set project: gcloud config set project YOUR_PROJECT_ID")
        print("4. Verify bucket exists: gsutil ls gs://{bucket_name}")
        print("5. Check permissions: gcloud projects get-iam-policy YOUR_PROJECT_ID")

def main():
    # Generate and save data
    with open(FILE_NAME, 'w', newline='', encoding='utf-8') as file:
        fieldnames = ["first_name", "last_name", "job_title", "department", 
                    "email", "address", "phone_number", "salary", "password"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        
        writer.writeheader()
        for _ in range(NUM_EMPLOYEES):
            writer.writerow(generate_clean_data())
    
    print(f"✅ Clean data generated in {FILE_NAME}")
    
    # Upload to GCS
    upload_to_gcs(BUCKET_NAME, FILE_NAME, FILE_NAME)

if __name__ == "__main__":
    main()