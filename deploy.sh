#!/bin/bash
# deploy.sh - Quick deployment script for Google Cloud

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  LeadGen Dashboard - Cloud Deployment ${NC}"
echo -e "${GREEN}========================================${NC}"

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}Error: gcloud CLI is not installed${NC}"
    echo "Install it from: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Get current project
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
if [ -z "$PROJECT_ID" ]; then
    echo -e "${RED}Error: No project set. Run: gcloud config set project YOUR_PROJECT_ID${NC}"
    exit 1
fi

echo -e "${YELLOW}Using project: ${PROJECT_ID}${NC}"

# Check if Cloud SQL instance exists
echo -e "\n${YELLOW}Checking Cloud SQL instance...${NC}"
INSTANCE_EXISTS=$(gcloud sql instances list --filter="name=leadgen-db" --format="value(name)" 2>/dev/null)

if [ -z "$INSTANCE_EXISTS" ]; then
    echo -e "${YELLOW}Cloud SQL instance not found. Creating...${NC}"
    echo -e "${YELLOW}This will take 5-10 minutes...${NC}"
    
    # Prompt for password
    read -sp "Enter a password for the database: " DB_PASSWORD
    echo
    
    # Create instance
    gcloud sql instances create leadgen-db \
        --database-version=POSTGRES_14 \
        --tier=db-f1-micro \
        --region=us-central1 \
        --root-password="$DB_PASSWORD" \
        --storage-type=SSD \
        --storage-size=10GB
    
    # Create database
    gcloud sql databases create leadgen --instance=leadgen-db
    
    # Create user
    gcloud sql users create leadgen --instance=leadgen-db --password="$DB_PASSWORD"
    
    echo -e "${GREEN}Cloud SQL instance created!${NC}"
else
    echo -e "${GREEN}Cloud SQL instance 'leadgen-db' exists${NC}"
    read -sp "Enter your database password: " DB_PASSWORD
    echo
fi

# Get connection name
CONNECTION_NAME=$(gcloud sql instances describe leadgen-db --format="value(connectionName)")
echo -e "${GREEN}Connection name: ${CONNECTION_NAME}${NC}"

# Build DATABASE_URL
DATABASE_URL="postgresql://leadgen:${DB_PASSWORD}@/leadgen?host=/cloudsql/${CONNECTION_NAME}"

# Deploy to Cloud Run
echo -e "\n${YELLOW}Deploying to Cloud Run...${NC}"

gcloud run deploy leadgen-dashboard \
    --source . \
    --region us-central1 \
    --platform managed \
    --allow-unauthenticated \
    --memory 1Gi \
    --timeout 300 \
    --add-cloudsql-instances "$CONNECTION_NAME" \
    --set-env-vars "DATABASE_URL=${DATABASE_URL}" \
    --set-env-vars "SECRET_KEY=$(openssl rand -hex 32)" \
    --set-env-vars "ADMIN_EMAIL=dev@maigreeks.com"

# Get service URL
SERVICE_URL=$(gcloud run services describe leadgen-dashboard --region us-central1 --format="value(status.url)")

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}  Deployment Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "Service URL: ${SERVICE_URL}"
echo -e "\n${YELLOW}IMPORTANT: Update your Google OAuth redirect URI:${NC}"
echo -e "  ${SERVICE_URL}/auth/callback"
echo -e "\nGo to: https://console.cloud.google.com/apis/credentials"
