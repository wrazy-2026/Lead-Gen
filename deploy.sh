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

# Deploy to Cloud Run
echo -e "\n${YELLOW}Deploying to Cloud Run...${NC}"

gcloud run deploy leadgen-dashboard \
    --source . \
    --region us-central1 \
    --platform managed \
    --allow-unauthenticated \
    --memory 1Gi \
    --timeout 300 \
    --set-env-vars "SECRET_KEY=REPLACE_ME" \
    --set-env-vars "ADMIN_EMAIL=samadly728@gmail.com" \
    --set-env-vars "GOOGLE_CLIENT_ID=REPLACE_ME" \
    --set-env-vars "GOOGLE_CLIENT_SECRET=REPLACE_ME" \
    --set-env-vars "SERPER_API_KEY=REPLACE_ME" \
    --set-env-vars "APIFY_TOKEN=REPLACE_ME" \
    --set-env-vars "GEMINI_API_KEY=REPLACE_ME" \
    --set-env-vars "FIREBASE_PROJECT_ID=lively-paratext-487716-r8" \
    --set-env-vars "FIRESTORE_DATABASE_ID=leadgen"

# Get service URL
SERVICE_URL=$(gcloud run services describe leadgen-dashboard --region us-central1 --format="value(status.url)")

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}  Deployment Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "Service URL: ${SERVICE_URL}"
echo -e "\n${YELLOW}IMPORTANT: Update your Google OAuth redirect URI:${NC}"
echo -e "  ${SERVICE_URL}/auth/callback"
echo -e "\nGo to: https://console.cloud.google.com/apis/credentials"
