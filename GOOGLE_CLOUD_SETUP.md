# Google Cloud SQL Setup Guide

## Step 1: Create Cloud SQL Instance

Run these commands in Google Cloud Shell or with gcloud CLI installed:

```bash
# Set your project ID
gcloud config set project gen-lang-client-0678707594

# Enable required APIs
gcloud services enable sqladmin.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable cloudbuild.googleapis.com

# Create Cloud SQL PostgreSQL instance (this takes 5-10 minutes)
gcloud sql instances create leadgen-db \
    --database-version=POSTGRES_14 \
    --tier=db-f1-micro \
    --region=us-central1 \
    --root-password=YOUR_SECURE_PASSWORD \
    --storage-type=SSD \
    --storage-size=10GB
```

## Step 2: Create Database and User

```bash
# Create the database
gcloud sql databases create leadgen --instance=leadgen-db

# Create a user (optional - can use postgres user)
gcloud sql users create leadgen \
    --instance=leadgen-db \
    --password=YOUR_USER_PASSWORD
```

## Step 3: Get Connection Name

```bash
# Get the connection name (format: project:region:instance)
gcloud sql instances describe leadgen-db --format="value(connectionName)"
```

This will output something like: `gen-lang-client-0678707594:us-central1:leadgen-db`

## Step 4: Update cloudbuild.yaml

Edit `cloudbuild.yaml` and replace the substitutions:

```yaml
substitutions:
  _CLOUD_SQL_CONNECTION: 'gen-lang-client-0678707594:us-central1:leadgen-db'
  _DATABASE_URL: 'postgresql://leadgen:YOUR_USER_PASSWORD@/leadgen?host=/cloudsql/gen-lang-client-0678707594:us-central1:leadgen-db'
  _GOOGLE_CLIENT_ID: '241819621736-jffp03gjmd8jqjt7scfdl9bcp6d1ka5v.apps.googleusercontent.com'
  _GOOGLE_CLIENT_SECRET: 'YOUR_GOOGLE_CLIENT_SECRET'
  _SECRET_KEY: 'generate-a-random-string-here'
  _ADMIN_EMAIL: 'samadly728@gmail.com'
  _SERPER_API_KEY: 'your-serper-api-key'
  _APIFY_TOKEN: 'your-apify-token'
```

## Step 5: Deploy to Cloud Run

```bash
# Submit build to Cloud Build
gcloud builds submit --config=cloudbuild.yaml

# Or deploy directly with environment variables
gcloud run deploy leadgen-dashboard \
    --source . \
    --region us-central1 \
    --allow-unauthenticated \
    --add-cloudsql-instances gen-lang-client-0678707594:us-central1:leadgen-db \
    --set-env-vars "DATABASE_URL=postgresql://leadgen:PASSWORD@/leadgen?host=/cloudsql/gen-lang-client-0678707594:us-central1:leadgen-db"
```

## Step 6: Update OAuth Redirect URI

After deployment, get your Cloud Run URL:
```bash
gcloud run services describe leadgen-dashboard --region us-central1 --format="value(status.url)"
```

Then update Google OAuth Console:
1. Go to https://console.cloud.google.com/apis/credentials
2. Edit your OAuth 2.0 Client ID
3. Add authorized redirect URI: `https://YOUR-CLOUD-RUN-URL/auth/callback`

## Cost Estimate

- **Cloud SQL db-f1-micro**: ~$7-10/month
- **Cloud Run**: Pay per use (likely $0-5/month for low traffic)
- **Cloud Build**: 120 free build-minutes/day

## Quick Commands Reference

```bash
# Check instance status
gcloud sql instances describe leadgen-db

# Connect to database locally (for testing)
gcloud sql connect leadgen-db --user=leadgen --database=leadgen

# View Cloud Run logs
gcloud run services logs read leadgen-dashboard --region us-central1

# Delete instance (if needed)
gcloud sql instances delete leadgen-db
```

## Troubleshooting

### Connection refused
- Ensure Cloud SQL Admin API is enabled
- Check that `--add-cloudsql-instances` is set correctly in Cloud Run

### Permission denied
- Grant Cloud SQL Client role to Cloud Run service account:
```bash
gcloud projects add-iam-policy-binding gen-lang-client-0678707594 \
    --member="serviceAccount:YOUR_PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
    --role="roles/cloudsql.client"
```

### Database not found
- Verify database was created: `gcloud sql databases list --instance=leadgen-db`
