# 🚀 Git Setup and GitHub Push Guide

Follow these steps to push your project to GitHub:

## Step 1: Initialize Git Repository

```bash
cd /Users/radityaaruan/pegadaian
git init
```

## Step 2: Create .gitignore (Already Done ✅)

The .gitignore file has been created to exclude unnecessary files.

## Step 3: Add All Files

```bash
git add .
```

## Step 4: Make Initial Commit

```bash
git commit -m "Initial commit: Pegadaian ETL Pipeline with Airflow and PySpark

- Implemented medallion architecture (Silver-Bronze-Gold)
- Added Apache Airflow DAG for workflow orchestration
- Created PySpark ETL scripts for data processing
- Integrated PostgreSQL for data warehouse
- Added comprehensive documentation and setup guide"
```

## Step 5: Create GitHub Repository

1. Go to [GitHub.com](https://github.com)
2. Click "+" in top right corner
3. Select "New repository"
4. Repository name: `pegadaian-etl-pipeline`
5. Description: `End-to-end ETL pipeline for Pegadaian sales data using Apache Airflow, PySpark, and PostgreSQL`
6. Set to Public (for portfolio visibility)
7. Don't initialize with README (you already have one)
8. Click "Create repository"

## Step 6: Connect Local Repository to GitHub

Replace `yourusername` with your actual GitHub username:

```bash
git remote add origin https://github.com/yourusername/pegadaian-etl-pipeline.git
git branch -M main
```

## Step 7: Push to GitHub

```bash
git push -u origin main
```

## Step 8: Verify Upload

1. Go to your repository on GitHub
2. Check that all files are uploaded
3. Verify README.md displays correctly

## Step 9: Create Release (Optional)

1. Go to your repository
2. Click "Releases" → "Create a new release"
3. Tag: `v1.0.0`
4. Title: `Initial Release - Pegadaian ETL Pipeline`
5. Description: Brief overview of features
6. Click "Publish release"

## Step 10: Update Your Profile

1. Add repository to your pinned repositories
2. Update your GitHub profile README to mention this project
3. Add relevant topics/tags to the repository

## Recommended Repository Topics:

Add these topics to your repository for better discoverability:

- `etl-pipeline`
- `apache-airflow`
- `pyspark`
- `postgresql`
- `data-engineering`
- `python`
- `medallion-architecture`
- `data-warehouse`
- `workflow-orchestration`

## Future Updates

To push future changes:

```bash
# Make your changes
git add .
git commit -m "Description of changes"
git push origin main
```

## Troubleshooting

### If you get authentication errors:
1. Use Personal Access Token instead of password
2. Or set up SSH key authentication

### If repository already exists:
```bash
git remote remove origin
git remote add origin https://github.com/yourusername/pegadaian-etl-pipeline.git
```

## Next Steps After Push

1. ✅ Share on LinkedIn with your post
2. ✅ Add to your portfolio website
3. ✅ Consider writing a technical blog post
4. ✅ Add to your resume under projects section
