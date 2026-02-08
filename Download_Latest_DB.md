# Instructions to Download the Latest DB

## Step 1: Pull the files
```bash
git clone https://github.com/blueai2022/gstream
```
**Expected output:** You should see progress messages like "Cloning into 'gstream'..." followed by file download progress.

## Step 2: Or if you already have the directory from last git clone, just pull the latest
```bash
cd gstream
git pull origin
```
**Expected output:** Either "Already up to date" or a list of files being updated.

## Step 3: Reassemble the file
```bash
cd db
cat news_db_part_* > news_database_07.db.gz
```
**Expected output:** No output means success. A new file `news_database_07.db.gz` will be created.

## Step 4: Verify integrity (optional)
```bash
gunzip news_database_07.db.gz
```
**Expected output:** No output means success. The file will be decompressed to `news_database_07.db`.

## Step 5: Clean up parts if needed
```bash
rm news_db_part_*
```
**Expected output:** No output means success. All `news_db_part_*` files will be deleted.