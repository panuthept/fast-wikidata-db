# Download the lastest version of wikidata dump
wget -P ./data https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz --no-check-certificate

# Preprocess the dump
python -m fast_wikidata_db.preprocess.preprocess_dump \
    --input_dir ./data/latest-all.json.gz \
    --output_dir ./data/en \
    --num_lines_in_dump 99962322 \
    --batch_nums 10000 \
    --language_id en

# # Indexing the preprocessed dump
python -m fast_wikidata_db.indexing.indexing_dump \
    --input_dir ./data/en \
    --output_dir ./data/wikidata_db \
    --remove_old