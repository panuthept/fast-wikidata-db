# Download the lastest version of wikidata dump
wget -P ./data https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz

# Preprocess the dump
python preprocess_dump.py \
    --input_file ./data/latest-all.json.gz \
    --out_dir ./data
    --batch_size 10000
    --language_id en
