# Download the lastest version of wikidata dump
wget -P ./data https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz --no-check-certificate

# Preprocess the dump
python -m fast_wikidata_db.preprocess.preprocess_dump \
    --input_dir ./data/latest-all.json.gz \
    --output_dir ./data/en \
    --batch_nums 10000 \
    --language_id en

# # Indexing the preprocessed dump
# python fast_wikidata_db/indexing/indexing_entity_rels.py \
#     --input_file ./data/en \
#     --num_files 10000

# python fast_wikidata_db/indexing/indexing_value_rels.py \
#     --input_file ./data/en \
#     --num_files 10000

# python fast_wikidata_db/indexing/indexing_labels.py \
#     --input_file ./data/en \
#     --num_files 10000

# python fast_wikidata_db/indexing/indexing_descriptions.py \
#     --input_file ./data/en \
#     --num_files 10000

# python fast_wikidata_db/indexing/indexing_wikipedia_titles.py \
#     --input_file ./data/en \
#     --num_files 10000