from argparse import ArgumentParser
from fast_wikidata_db.wikidata import Wikidata


if __name__ == "__main__":
    from time import time
    # Input arguments
    parser = ArgumentParser()
    parser.add_argument('--qcode', type=str, required=True, help='Qcode of the entity')
    parser.add_argument('--data_dir', type=str, default="./data/wikidata_db")
    parser.add_argument('--max_hop', type=int, default=9)
    args = parser.parse_args()

    wikidata = Wikidata()

    start_time = time()
    superclasses = []
    prev_qcodes = {args.qcode}
    for _ in range(args.max_hop):
        next_qcodes = set()
        for prev_qcode in prev_qcodes:
            visit_qcodes = wikidata.retrieve_entities(prev_qcode, pcode="P279")
            for visit_qcode in visit_qcodes:
                title = wikidata.retrieve_entity_title(visit_qcode)
                superclass = f"{title} ({visit_qcode})"
                if superclass not in superclasses:
                    superclasses.append(f"{title} ({visit_qcode})")
            next_qcodes.update(visit_qcodes)
        prev_qcodes = next_qcodes
    print("Time: {:.2f} seconds".format(time() - start_time))
    print(superclasses)