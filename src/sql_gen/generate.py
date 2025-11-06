import json
import os.path
import random

from sql_gen.generator.generate_pipeline import generate_equivalent_sql_pair
from sql_gen.generator.method import add_point_to_point_dict
from sql_gen.utils import load_multi_points
from utils.tools import get_proj_root_path


def generate_entry():
    proj_root_path = get_proj_root_path()
    config_path = os.path.join(proj_root_path, 'src', 'sql_gen', 'generate_config.json')
    if os.path.exists(config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            gen_config = json.load(f)
            src_dialect = gen_config['src_dialect']
            tgt_dialect = gen_config['tgt_dialect']
            mode = gen_config['mode']
            if mode == 'specified':
                sql_pair = generate_equivalent_sql_pair(src_dialect, tgt_dialect, gen_config['points'], None, 5)
            elif mode == 'random':
                point_number = gen_config['point_number']
                points = load_multi_points(src_dialect, tgt_dialect)
                point_list = []
                for j in range(point_number):
                    new_point = random.sample(points, 1)[0]
                    point_list.append(new_point)
                added_points = []
                for p in point_list:
                    add_point_to_point_dict(added_points, p)
                    print(f'added: {p}')
                print(added_points)
                sql_pair = generate_equivalent_sql_pair(src_dialect, tgt_dialect, gen_config['points'], None, 5)

            output_path = gen_config['output_path']
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(sql_pair, f, indent=4, ensure_ascii=False)


generate_entry()
