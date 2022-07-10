from sqlalchemy import create_engine
from creds import USER, PW, HOST, DB, SCHEMA, SPOT_SCHEMA, MART_SCHEMA

engine = create_engine(
            f"postgresql+psycopg2://{USER}:{PW}@{HOST}:5432/{DB}",
            connect_args={'options': f'-csearch_path={SCHEMA}'})

spot_engine = create_engine(
            f"postgresql+psycopg2://{USER}:{PW}@{HOST}:5432/{DB}",
            connect_args={'options': f'-csearch_path={SPOT_SCHEMA}'})

mart_engine = create_engine(
            f"postgresql+psycopg2://{USER}:{PW}@{HOST}:5432/{DB}",
            connect_args={'options': f'-csearch_path={MART_SCHEMA}'})
