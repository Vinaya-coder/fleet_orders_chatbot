import os
from dotenv import load_dotenv
from langchain_community.utilities import SQLDatabase
from sqlalchemy import create_engine, inspect, text
import psycopg2
from typing import Optional
import time

load_dotenv()

class FleetDB:
    _schema_cache = None
    _joins_cache = None
    _cache_time = 0
    _cache_ttl = 3600 
    
    def __init__(self):
        self.tables = [
            "document_uploads", "document_orders", "email_requests",
            "extracted_data_revisions", "llm_usage_logs"
        ]
        self.db_url = os.getenv("DATABASE_URL")
        if not self.db_url:
            raise ValueError("❌ DATABASE_URL missing from .env")
        # Create a reusable engine for reflection
        self.engine = create_engine(self.db_url, pool_pre_ping=True, pool_size=5)

    def get_main_db(self) -> SQLDatabase:
        return SQLDatabase.from_uri(
            self.db_url,
            include_tables=self.tables,
            view_support=True,
            sample_rows_in_table_info=2  # Reduced from 3 to 2 rows
        )
    
    def get_live_schema(self) -> str:
        # Check cache validity
        current_time = time.time()
        if FleetDB._schema_cache and (current_time - FleetDB._cache_time) < FleetDB._cache_ttl:
            return FleetDB._schema_cache
        
        inspector = inspect(self.engine)
        schema_info = []

        # 1. Get Table/Column structures
        for table_name in self.tables:
            columns = inspector.get_columns(table_name)
            col_desc = [f"{c['name']} ({c['type']})" for c in columns]
            schema_info.append(f"Table: {table_name} | Columns: {', '.join(col_desc)}")

        # 2. Get PG Enums (This solves your 'status' mismatch issues)
        enum_query = text("""
            SELECT t.typname, e.enumlabel 
            FROM pg_type t JOIN pg_enum e ON t.oid = e.enumtypid
            ORDER BY t.typname, e.enumsortorder;
        """)
        
        try:
            with self.engine.connect() as conn:
                enums = conn.execute(enum_query).fetchall()
                if enums:
                    schema_info.append("\n--- Database Custom Types (Enums) ---")
                    current_enum = ""
                    for e_name, e_val in enums:
                        if e_name != current_enum:
                            schema_info.append(f"Enum {e_name}:")
                            current_enum = e_name
                        schema_info.append(f"  - {e_val}")
        except Exception as e:
            print(f"⚠️ Could not fetch enums: {e}")

        result = "\n".join(schema_info)
        FleetDB._schema_cache = result
        FleetDB._cache_time = current_time
        return result
    
    @staticmethod
    def test_connections() -> bool:
        """Test all connections before starting."""
        print("🔍 Testing connections...")

        # Main DB
        try:
            db = FleetDB().get_main_db()
            tables = db.get_usable_table_names()
            print(f"✅ Main DB: Found {len(tables)} tables: {tables}")
        except Exception as e:
            print(f"❌ Main DB failed: {e}")
            return False

        # pgvector (optional)
        pg_conn = FleetDB().get_pgvector_db()
        if pg_conn:
            try:
                cur = pg_conn.cursor()
                cur.execute("SELECT 1")
                print("✅ pgvector DB: Ready for schema linking")
                cur.close()
            except Exception as e:
                print(f"❌ pgvector DB failed: {e}")
                pg_conn.close()
        else:
            print("⚠️ pgvector optional - using JSON schema fallback")

        return True
    def get_live_joins(self) -> str:
        """Reflects the database to find actual Foreign Key relationships with caching."""
        # Check cache validity
        current_time = time.time()
        if FleetDB._joins_cache and (current_time - FleetDB._cache_time) < FleetDB._cache_ttl:
            return FleetDB._joins_cache
        
        inspector = inspect(self.engine)
        links = []
        
        for table_name in self.tables:
            try:
                fks = inspector.get_foreign_keys(table_name)
                for fk in fks:
                    referred_table = fk['referred_table']
                    constrained_cols = fk['constrained_columns']
                    referred_cols = fk['referred_columns']
                    
                    # Create a clear string for the LLM to understand the link
                    links.append(
                        f"Relation: {table_name}.{constrained_cols[0]} -> {referred_table}.{referred_cols[0]}"
                    )
            except Exception as e:
                print(f"⚠️ Could not fetch joins for {table_name}: {e}")
        
        result = "\n".join(links) if links else "No explicit foreign key relationships found."
        FleetDB._joins_cache = result
        FleetDB._cache_time = current_time
        return result
    
def get_db_instance() -> SQLDatabase:
    """Backward compatibility."""
    return FleetDB().get_main_db()


def get_pgvector_connection():
    """For schema_linker.py."""
    return FleetDB().get_pgvector_db()


if __name__ == "__main__":
    FleetDB.test_connections()
