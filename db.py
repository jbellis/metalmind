import time
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple, Union, Any, Optional
from urllib.parse import urlparse
from uuid import uuid4, uuid1, UUID

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

# data model:
# we have urls, paths, and chunks.
# the url table records the full url.  Every time we save a page, we save a new row here.
# the paths table records url hostname and path.  Before saving a page, we check the most
# recent version of the page in the paths table.  If it's the same, we don't save the page.
# This allows us to accommodate urls that only differ by query string, without saving
# multiple copies of the same page.
class DB:
    def __init__(self, cluster: Cluster) -> None:
        self.keyspace = "total_recall"
        self.table_chunks = "saved_chunks"
        self.table_pages = "saved_pages"
        fingerprint_index_name = f"{self.table_pages}_fingerprint_idx" # update this when index column name changes
        embedding_index_name = f"{self.table_chunks}_embedding_idx" # update this when index column name changes
        # TODO add chunks_embedding_column as a constant so it can change easier
        self.cluster = cluster
        self.session = self.cluster.connect()

        # Keyspace (don't try to create unless it's a local cluster)
        if self.cluster.contact_points == ['127.0.0.1']:
            self.session.execute(
                f"""
                CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
                WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}
                """
            )

        # Pages table
        self.session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.{self.table_pages} (
            user_id uuid,
            url_id timeuuid,
            full_url text,
            title text,
            text_content text,
            content_gz blob,
            fingerprint vector<float, 4096>,
            PRIMARY KEY (user_id, url_id));
            """
        )
        # fingerprint index
        self.session.execute(
            f"""
            CREATE CUSTOM INDEX IF NOT EXISTS {fingerprint_index_name} ON {self.keyspace}.{self.table_pages}(fingerprint)
            USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'
            WITH OPTIONS = {{ 'source_model': 'ada002' }}
            """
        )

        # Chunks table
        self.session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.{self.table_chunks} (
            user_id uuid,
            url_id timeuuid,
            full_url text,
            title text,
            chunk text,
            embedding_g4 vector<float, 768>,
            PRIMARY KEY (user_id, chunk));
            """
        )
        # Embedding index
        self.session.execute(
            f"""
            CREATE CUSTOM INDEX IF NOT EXISTS {embedding_index_name} ON {self.keyspace}.{self.table_chunks}(embedding_g4)
            USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'
            WITH OPTIONS = {{ 'source_model': 'gecko' }}
            """
        )

    def upsert_chunks(self,
                      user_id: uuid4,
                      full_url: str,
                      title: str,
                      text_content: str,
                      fingerprint: List[float],
                      chunks: List[Tuple[str, List[float]]],
                      url_uuid: Optional[uuid1]) -> None:
        st_pages = self.session.prepare(
            f"""
            INSERT INTO {self.keyspace}.{self.table_pages}
            (user_id, url_id, full_url, title, text_content, fingerprint)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        )
        self.session.execute(st_pages, (user_id, url_uuid, full_url, title, text_content, fingerprint))

        st_chunks = self.session.prepare(
            f"""
            INSERT INTO {self.keyspace}.{self.table_chunks}
            (user_id, url_id, full_url, title, chunk, embedding_g4)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        )
        denormalized_chunks = [(user_id, url_uuid, full_url, title, chunk, embedding)
                               for chunk, embedding in chunks]
        backoff = 0.5
        # print(f"Inserting {denormalized_chunks}")
        while denormalized_chunks and backoff < 60:
            results = execute_concurrent_with_args(self.session, st_chunks, denormalized_chunks,
                                                   concurrency=16, raise_on_first_error=True)
            denormalized_chunks = [chunk for chunk, (success, _)
                                   in zip(denormalized_chunks, results) if not success]
            time.sleep(backoff)
            backoff *= 2
        if denormalized_chunks:
            raise Exception(f"Failed to insert {len(denormalized_chunks)} chunks")


    def recent_urls(self, user_id: uuid4, saved_before: Optional[datetime], limit: int) -> List[Dict[str, Union[str, datetime, UUID]]]:
        if saved_before:
            cql = f"""
                  SELECT full_url, title, url_id 
                  FROM {self.keyspace}.{self.table_pages} 
                  WHERE user_id = ? AND url_id < minTimeuuid(?)
                  ORDER BY url_id DESC
                  LIMIT ?
                  """
        else:
            cql = f"""
                  SELECT full_url, title, url_id 
                  FROM {self.keyspace}.{self.table_pages} 
                  WHERE user_id = ? 
                  ORDER BY url_id DESC
                  LIMIT ?
                  """
        query = self.session.prepare(
            cql
        )
        if saved_before:
            results = self.session.execute(query, (user_id, saved_before, limit))
        else:
            results = self.session.execute(query, (user_id, limit))
        return [{k: getattr(row, k) for k in ['full_url', 'title', 'url_id']} for row in results]


    def search(self, user_id: uuid4, vector: List[float]) -> List[Dict[str, Union[Tuple[str, float, UUID]]]]:
        query = self.session.prepare(
            f"""
            SELECT full_url, title, chunk, url_id, similarity_dot_product(embedding_g4, ?) as score
            FROM {self.keyspace}.{self.table_chunks} 
            WHERE user_id = ? 
            ORDER BY embedding_g4 ANN OF ? LIMIT 10
            """
        )
        result_set = self.session.execute(query, (vector, user_id, vector))
        url_dict = defaultdict(lambda: {'chunks': [], 'title': None, 'url_id': None})

        for row in result_set:
            if len(url_dict[row.full_url]['chunks']) < 3:  # only keep the top 3 chunks for each URL
                url_dict[row.full_url]['chunks'].append((row.chunk, row.score))
                url_dict[row.full_url]['title'] = row.title
                url_dict[row.full_url]['url_id'] = row.url_id

        # Convert dictionary to list
        return [{'full_url': url, **info} for url, info in url_dict.items()]

    def load_snapshot(self, user_id: uuid4, url_id: uuid1) -> tuple[str, str, str, str]:
        query = self.session.prepare(
            f"""
            SELECT full_url, title, text_content, content_gz
            FROM {self.keyspace}.{self.table_pages} 
            WHERE user_id = ? AND url_id = ?
            """
        )
        return self.session.execute(query, (user_id, url_id)).one()

    def save_formatting(self, user_id: uuid4, url_id: uuid1, content_gz: str) -> None:
        request = self.session.prepare(
            f"""
            UPDATE {self.keyspace}.{self.table_pages}
            SET content_gz = ?
            WHERE user_id = ? AND url_id = ?
            """
        )
        self.session.execute(request, (content_gz, user_id, url_id))

    def _get_user_ids(self):
        return  self.session.execute(f"SELECT user_id FROM {self.keyspace}.{self.table_chunks}").all()

    def similar_page_exists(self, user_id, fingerprint):
        query = self.session.prepare(
            f"""
            SELECT similarity_dot_product(fingerprint, ?) 
            FROM {self.keyspace}.{self.table_pages} 
            WHERE user_id = ? 
            ORDER BY fingerprint ANN OF ? LIMIT 1
            """
        )
        rs = self.session.execute(query, (fingerprint, user_id, fingerprint))
        if not rs.current_rows:
            print("No similar pages found")
            return False
        print("Most similar page is ", rs.one()[0])
        return rs.one()[0] >= 0.99
