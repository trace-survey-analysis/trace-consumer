import psycopg2
import psycopg2.extras
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

from models.data_models import (
    DbCourse,
    DbInstructor,
    DbRating,
    DbComment,
    DbProcessedTrace,
    TraceProcessedMessage,
)
from utils.logging import Logger


class PostgresClient:
    """Client for interacting with PostgreSQL database"""

    def __init__(
        self,
        host: str,
        port: int,
        dbname: str,
        user: str,
        password: str,
        logger: Logger,
        schema: str = "trace",
    ):
        """Initialize the PostgreSQL client"""
        self.connection_params = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
        }
        self.logger = logger
        self.schema = schema
        self.conn = None

        # Try to establish initial connection
        self._connect()

    def _connect(self) -> None:
        """Establish connection to PostgreSQL database"""
        try:
            if self.conn is None or self.conn.closed:
                self.logger.info(
                    f"Connecting to PostgreSQL at {self.connection_params['host']}:{self.connection_params['port']}"
                )
                self.conn = psycopg2.connect(**self.connection_params)
                self.conn.autocommit = False
                self.logger.info("Connected to PostgreSQL successfully")
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {str(e)}", e)
            raise

    def _ensure_connection(self) -> None:
        """Ensure connection is established"""
        try:
            if self.conn is None or self.conn.closed:
                self._connect()

            # Check if connection is alive
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
        except Exception:
            # Try to reconnect
            self.logger.info("Connection lost, attempting to reconnect...")
            self._connect()

    def test_connection(self) -> bool:
        """Test database connection, return True if successful"""
        try:
            self._ensure_connection()
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception as e:
            self.logger.error(f"Database connection test failed: {str(e)}", e)
            return False

    def close(self) -> None:
        """Close the database connection"""
        if self.conn and not self.conn.closed:
            self.conn.close()
            self.logger.info("Database connection closed")

    def save_processed_message(self, message: TraceProcessedMessage) -> bool:
        """
        Save all data from a processed message to the database

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self._ensure_connection()
            with self.conn:  # Transaction context
                with self.conn.cursor() as cursor:
                    # First check if this trace has already been processed
                    cursor.execute(
                        f"SELECT id FROM {self.schema}.processed_traces WHERE trace_id = %s",
                        (message.traceId,),
                    )
                    if cursor.fetchone():
                        self.logger.info(
                            f"Trace {message.traceId} already processed, skipping"
                        )
                        return True

                    # 1. Save the instructor
                    db_instructor = DbInstructor.from_instructor(message.instructor)
                    cursor.execute(
                        f"""
                        INSERT INTO {self.schema}.instructors (name)
                        VALUES (%s)
                        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                        RETURNING id
                        """,
                        (db_instructor.name,),
                    )
                    instructor_id = cursor.fetchone()[0]

                    # 2. Save the course
                    db_course = DbCourse.from_course(message.course)
                    cursor.execute(
                        f"""
                        INSERT INTO {self.schema}.courses 
                        (course_id, course_name, subject, catalog_section, semester, year, 
                         enrollment, responses, declines, processed_at, original_file_name, 
                         gcs_bucket, gcs_path)
                        VALUES 
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (course_id, semester, year) 
                        DO UPDATE SET 
                            course_name = EXCLUDED.course_name,
                            subject = EXCLUDED.subject,
                            catalog_section = EXCLUDED.catalog_section,
                            enrollment = EXCLUDED.enrollment,
                            responses = EXCLUDED.responses,
                            declines = EXCLUDED.declines,
                            processed_at = EXCLUDED.processed_at,
                            original_file_name = EXCLUDED.original_file_name,
                            gcs_bucket = EXCLUDED.gcs_bucket,
                            gcs_path = EXCLUDED.gcs_path
                        RETURNING id
                        """,
                        (
                            db_course.course_id,
                            db_course.course_name,
                            db_course.subject,
                            db_course.catalog_section,
                            db_course.semester,
                            db_course.year,
                            db_course.enrollment,
                            db_course.responses,
                            db_course.declines,
                            db_course.processed_at,
                            db_course.original_file_name,
                            db_course.gcs_bucket,
                            db_course.gcs_path,
                        ),
                    )
                    course_id = cursor.fetchone()[0]

                    # 3. Link course and instructor in the join table
                    cursor.execute(
                        f"""
                        INSERT INTO {self.schema}.course_instructors (course_id, instructor_id)
                        VALUES (%s, %s)
                        ON CONFLICT (course_id, instructor_id) DO NOTHING
                        """,
                        (course_id, instructor_id),
                    )

                    # 4. Save ratings
                    for rating in message.ratings:
                        db_rating = DbRating.from_rating(rating, course_id)
                        cursor.execute(
                            f"""
                            INSERT INTO {self.schema}.ratings 
                            (course_id, question_text, category, responses, response_rate, 
                             course_mean, dept_mean, univ_mean, course_median, dept_median, univ_median)
                            VALUES 
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                db_rating.course_id,
                                db_rating.question_text,
                                db_rating.category,
                                db_rating.responses,
                                db_rating.response_rate,
                                db_rating.course_mean,
                                db_rating.dept_mean,
                                db_rating.univ_mean,
                                db_rating.course_median,
                                db_rating.dept_median,
                                db_rating.univ_median,
                            ),
                        )

                    # 5. Save comments
                    for comment in message.comments:
                        db_comment = DbComment.from_comment(comment, course_id)
                        cursor.execute(
                            f"""
                            INSERT INTO {self.schema}.comments 
                            (course_id, category, question_text, response_number, comment_text)
                            VALUES 
                            (%s, %s, %s, %s, %s)
                            """,
                            (
                                db_comment.course_id,
                                db_comment.category,
                                db_comment.question_text,
                                db_comment.response_number,
                                db_comment.comment_text,
                            ),
                        )

                    # 6. Save record of processed trace
                    status = "error" if message.error else "success"
                    cursor.execute(
                        f"""
                        INSERT INTO {self.schema}.processed_traces 
                        (trace_id, course_id, processed_at, status, error_message)
                        VALUES 
                        (%s, %s, %s, %s, %s)
                        """,
                        (
                            message.traceId,
                            course_id,
                            message.processedAt,
                            status,
                            message.error,
                        ),
                    )

                    self.logger.info(
                        f"Successfully saved trace {message.traceId} to database"
                    )

            return True
        except Exception as e:
            self.logger.error(
                f"Error saving processed message to database: {str(e)}", e
            )
            return False

    def get_processed_trace_ids(self, limit: int = 100) -> List[str]:
        """Get a list of trace IDs that have already been processed"""
        try:
            self._ensure_connection()
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.execute(
                        f"""
                        SELECT trace_id FROM {self.schema}.processed_traces
                        ORDER BY processed_at DESC
                        LIMIT %s
                        """,
                        (limit,),
                    )
                    return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error getting processed trace IDs: {str(e)}", e)
            return []
