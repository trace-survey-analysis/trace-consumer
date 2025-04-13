from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class Comment(BaseModel):
    """Represents a student comment from a trace survey"""

    category: str
    questionText: str
    responseNumber: int
    commentText: str


class Instructor(BaseModel):
    """Represents an instructor from a trace survey"""

    name: str


class Rating(BaseModel):
    """Represents a rating question and response from a trace survey"""

    questionText: str
    category: str
    responses: int
    responseRate: float
    courseMean: float
    deptMean: float
    univMean: float
    courseMedian: float
    deptMedian: float
    univMedian: float


class Course(BaseModel):
    """Represents a course from a trace survey"""

    courseId: str
    courseName: str
    subject: str
    catalogSection: str
    semester: str
    year: int
    enrollment: int
    responses: int
    declines: int
    processedAt: datetime
    originalFileName: str
    gcsBucket: str
    gcsPath: str


class TraceProcessedMessage(BaseModel):
    """Represents a message received from the Kafka topic"""

    traceId: str
    course: Course
    instructor: Instructor
    ratings: List[Rating]
    comments: List[Comment]
    processedAt: datetime
    error: Optional[str] = None


class DbCourse(BaseModel):
    """Represents a course record in the database"""

    id: Optional[int] = None
    course_id: str
    course_name: str
    subject: str
    catalog_section: str
    semester: str
    year: int
    enrollment: int
    responses: int
    declines: int
    processed_at: datetime
    original_file_name: str
    gcs_bucket: str
    gcs_path: str
    created_at: Optional[datetime] = None

    @classmethod
    def from_course(cls, course: Course) -> "DbCourse":
        """Convert from message Course model to database Course model"""
        return cls(
            course_id=course.courseId,
            course_name=course.courseName,
            subject=course.subject,
            catalog_section=course.catalogSection,
            semester=course.semester,
            year=course.year,
            enrollment=course.enrollment,
            responses=course.responses,
            declines=course.declines,
            processed_at=course.processedAt,
            original_file_name=course.originalFileName,
            gcs_bucket=course.gcsBucket,
            gcs_path=course.gcsPath,
        )


class DbInstructor(BaseModel):
    """Represents an instructor record in the database"""

    id: Optional[int] = None
    name: str
    created_at: Optional[datetime] = None

    @classmethod
    def from_instructor(cls, instructor: Instructor) -> "DbInstructor":
        """Convert from message Instructor model to database Instructor model"""
        return cls(name=instructor.name)


class DbRating(BaseModel):
    """Represents a rating record in the database"""

    id: Optional[int] = None
    course_id: int
    question_text: str
    category: str
    responses: int
    response_rate: float
    course_mean: float
    dept_mean: float
    univ_mean: float
    course_median: float
    dept_median: float
    univ_median: float
    created_at: Optional[datetime] = None

    @classmethod
    def from_rating(cls, rating: Rating, course_id: int) -> "DbRating":
        """Convert from message Rating model to database Rating model"""
        return cls(
            course_id=course_id,
            question_text=rating.questionText,
            category=rating.category,
            responses=rating.responses,
            response_rate=rating.responseRate,
            course_mean=rating.courseMean,
            dept_mean=rating.deptMean,
            univ_mean=rating.univMean,
            course_median=rating.courseMedian,
            dept_median=rating.deptMedian,
            univ_median=rating.univMedian,
        )


class DbComment(BaseModel):
    """Represents a comment record in the database"""

    id: Optional[int] = None
    course_id: int
    category: str
    question_text: str
    response_number: int
    comment_text: str
    created_at: Optional[datetime] = None

    @classmethod
    def from_comment(cls, comment: Comment, course_id: int) -> "DbComment":
        """Convert from message Comment model to database Comment model"""
        return cls(
            course_id=course_id,
            category=comment.category,
            question_text=comment.questionText,
            response_number=comment.responseNumber,
            comment_text=comment.commentText,
        )


class DbProcessedTrace(BaseModel):
    """Represents a processed trace record in the database"""

    id: Optional[int] = None
    trace_id: str
    course_id: Optional[int] = None
    processed_at: datetime
    status: str
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
