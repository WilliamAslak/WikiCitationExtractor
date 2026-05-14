from sqlalchemy import Column, Integer, String, Text, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Article(Base):
    __tablename__ = 'articles'

    # q_id is the primary identifier for the entity
    q_id = Column(String(50), primary_key=True, index=True)
    name = Column(Text, nullable=False)
    references = relationship("Reference", back_populates="article", cascade="all, delete-orphan")


class Reference(Base):
    __tablename__ = 'references'

    id = Column(Integer, primary_key=True, index=True)
    article_q_id = Column(String(50), ForeignKey('articles.q_id'), nullable=False)

    # Added q_id directly to the Reference table
    q_id = Column(String(50), index=True, nullable=True)

    # Source tracking
    language = Column(String(10), nullable=False)
    source_url = Column(String(500), nullable=False)

    raw_text = Column(Text, nullable=False)
    context_text = Column(Text, nullable=True)
    ref_type = Column(String(50))
    ref_name = Column(Text, nullable=True)
    doi = Column(String(100), index=True)
    pmid = Column(String(100), index=True)
    arxiv = Column(String(100), index=True)

    article = relationship("Article", back_populates="references")