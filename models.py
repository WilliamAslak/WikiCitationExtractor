from sqlalchemy import Column, Integer, String, Text, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Article(Base):
    __tablename__ = 'articles'

    # q_id is the primary identifier for the entity
    q_id = Column(String(50), primary_key=True, index=True)
    name = Column(Text, nullable=False)
    # possible types: Person, Work and Unknown
    entity_type = Column(String(50), default="unknown")
    references = relationship("Reference", back_populates="article", cascade="all, delete-orphan")

# The bridge between an author and their related work.
class EntityLink(Base):
    __tablename__ = 'entity_links'

    id = Column(Integer, primary_key=True, index=True)
    parent_q_id = Column(String(50), index=True)
    child_q_id = Column(String(50), index=True)


class Reference(Base):
    __tablename__ = 'references'

    id = Column(Integer, primary_key=True, index=True)

    # Source tracking
    article_q_id = Column(String(50), ForeignKey('articles.q_id'), nullable=False)
    language = Column(String(10), nullable=False)
    source_url = Column(String(500), nullable=False)

    q_id = Column(String(50), index=True, nullable=True)
    raw_text = Column(Text, nullable=False)
    context_text = Column(Text, nullable=True)
    ref_type = Column(String(50))
    ref_name = Column(Text, nullable=True)
    doi = Column(String(100), index=True)
    pmid = Column(String(100), index=True)
    arxiv = Column(String(100), index=True)

    article = relationship("Article", back_populates="references")